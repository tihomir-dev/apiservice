package customer.apiservice.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import customer.apiservice.ias.ScimClient;
import java.net.http.HttpResponse;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class UserSyncService {

  private static final Logger log = LoggerFactory.getLogger(UserSyncService.class);

  private static final String SAP_EXT = "urn:ietf:params:scim:schemas:extension:sap:2.0:User";
  private static final String ENT_EXT =
      "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User";
  private static final String DEFAULT_USER_TYPE = "employee";

  private final ScimClient scim;
  private final JdbcTemplate jdbc;
  private final SyncNotificationService syncNotificationService;
  private final ObjectMapper om = new ObjectMapper();

  public UserSyncService(
      ScimClient scim, JdbcTemplate jdbc, SyncNotificationService syncNotificationService) {
    this.scim = scim;
    this.jdbc = jdbc;
    this.syncNotificationService = syncNotificationService;
  }

  @Transactional
  public Map<String, Object> syncAllUsers() {
    log.info("=== Starting user sync from IAS ===");

    int fetched = 0;
    int inserted = 0;
    int updated = 0;
    int deleted = 0;
    int skipped = 0;
    int errors = 0;

    // Load ALL users from db at the start
    Map<String, Map<String, Object>> dbUsers = loadAllUsersFromDb();
    Set<String> iasUserIds = new HashSet<>();
    List<Map<String, Object>> detailedChanges = new ArrayList<>();

    log.info("Loaded {} users from DB", dbUsers.size());

    int startIndex = 1;
    int pageSize = 100;

    try {
      while (true) {
        HttpResponse<String> resp = scim.getUsers("startIndex=" + startIndex + "&count=" + pageSize);

        if (resp.statusCode() / 100 != 2) {
          throw new RuntimeException(
              "IAS SCIM /Users failed: " + resp.statusCode() + " body=" + resp.body());
        }

        try {
          JsonNode root = om.readTree(resp.body());
          JsonNode resources = root.path("Resources");

          if (!resources.isArray() || resources.size() == 0) break;

          for (JsonNode u : resources) {
            try {
              fetched++;

              String id = text(u, "id");
              String loginName = text(u, "userName");
              String email = firstEmailCoreOrSap(u);
              String lastName = text(u.path("name"), "familyName");
              String firstName = text(u.path("name"), "givenName");
              String userType = text(u, "userType");
              if (isBlank(userType)) userType = DEFAULT_USER_TYPE;
              String status = statusFromCoreOrSap(u);

              LocalDate validFrom = parseDateFromIsoInstant(text(u.path(SAP_EXT), "validFrom"));
              LocalDate validTo = parseDateFromIsoInstant(text(u.path(SAP_EXT), "validTo"));
              String company = text(u.path(ENT_EXT), "organization");
              Address addr = pickAddressCoreOrSap(u);
              String country = addr.country();
              String city = addr.city();
              Timestamp iasLastModified = parseTimestamp(text(u.path("meta"), "lastModified"));

              if (isBlank(loginName)) loginName = email;

              if (isBlank(id)
                  || isBlank(loginName)
                  || isBlank(email)
                  || isBlank(lastName)
                  || isBlank(userType)
                  || isBlank(status)) {
                skipped++;
                continue;
              }

              iasUserIds.add(id);

              // Check if user changed
               changeInfo =
                  checkUserChanges(
                      id,
                      email,
                      lastName,
                      firstName,
                      userType,
                      status,
                      validFrom,
                      validTo,
                      company,
                      country,
                      city,
                      iasLastModified,
                      dbUsers);

              if (changeInfo.isNew) {
                // New user - INSERT
                jdbc.update(
                    MERGE_USERS_SQL,
                    id,
                    loginName,
                    email,
                    lastName,
                    userType,
                    firstName,
                    validFrom,
                    validTo,
                    company,
                    country,
                    city,
                    iasLastModified,
                    status,
                    id,
                    loginName,
                    email,
                    lastName,
                    userType,
                    firstName,
                    validFrom,
                    validTo,
                    company,
                    country,
                    city,
                    iasLastModified,
                    status);

                Map<String, Object> change = new HashMap<>();
                change.put("userId", id);
                change.put("action", "INSERTED");
                change.put("loginName", loginName);
                change.put("email", email);
                change.put("timestamp", System.currentTimeMillis());
                detailedChanges.add(change);

                inserted++;
                log.debug("Inserted new user: {} ({})", id, email);

              } else if (!changeInfo.changedFields.isEmpty()) {
                // User changed - UPDATE
                jdbc.update(
                    MERGE_USERS_SQL,
                    id,
                    loginName,
                    email,
                    lastName,
                    userType,
                    firstName,
                    validFrom,
                    validTo,
                    company,
                    country,
                    city,
                    iasLastModified,
                    status,
                    id,
                    loginName,
                    email,
                    lastName,
                    userType,
                    firstName,
                    validFrom,
                    validTo,
                    company,
                    country,
                    city,
                    iasLastModified,
                    status);

                Map<String, Object> change = new HashMap<>();
                change.put("userId", id);
                change.put("action", "UPDATED");
                change.put("loginName", loginName);
                change.put("email", email);
                change.put("changedFields", new ArrayList<>(changeInfo.changedFields));
                change.put("timestamp", System.currentTimeMillis());
                detailedChanges.add(change);

                updated++;
                log.debug("Updated user: {} (changed fields: {})", id, changeInfo.changedFields);

              } else {
                skipped++;
              }

            } catch (Exception e) {
              log.error("Error processing user from IAS", e);
              errors++;
            }
          }

          startIndex += resources.size();
          int totalResults = root.path("totalResults").asInt(-1);
          if (totalResults >= 0 && startIndex > totalResults) break;

        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to parse/merge IAS users page startIndex=" + startIndex, e);
        }
      }

      // Handle deletions: users in DB but not in IAS
      for (String dbUserId : dbUsers.keySet()) {
        if (!iasUserIds.contains(dbUserId)) {
          // User was deleted from IAS - completely remove from DB
          jdbc.update("DELETE FROM \"USERS\" WHERE \"ID\" = ?", dbUserId);

          Map<String, Object> change = new HashMap<>();
          change.put("userId", dbUserId);
          change.put("action", "DELETED");
          change.put("loginName", dbUsers.get(dbUserId).get("LOGIN_NAME"));
          change.put("email", dbUsers.get(dbUserId).get("EMAIL"));
          change.put("timestamp", System.currentTimeMillis());
          detailedChanges.add(change);

          deleted++;
          log.debug("Deleted user from DB: {}", dbUserId);
        }
      }

      log.info(
          "=== User sync completed: fetched={}, inserted={}, updated={}, deleted={}, skipped={}, errors={} ===",
          fetched,
          inserted,
          updated,
          deleted,
          skipped,
          errors);

      Map<String, Object> result = new HashMap<>();
      result.put("fetched", fetched);
      result.put("inserted", inserted);
      result.put("updated", updated);
      result.put("deleted", deleted);
      result.put("skipped", skipped);
      result.put("failed", errors);
      result.put("upserted", inserted + updated); // For backward compatibility
      result.put("detailedChanges", detailedChanges);

      // Notify if changes
      if ((inserted + updated + deleted) > 0) {
        syncNotificationService.notifyUserSync(result);
      } 

      return result;

    } catch (Exception e) {
      log.error("Failed to sync users from IAS", e);
      return Map.of("success", false, "error", e.getMessage());
    }
  }

  /** Load all users from DB into memory once. */
  private Map<String, Map<String, Object>> loadAllUsersFromDb() {
    String query =
        """
        SELECT "ID", "LOGIN_NAME", "EMAIL", "LAST_NAME", "FIRST_NAME", "USER_TYPE", "STATUS",
               "VALID_FROM", "VALID_TO", "COMPANY", "COUNTRY", "CITY", "IAS_LAST_MODIFIED"
        FROM "USERS"
        """;

    Map<String, Map<String, Object>> users = new HashMap<>();

    try {
      List<Map<String, Object>> rows = jdbc.queryForList(query);
      for (Map<String, Object> row : rows) {
        String id = (String) row.get("ID");
        users.put(id, row);
      }
    } catch (Exception e) {
      log.warn("Failed to load users from DB: {}", e.getMessage());
      return new HashMap<>();
    }

    return users;
  }

  /** Check if user changed and return what fields changed */
  private UserChangeInfo checkUserChanges(
      String id,
      String email,
      String lastName,
      String firstName,
      String userType,
      String status,
      LocalDate validFrom,
      LocalDate validTo,
      String company,
      String country,
      String city,
      Timestamp iasLastModified,
      Map<String, Map<String, Object>> dbUsers) {

    UserChangeInfo info = new UserChangeInfo();

    if (!dbUsers.containsKey(id)) {
      info.isNew = true;
      return info;
    }

    Map<String, Object> dbUser = dbUsers.get(id);

    // Check each field and track changes
    if (!nullSafeEquals(lastName, dbUser.get("LAST_NAME"))) {
      info.changedFields.add("lastName");
    }
    if (!nullSafeEquals(firstName, dbUser.get("FIRST_NAME"))) {
      info.changedFields.add("firstName");
    }
    if (!nullSafeEquals(userType, dbUser.get("USER_TYPE"))) {
      info.changedFields.add("userType");
    }
    if (!nullSafeEquals(status, dbUser.get("STATUS"))) {
      info.changedFields.add("status");
    }
    if (!nullSafeEquals(company, dbUser.get("COMPANY"))) {
      info.changedFields.add("company");
    }
    if (!nullSafeEquals(country, dbUser.get("COUNTRY"))) {
      info.changedFields.add("country");
    }
    if (!nullSafeEquals(city, dbUser.get("CITY"))) {
      info.changedFields.add("city");
    }
    if (!nullSafeEquals(email, dbUser.get("EMAIL"))) {
      info.changedFields.add("email");
    }

    // DATE comparisons
    LocalDate dbValidFrom = parseDbDate(dbUser.get("VALID_FROM"));
    if (!nullSafeEquals(validFrom, dbValidFrom)) {
      info.changedFields.add("validFrom");
    }

    LocalDate dbValidTo = parseDbDate(dbUser.get("VALID_TO"));
    if (!nullSafeEquals(validTo, dbValidTo)) {
      info.changedFields.add("validTo");
    }

    return info;
  }

  private LocalDate parseDbDate(Object dbValue) {
    if (dbValue == null) return null;
    try {
      return LocalDate.parse(dbValue.toString());
    } catch (Exception e) {
      return null;
    }
  }

  private boolean nullSafeEquals(Object a, Object b) {
    if (a == null && b == null) return true;
    if (a == null || b == null) return false;
    return a.equals(b);
  }

  private static final String MERGE_USERS_SQL =
      """
      MERGE INTO "USERS" AS T
      USING (SELECT ? AS "ID" FROM DUMMY) AS S
      ON (T."ID" = S."ID")
      WHEN MATCHED THEN UPDATE SET
        "LOGIN_NAME" = ?,
        "EMAIL" = ?,
        "LAST_NAME" = ?,
        "USER_TYPE" = ?,
        "FIRST_NAME" = ?,
        "VALID_FROM" = ?,
        "VALID_TO" = ?,
        "COMPANY" = ?,
        "COUNTRY" = ?,
        "CITY" = ?,
        "IAS_LAST_MODIFIED" = ?,
        "STATUS" = ?,
        "UPDATED_AT" = CURRENT_UTCTIMESTAMP
      WHEN NOT MATCHED THEN INSERT (
        "ID","LOGIN_NAME","EMAIL","LAST_NAME","USER_TYPE",
        "FIRST_NAME","VALID_FROM","VALID_TO","COMPANY","COUNTRY","CITY",
        "IAS_LAST_MODIFIED","STATUS","CREATED_AT","UPDATED_AT"
      ) VALUES (
        ?,?,?,?,?,  ?,?,?,?,?,?,  ?,?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP
      )
      """;

  private static String firstEmailCoreOrSap(JsonNode user) {
    String core = firstEmailFromArray(user.path("emails"));
    if (!isBlank(core)) return core;
    JsonNode sap = user.path(SAP_EXT);
    String ext = firstEmailFromArray(sap.path("emails"));
    if (!isBlank(ext)) return ext;
    return null;
  }

  private static String firstEmailFromArray(JsonNode emails) {
    if (emails != null && emails.isArray()) {
      for (JsonNode e : emails) {
        String value = e.path("value").asText(null);
        if (!isBlank(value)) return value;
      }
    }
    return null;
  }

  private static String statusFromCoreOrSap(JsonNode user) {
    JsonNode activeNode = user.get("active");
    if (activeNode != null && !activeNode.isNull()) {
      boolean active = activeNode.asBoolean(true);
      return active ? "ACTIVE" : "INACTIVE";
    }
    String extStatus = text(user.path(SAP_EXT), "status");
    if (!isBlank(extStatus) && "inactive".equalsIgnoreCase(extStatus)) return "INACTIVE";
    return "ACTIVE";
  }

  private static Address pickAddressCoreOrSap(JsonNode user) {
    Address core = pickAddressFromArray(user.path("addresses"));
    if (!core.isEmpty()) return core;
    Address ext = pickAddressFromArray(user.path(SAP_EXT).path("addresses"));
    if (!ext.isEmpty()) return ext;
    return new Address(null, null);
  }

  private static Address pickAddressFromArray(JsonNode addresses) {
    if (addresses == null || !addresses.isArray() || addresses.size() == 0)
      return new Address(null, null);
    JsonNode home = null, work = null, first = addresses.get(0);
    for (JsonNode a : addresses) {
      String type = a.path("type").asText("");
      if (home == null && "home".equalsIgnoreCase(type)) home = a;
      if (work == null && "work".equalsIgnoreCase(type)) work = a;
    }
    JsonNode chosen = (home != null) ? home : (work != null ? work : first);
    String country = chosen.path("country").asText(null);
    String city = chosen.path("locality").asText(null);
    return new Address(country, city);
  }

  private record Address(String country, String city) {
    boolean isEmpty() {
      return isBlank(country) && isBlank(city);
    }
  }

  private static String text(JsonNode node, String field) {
    if (node == null || node.isNull()) return null;
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) return null;
    String s = v.asText(null);
    return isBlank(s) ? null : s;
  }

  private static Timestamp parseTimestamp(String isoInstant) {
    if (isBlank(isoInstant)) return null;
    try {
      return Timestamp.from(Instant.parse(isoInstant));
    } catch (Exception e) {
      return null;
    }
  }

  private static LocalDate parseDateFromIsoInstant(String isoInstant) {
    if (isBlank(isoInstant)) return null;
    try {
      return OffsetDateTime.parse(isoInstant).toLocalDate();
    } catch (Exception e) {
      return null;
    }
  }

  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }

  /** Helper class to track user changes */
  private static class UserChangeInfo {
    boolean isNew = false;
    Set<String> changedFields = new HashSet<>();
  }
}