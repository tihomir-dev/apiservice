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

@Service
public class UserSyncService {

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

  public Map<String, Object> syncAllUsers() {
    int fetched = 0;
    int actualChanges = 0;
    int skipped = 0;

    //  Load ALL users from db at the start

    Map<String, Map<String, Object>> dbUsers = loadAllUsersFromDb();
    System.out.println("Loaded " + dbUsers.size() + " users from DB");

    int startIndex = 1;
    int pageSize = 100;

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

          // Check if user changed
          boolean hasChanged =
              userHasChanged(
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

          if (!hasChanged) {
            skipped++;
            continue;
          }

          // User changed, do merge
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

          actualChanges++;
        }

        startIndex += resources.size();
        int totalResults = root.path("totalResults").asInt(-1);
        if (totalResults >= 0 && startIndex > totalResults) break;

      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to parse/merge IAS users page startIndex=" + startIndex, e);
      }
    }

    Map<String, Object> out = new HashMap<>();
    out.put("fetched", fetched);
    out.put("upserted", actualChanges);
    out.put("skipped", skipped);

    // notify if changes
    if (actualChanges > 0) {
      syncNotificationService.notifyUserSync(out);
    }

    return out;
  }

  /** Load all users from DB into memory once. */
  private Map<String, Map<String, Object>> loadAllUsersFromDb() {
    String query =
        """
        SELECT "ID", "EMAIL", "LAST_NAME", "FIRST_NAME", "USER_TYPE", "STATUS",
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
      System.err.println("Failed to load users from DB: " + e.getMessage());
      return new HashMap<>(); // empty map, will treat all as new
    }

    return users;
  }

  /** Check if user changed - */
  private boolean userHasChanged(
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

    if (!dbUsers.containsKey(id)) {
      return true; // New user
    }

    Map<String, Object> dbUser = dbUsers.get(id);

    // string comparisons
    if (!nullSafeEquals(lastName, dbUser.get("LAST_NAME"))) return true;
    if (!nullSafeEquals(firstName, dbUser.get("FIRST_NAME"))) return true;
    if (!nullSafeEquals(userType, dbUser.get("USER_TYPE"))) return true;
    if (!nullSafeEquals(status, dbUser.get("STATUS"))) return true;
    if (!nullSafeEquals(company, dbUser.get("COMPANY"))) return true;
    if (!nullSafeEquals(country, dbUser.get("COUNTRY"))) return true;
    if (!nullSafeEquals(city, dbUser.get("CITY"))) return true;

    // DATE comparisons - convert DB string to LocalDate
    LocalDate dbValidFrom = parseDbDate(dbUser.get("VALID_FROM"));
    if (!nullSafeEquals(validFrom, dbValidFrom)) return true;

    LocalDate dbValidTo = parseDbDate(dbUser.get("VALID_TO"));
    if (!nullSafeEquals(validTo, dbValidTo)) return true;

    return false; // No changes
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
}
