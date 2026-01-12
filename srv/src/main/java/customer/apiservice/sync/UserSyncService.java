package customer.apiservice.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import customer.apiservice.ias.ScimClient;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.net.http.HttpResponse;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
public class UserSyncService {

  private static final String SAP_EXT = "urn:ietf:params:scim:schemas:extension:sap:2.0:User";
  private static final String ENT_EXT = "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User";

  // DB requires USER_TYPE NOT NULL
  private static final String DEFAULT_USER_TYPE = "employee";

  private final ScimClient scim;
  private final JdbcTemplate jdbc;
  private final ObjectMapper om = new ObjectMapper();

  public UserSyncService(ScimClient scim, JdbcTemplate jdbc) {
    this.scim = scim;
    this.jdbc = jdbc;
  }

  public Map<String, Object> syncAllUsers() {
    int fetched = 0;
    int upserted = 0;
    int skipped = 0;

    int startIndex = 1;   // SCIM is 1-based
    int pageSize = 100;

    while (true) {
      HttpResponse<String> resp = scim.getUsers("startIndex=" + startIndex + "&count=" + pageSize);

      if (resp.statusCode() / 100 != 2) {
        throw new RuntimeException("IAS SCIM /Users failed: " + resp.statusCode() + " body=" + resp.body());
      }

      try {
        JsonNode root = om.readTree(resp.body());
        JsonNode resources = root.path("Resources");

        if (!resources.isArray() || resources.size() == 0) break;

        for (JsonNode u : resources) {
          fetched++;

          // --- Required/Key DB fields ---
          String id = text(u, "id");

          String loginName = text(u, "userName");
          String email = firstEmailCoreOrSap(u);

          String lastName = text(u.path("name"), "familyName");
          String firstName = text(u.path("name"), "givenName");

          String userType = text(u, "userType");
          if (isBlank(userType)) userType = DEFAULT_USER_TYPE;

          String status = statusFromCoreOrSap(u); // ACTIVE/INACTIVE

          // --- Optional DB fields ---
          LocalDate validFrom = parseDateFromIsoInstant(text(u.path(SAP_EXT), "validFrom"));
          LocalDate validTo = parseDateFromIsoInstant(text(u.path(SAP_EXT), "validTo"));

          // company: in your payload it’s enterprise extension organization
          String company = text(u.path(ENT_EXT), "organization");

          Address addr = pickAddressCoreOrSap(u);
          String country = addr.country();
          String city = addr.city();

          Timestamp iasLastModified = parseTimestamp(text(u.path("meta"), "lastModified"));

          // Fix-up: IAS sometimes returns empty userName; for DB LOGIN_NAME NOT NULL we fallback to email
          if (isBlank(loginName)) loginName = email;

          // Validate DB required fields; skip bad records rather than failing the whole sync
          if (isBlank(id) || isBlank(loginName) || isBlank(email) || isBlank(lastName) || isBlank(userType) || isBlank(status)) {
            skipped++;
            continue;
          }

          jdbc.update(MERGE_USERS_SQL,
              // UPDATE: id then values
              id,
              loginName, email, lastName, userType,
              firstName, validFrom, validTo,
              company, country, city,
              iasLastModified, status,

              // INSERT values
              id,
              loginName, email, lastName, userType,
              firstName, validFrom, validTo,
              company, country, city,
              iasLastModified, status
          );

          upserted++;
        }

        startIndex += resources.size();

        int totalResults = root.path("totalResults").asInt(-1);
        if (totalResults >= 0 && startIndex > totalResults) break;

      } catch (Exception e) {
        throw new RuntimeException("Failed to parse/merge IAS users page startIndex=" + startIndex, e);
      }
    }

    Map<String, Object> out = new HashMap<>();
    out.put("fetched", fetched);
    out.put("upserted", upserted);
    out.put("skipped", skipped);
    return out;
  }

  private static final String MERGE_USERS_SQL = """
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

  // ---------- Mapping helpers ----------

  private static String firstEmailCoreOrSap(JsonNode user) {
    // core emails
    String core = firstEmailFromArray(user.path("emails"));
    if (!isBlank(core)) return core;

    // SAP extension emails
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
    // Prefer core boolean "active"
    JsonNode activeNode = user.get("active");
    if (activeNode != null && !activeNode.isNull()) {
      boolean active = activeNode.asBoolean(true);
      return active ? "ACTIVE" : "INACTIVE";
    }

    // fallback: SAP extension "status": "active"/"inactive"
    String extStatus = text(user.path(SAP_EXT), "status");
    if (!isBlank(extStatus) && "inactive".equalsIgnoreCase(extStatus)) return "INACTIVE";
    return "ACTIVE";
  }

  private static Address pickAddressCoreOrSap(JsonNode user) {
    // Prefer core addresses first
    Address core = pickAddressFromArray(user.path("addresses"));
    if (!core.isEmpty()) return core;

    // fallback to SAP extension addresses
    Address ext = pickAddressFromArray(user.path(SAP_EXT).path("addresses"));
    if (!ext.isEmpty()) return ext;

    return new Address(null, null);
  }

  private static Address pickAddressFromArray(JsonNode addresses) {
    if (addresses == null || !addresses.isArray() || addresses.size() == 0) return new Address(null, null);

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
    boolean isEmpty() { return isBlank(country) && isBlank(city); }
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
      // validFrom/validTo are like 2026-01-12T00:00:00Z => date part
      return OffsetDateTime.parse(isoInstant).toLocalDate();
    } catch (Exception e) {
      return null;
    }
  }

  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }
}
