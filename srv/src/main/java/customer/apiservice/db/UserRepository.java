package customer.apiservice.db;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class UserRepository {

  private static final Logger log = LoggerFactory.getLogger(UserRepository.class);
  private final JdbcTemplate jdbc;
  private final GroupMemberRepository groupMemberRepository;

  public UserRepository(JdbcTemplate jdbc, GroupMemberRepository groupMemberRepository) {
    this.jdbc = jdbc;
    this.groupMemberRepository = groupMemberRepository;
  }

  public Optional<Map<String, Object>> findById(String id) {
    List<Map<String, Object>> rows = jdbc.queryForList("SELECT * FROM USERS WHERE ID = ?", id);
    return rows.stream().findFirst();
  }

  public List<Map<String, Object>> findUsers(
      int startIndex,
      int count,
      String search,
      String email,
      String status,
      String userType,
      String country) {
    SqlAndParams sp =
        buildUsersQuery(false, startIndex, count, search, email, status, userType, country);

    log.info("=== FIND USERS DEBUG ===");
    log.info("SQL: {}", sp.sql);
    log.info("Params: {}", sp.params);
    log.info("Param count: {}", sp.params.size());

    List<Map<String, Object>> results = jdbc.queryForList(sp.sql, sp.params.toArray());

    log.info("Results returned: {}", results.size());
    log.info("========================");

    return results;
  }

  public int countUsers(
      String search, String email, String status, String userType, String country) {
    SqlAndParams sp = buildUsersQuery(true, 1, 1, search, email, status, userType, country);

    log.info("=== COUNT USERS DEBUG ===");
    //REVIEW: Logging direct SQL can be considered a vulnerability and opens up a possibility of a breach
    log.info("SQL: {}", sp.sql);
    log.info("Params: {}", sp.params);

    Integer c = jdbc.queryForObject(sp.sql, Integer.class, sp.params.toArray());
    int count = c == null ? 0 : c;

    log.info("Count: {}", count);
    log.info("=========================");

    return count;
  }

  private SqlAndParams buildUsersQuery(
      boolean countOnly,
      int startIndex,
      int count,
      String search,
      String email,
      String status,
      String userType,
      String country) {
    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    if (countOnly) {
      sql.append("SELECT COUNT(*) FROM USERS");
    } else {
      sql.append("SELECT * FROM USERS");
    }

    sql.append(" WHERE 1=1");

    // Filter by exact email
    if (email != null && !email.trim().isEmpty()) {
      sql.append(" AND LOWER(EMAIL) = LOWER(?)");
      params.add(email.trim());
      log.debug("Adding email filter: {}", email.trim());
    }

    // Filter by exact status
    if (status != null && !status.trim().isEmpty()) {
      sql.append(" AND UPPER(STATUS) = UPPER(?)");
      params.add(status.trim());
      log.debug("Adding status filter: {}", status.trim());
    }

    // Filter by exact user type
    if (userType != null && !userType.trim().isEmpty()) {
      sql.append(" AND LOWER(USER_TYPE) = LOWER(?)");
      params.add(userType.trim());
      log.debug("Adding userType filter: {}", userType.trim());
    }

    if (country != null && !country.trim().isEmpty()) {
      sql.append(" AND UPPER(COUNTRY) = UPPER(?)");
      params.add(country.trim());
      log.debug("Adding country filter: {}", country.trim());
    }

    // Search across multiple columns
    if (search != null && !search.trim().isEmpty()) {
      String searchPattern = "%" + search.trim().toLowerCase(Locale.ROOT) + "%";
      sql.append(" AND (");
      sql.append("LOWER(LOGIN_NAME) LIKE ? OR ");
      sql.append("LOWER(EMAIL) LIKE ? OR ");
      sql.append("LOWER(FIRST_NAME) LIKE ? OR ");
      sql.append("LOWER(LAST_NAME) LIKE ?");
      sql.append(")");

      // Add the search pattern 4 times (once for each column)
      params.add(searchPattern);
      params.add(searchPattern);
      params.add(searchPattern);
      params.add(searchPattern);

      log.debug("Adding search filter: {}", searchPattern);
    }

    // Add ordering
    if (!countOnly) {
      int offset = Math.max(0, startIndex - 1);
      int limit = Math.max(1, Math.min(count, 200));

      sql.append(" ORDER BY CREATED_AT DESC");
      sql.append(" LIMIT ").append(limit);
      sql.append(" OFFSET ").append(offset);
    }

    return new SqlAndParams(sql.toString(), params);
  }

  private static class SqlAndParams {
    final String sql;
    final List<Object> params;

    SqlAndParams(String sql, List<Object> params) {
      this.sql = sql;
      this.params = params;
    }
  }

  /** Inserts a new user into the database Uses the ID generated by IAS */
  public void insertUser(Map<String, Object> user) {
    String sql =
        """
        INSERT INTO "USERS" (
            "ID",
            "LOGIN_NAME",
            "EMAIL",
            "LAST_NAME",
            "USER_TYPE",
            "STATUS",
            "FIRST_NAME",
            "VALID_FROM",
            "VALID_TO",
            "COMPANY",
            "COUNTRY",
            "CITY",
            "IAS_LAST_MODIFIED",
            "CREATED_AT",
            "UPDATED_AT"
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)
        """;

    jdbc.update(
        sql,
        user.get("id"), // ID from IAS
        user.get("loginName"), // LOGIN_NAME
        user.get("email"), // EMAIL
        user.get("lastName"), // LAST_NAME
        user.get("userType"), // USER_TYPE
        user.get("status"), // STATUS
        user.get("firstName"), // FIRST_NAME
        user.get("validFrom"), // VALID_FROM
        user.get("validTo"), // VALID_TO
        user.get("company"), // COMPANY
        user.get("country"), // COUNTRY
        user.get("city"), // CITY
        user.get("iasLastModified") // IAS_LAST_MODIFIED
        );

    log.info(
        "User inserted successfully: ID={}, loginName={}, email={}",
        user.get("id"),
        user.get("loginName"),
        user.get("email"));
  }

  /** Update a user in the database */
  public void updateUser(String id, Map<String, Object> updates) {
    StringBuilder sql = new StringBuilder("UPDATE USERS SET ");
    List<Object> params = new ArrayList<>();

    boolean first = true;
    //REVIEW: can use the Map logic here too to modularize and simplify the method
    // Handle loginName
    if (updates.containsKey("loginName")) {
      if (!first) sql.append(", ");
      sql.append("LOGIN_NAME = ?");
      params.add(updates.get("loginName"));
      first = false;
    }

    // Handle lastName
    if (updates.containsKey("lastName")) {
      if (!first) sql.append(", ");
      sql.append("LAST_NAME = ?");
      params.add(updates.get("lastName"));
      first = false;
    }

    // Handle firstName
    if (updates.containsKey("firstName")) {
      if (!first) sql.append(", ");
      sql.append("FIRST_NAME = ?");
      params.add(updates.get("firstName"));
      first = false;
    }

    // Handle email
    if (updates.containsKey("email")) {
      if (!first) sql.append(", ");
      sql.append("EMAIL = ?");
      params.add(updates.get("email"));
      first = false;
    }

    // Handle status
    if (updates.containsKey("status")) {
      if (!first) sql.append(", ");
      sql.append("STATUS = ?");
      params.add(updates.get("status"));
      first = false;
    }

    // Handle userType
    if (updates.containsKey("userType")) {
      if (!first) sql.append(", ");
      sql.append("USER_TYPE = ?");
      params.add(updates.get("userType"));
      first = false;
    }

    // Handle company
    if (updates.containsKey("company")) {
      if (!first) sql.append(", ");
      sql.append("COMPANY = ?");
      params.add(updates.get("company"));
      first = false;
    }

    // Handle city
    if (updates.containsKey("city")) {
      if (!first) sql.append(", ");
      sql.append("CITY = ?");
      params.add(updates.get("city"));
      first = false;
    }

    // Handle country
    if (updates.containsKey("country")) {
      if (!first) sql.append(", ");
      sql.append("COUNTRY = ?");
      params.add(updates.get("country"));
      first = false;
    }

    if (updates.containsKey("validFrom")) {
      Object validFromValue = updates.get("validFrom");
      if (validFromValue != null) {
        try {
          if (!first) sql.append(", ");
          sql.append("VALID_FROM = ?");

          //REVIEW: Parse this as instant and then to timestamp - currently you're risking a DST bug
          // Parse ISO8601 string "2026-01-12T00:00:00Z"
          String dateStr = String.valueOf(validFromValue).replace("Z", "");
          LocalDateTime parsed =
              LocalDateTime.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
          params.add(Timestamp.valueOf(parsed));

          first = false;
        } catch (Exception e) {
          throw new IllegalArgumentException(
              "Invalid validFrom date format: "
                  + validFromValue
                  + ". Expected format: YYYY-MM-DDTHH:mm:ssZ",
              e);
        }
      }
    }
    if (updates.containsKey("validTo")) {
      Object validToValue = updates.get("validTo");
      if (validToValue != null) {
        try {
          if (!first) sql.append(", ");
          sql.append("VALID_TO = ?");

          // Parse ISO8601 string "2026-08-19T00:00:00Z"
          String dateStr = String.valueOf(validToValue).replace("Z", "");
          LocalDateTime parsed =
              LocalDateTime.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
          params.add(Timestamp.valueOf(parsed));

          first = false;
        } catch (Exception e) {
          throw new IllegalArgumentException(
              "Invalid validTo date format: "
                  + validToValue
                  + ". Expected format: YYYY-MM-DDTHH:mm:ssZ",
              e);
        }
      }
    }

    if (!first) sql.append(", ");
    sql.append("UPDATED_AT = CURRENT_TIMESTAMP");

    sql.append(" WHERE ID = ?");
    params.add(id);

    // run update
    int rowsAffected = jdbc.update(sql.toString(), params.toArray());

    if (rowsAffected == 0) {
      throw new RuntimeException("User not found with ID: " + id);
    }
  }

  /** Delete a user from the database */
  public int deleteUser(String id) {
    String sql = "DELETE FROM USERS WHERE ID = ?";
    return jdbc.update(sql, id);
  }

  public void addUserToGroup(String userId, String groupId) {
    groupMemberRepository.addMember(groupId, userId);
  }

  /** Remove user from group in database */
  public void removeUserFromGroup(String userId, String groupId) {
    groupMemberRepository.removeMember(groupId, userId);
  }

  /** Check if user is already in group */
  public boolean isUserInGroup(String userId, String groupId) {
    return groupMemberRepository.isMember(groupId, userId);
  }
}
