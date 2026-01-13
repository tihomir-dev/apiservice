package customer.apiservice.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Repository
public class UserRepository {

  private static final Logger log = LoggerFactory.getLogger(UserRepository.class);
  private final JdbcTemplate jdbc;

  public UserRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public Optional<Map<String, Object>> findById(String id) {
    List<Map<String, Object>> rows =
        jdbc.queryForList("SELECT * FROM USERS WHERE ID = ?", id);
    return rows.stream().findFirst();
  }

  public List<Map<String, Object>> findUsers(
      int startIndex,
      int count,
      String search,
      String email,
      String status,
      String userType
  ) {
    SqlAndParams sp = buildUsersQuery(false, startIndex, count, search, email, status, userType);
    
    log.info("=== FIND USERS DEBUG ===");
    log.info("SQL: {}", sp.sql);
    log.info("Params: {}", sp.params);
    log.info("Param count: {}", sp.params.size());
    
    List<Map<String, Object>> results = jdbc.queryForList(sp.sql, sp.params.toArray());
    
    log.info("Results returned: {}", results.size());
    log.info("========================");
    
    return results;
  }

  public int countUsers(String search, String email, String status, String userType) {
    SqlAndParams sp = buildUsersQuery(true, 1, 1, search, email, status, userType);
    
    log.info("=== COUNT USERS DEBUG ===");
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
      String userType
  ) {
    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    if (countOnly) {
      sql.append("SELECT COUNT(*) FROM USERS");
    } else {
      sql.append("SELECT * FROM USERS");
    }
    
    // Start with WHERE 1=1 to make appending conditions easier
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
}