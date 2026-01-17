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
public class GroupRepository {

  private static final Logger log = LoggerFactory.getLogger(GroupRepository.class);
  private final JdbcTemplate jdbc;

  public GroupRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Find group by ID
   */
  public Optional<Map<String, Object>> findById(String id) {
    List<Map<String, Object>> rows =
        jdbc.queryForList("SELECT * FROM USER_GROUPS WHERE ID = ?", id);
    return rows.stream().findFirst();
  }

  /**
   * Find group by name
   */
  public Optional<Map<String, Object>> findByName(String name) {
    List<Map<String, Object>> rows =
        jdbc.queryForList("SELECT * FROM USER_GROUPS WHERE NAME = ?", name);
    return rows.stream().findFirst();
  }

  /**
   * Find all groups with pagination and search
   */
  public List<Map<String, Object>> findGroups(
      int startIndex,
      int count,
      String search
  ) {
    SqlAndParams sp = buildGroupsQuery(false, startIndex, count, search);
    
    log.info("=== FIND GROUPS DEBUG ===");
    log.info("SQL: {}", sp.sql);
    log.info("Params: {}", sp.params);
    
    List<Map<String, Object>> results = jdbc.queryForList(sp.sql, sp.params.toArray());
    
    log.info("Results returned: {}", results.size());
    log.info("=========================");
    
    return results;
  }

  /**
   * Count groups with optional search
   */
  public int countGroups(String search) {
    SqlAndParams sp = buildGroupsQuery(true, 1, 1, search);
    
    log.info("=== COUNT GROUPS DEBUG ===");
    log.info("SQL: {}", sp.sql);
    log.info("Params: {}", sp.params);
    
    Integer c = jdbc.queryForObject(sp.sql, Integer.class, sp.params.toArray());
    int count = c == null ? 0 : c;
    
    log.info("Count: {}", count);
    log.info("==========================");
    
    return count;
  }

  /**
   * Build query for finding/counting groups
   */
  private SqlAndParams buildGroupsQuery(
      boolean countOnly,
      int startIndex,
      int count,
      String search
  ) {
    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    if (countOnly) {
      sql.append("SELECT COUNT(*) FROM USER_GROUPS");
    } else {
      sql.append("SELECT * FROM USER_GROUPS");
    }
    
    sql.append(" WHERE 1=1");

    // Search across name and display name
    if (search != null && !search.trim().isEmpty()) {
      String searchPattern = "%" + search.trim().toLowerCase(Locale.ROOT) + "%";
      sql.append(" AND (");
      sql.append("LOWER(NAME) LIKE ? OR ");
      sql.append("LOWER(DISPLAY_NAME) LIKE ? OR ");
      sql.append("LOWER(DESCRIPTION) LIKE ?");
      sql.append(")");
      
      params.add(searchPattern);
      params.add(searchPattern);
      params.add(searchPattern);
      
      log.debug("Adding search filter: {}", searchPattern);
    }

    if (!countOnly) {
      int offset = Math.max(0, startIndex - 1);
      int limit = Math.max(1, Math.min(count, 200));
      
      sql.append(" ORDER BY CREATED_AT DESC");
      sql.append(" LIMIT ").append(limit);
      sql.append(" OFFSET ").append(offset);
    }

    return new SqlAndParams(sql.toString(), params);
  }

  /**
   * Insert a new group
   */
  public void insertGroup(Map<String, Object> group) {
    String sql = """
        INSERT INTO USER_GROUPS (
            ID,
            NAME,
            DISPLAY_NAME,
            DESCRIPTION,
            IAS_LAST_MODIFIED,
            CREATED_AT,
            UPDATED_AT
        ) VALUES (?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)
        """;

    jdbc.update(sql,
        group.get("id"),
        group.get("name"),
        group.get("displayName"),
        group.get("description"),
        group.get("iasLastModified")
    );

    log.info("Group inserted successfully: ID={}, name={}", 
        group.get("id"), group.get("name"));
  }

  /**
   * Update a group
   */
  public void updateGroup(String id, Map<String, Object> updates) {
    StringBuilder sql = new StringBuilder("UPDATE USER_GROUPS SET ");
    List<Object> params = new ArrayList<>();
    
    boolean first = true;
    
    if (updates.containsKey("name")) {
      if (!first) sql.append(", ");
      sql.append("NAME = ?");
      params.add(updates.get("name"));
      first = false;
    }
    
    if (updates.containsKey("displayName")) {
      if (!first) sql.append(", ");
      sql.append("DISPLAY_NAME = ?");
      params.add(updates.get("displayName"));
      first = false;
    }
    
    if (updates.containsKey("description")) {
      if (!first) sql.append(", ");
      sql.append("DESCRIPTION = ?");
      params.add(updates.get("description"));
      first = false;
    }
    
    if (updates.containsKey("iasLastModified")) {
      if (!first) sql.append(", ");
      sql.append("IAS_LAST_MODIFIED = ?");
      params.add(updates.get("iasLastModified"));
      first = false;
    }
    
    // Always update timestamp
    if (!first) sql.append(", ");
    sql.append("UPDATED_AT = CURRENT_UTCTIMESTAMP");
    
    sql.append(" WHERE ID = ?");
    params.add(id);
    
    int rowsAffected = jdbc.update(sql.toString(), params.toArray());
    
    if (rowsAffected == 0) {
      throw new RuntimeException("Group not found with ID: " + id);
    }
    
    log.info("Group updated successfully: ID={}", id);
  }

  /**
   * Delete a group
   */
  public int deleteGroup(String id) {
    String sql = "DELETE FROM USER_GROUPS WHERE ID = ?";
    int rowsAffected = jdbc.update(sql, id);
    log.info("Group deleted: ID={}, rows affected={}", id, rowsAffected);
    return rowsAffected;
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