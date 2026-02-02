package customer.apiservice.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class GroupMemberRepository {

  private static final Logger log = LoggerFactory.getLogger(GroupMemberRepository.class);
  private final JdbcTemplate jdbc;

  public GroupMemberRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Get all members of a group
   */
  public List<Map<String, Object>> findMembersByGroupId(String groupId) {
    String sql = """
        SELECT 
          u.ID,
          u.LOGIN_NAME,
          u.EMAIL,
          u.FIRST_NAME,
          u.LAST_NAME,
          u.STATUS,
          gm.CREATED_AT as JOINED_AT
        FROM GROUP_MEMBERS gm
        INNER JOIN USERS u ON gm.USER_ID = u.ID
        WHERE gm.GROUP_ID = ?
        ORDER BY gm.CREATED_AT DESC
        """;
    
    List<Map<String, Object>> results = jdbc.queryForList(sql, groupId);
    log.info("Found {} members for group {}", results.size(), groupId);
    return results;
  }

  /**
   * Get all groups for a user
   */
  public List<Map<String, Object>> findGroupsByUserId(String userId) {
    String sql = """
        SELECT 
          g.ID,
          g.NAME,
          g.DISPLAY_NAME,
          g.DESCRIPTION,
          gm.CREATED_AT as JOINED_AT
        FROM GROUP_MEMBERS gm
        INNER JOIN USER_GROUPS g ON gm.GROUP_ID = g.ID
        WHERE gm.USER_ID = ?
        ORDER BY gm.CREATED_AT DESC
        """;
    
    List<Map<String, Object>> results = jdbc.queryForList(sql, userId);
    log.info("Found {} groups for user {}", results.size(), userId);
    return results;
  }

  /**
   * Add a user to a group
   */
  public void addMember(String groupId, String userId) {
    String sql = """
        INSERT INTO GROUP_MEMBERS (GROUP_ID, USER_ID, CREATED_AT)
        VALUES (?, ?, CURRENT_UTCTIMESTAMP)
        """;
    
    jdbc.update(sql, groupId, userId);
    log.info("Added user {} to group {}", userId, groupId);
  }

  /**
   * Remove a user from a group
   */
  public int removeMember(String groupId, String userId) {
    String sql = "DELETE FROM GROUP_MEMBERS WHERE GROUP_ID = ? AND USER_ID = ?";
    int rowsAffected = jdbc.update(sql, groupId, userId);
    log.info("Removed user {} from group {}, rows affected={}", userId, groupId, rowsAffected);
    return rowsAffected; 
  }
  
  public void removeMembers(String groupId, List<String> userIds) {
  String sql = "DELETE FROM GROUP_MEMBERS WHERE GROUP_ID = ? AND USER_ID = ?";
  
  for (String userId : userIds) {
    jdbc.update(sql, groupId, userId);
    log.info("Removed user {} from group {}", userId, groupId);
  }
}

  /**
   * Remove all members from a group (used before deleting group)
   */
  public int removeAllMembers(String groupId) {
    String sql = "DELETE FROM GROUP_MEMBERS WHERE GROUP_ID = ?";
    int rowsAffected = jdbc.update(sql, groupId);
    log.info("Removed all members from group {}, rows affected={}", groupId, rowsAffected);
    return rowsAffected;
  }

  /**
   * Check if a user is a member of a group
   */
  public boolean isMember(String groupId, String userId) {
    String sql = "SELECT COUNT(*) FROM GROUP_MEMBERS WHERE GROUP_ID = ? AND USER_ID = ?";
    Integer count = jdbc.queryForObject(sql, Integer.class, groupId, userId);
    return count != null && count > 0;
  }
}