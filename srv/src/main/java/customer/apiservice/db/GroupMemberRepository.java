package customer.apiservice.db;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class GroupMemberRepository {

  private static final Logger log = LoggerFactory.getLogger(GroupMemberRepository.class);
  private final JdbcTemplate jdbc;

  public GroupMemberRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /** Get all members of a group */
  public List<Map<String, Object>> findMembersByGroupId(String groupId) {

    //REVIEW: Take out sqls into constants, that way we can re-use as needed. Also track if you have repeating SELECTS, WHERE clauses etc so you can mix 
    //and match modularly whatever you may need. Future proofing and easier to use down the line when a DBManager class like this grows
    String sql =
        """
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

    //REVIEW: My advice is to create separate new Object Classes for your use cases - let's say for this case User - Object May cause casting issues
    //and make refactoring harder down the line if we want to build ontop of the existing functionality

    //REVIEW: Wrap all jdbc query calls with your own try/catch in order to have better control over your logic flow and handle errors when
    //the persistance layer folds. Return a human-readable log to notify that the DB layer has ran into a fault for whichever attempted operation
    List<Map<String, Object>> results = jdbc.queryForList(sql, groupId);
    log.info("Found {} members for group {}", results.size(), groupId);
    return results;
  }

  /** Get all groups for a user */
  public List<Map<String, Object>> findGroupsByUserId(String userId) {
    String sql =
        """
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

  /** Add a user to a group */
  public void addMember(String groupId, String userId) {
    String sql =
        """
        INSERT INTO GROUP_MEMBERS (GROUP_ID, USER_ID, CREATED_AT)
        VALUES (?, ?, CURRENT_UTCTIMESTAMP)
        """;
    //REVIEW: You don't check for uniqueness of the inserted user via GROUP_ID and USER_ID. If such a member already exists and you want to update
    //a field there, implement a separate method called updateMember
    jdbc.update(sql, groupId, userId);
    log.info("Added user {} to group {}", userId, groupId);
  }

  /** Remove a user from a group */
  public int removeMember(String groupId, String userId) {
    String sql = "DELETE FROM GROUP_MEMBERS WHERE GROUP_ID = ? AND USER_ID = ?";
    int rowsAffected = jdbc.update(sql, groupId, userId);
    log.info("Removed user {} from group {}, rows affected={}", userId, groupId, rowsAffected);
    return rowsAffected;
  }

  public void removeMembers(String groupId, List<String> userIds) {
    String sql = "DELETE FROM GROUP_MEMBERS WHERE GROUP_ID = ? AND USER_ID = ?";
    //REVIEW: Perform batch calls here - iterating over every user and delegating an entire db request individually can be cumbersome on the service
    // Use IN() and batch a 100 at a time. For the scale of this test project it doesn't really matter, but when scaled to a big use-case, 
    //you can run into resource troubles - Check your other methods for possibility of implementing batch queries
    for (String userId : userIds) {
      jdbc.update(sql, groupId, userId);
      log.info("Removed user {} from group {}", userId, groupId);
    }
  }

  /** Remove all members from a group (used before deleting group) */
  public int removeAllMembers(String groupId) {
    String sql = "DELETE FROM GROUP_MEMBERS WHERE GROUP_ID = ?";
    int rowsAffected = jdbc.update(sql, groupId);
    log.info("Removed all members from group {}, rows affected={}", groupId, rowsAffected);
    return rowsAffected;
  }

  /** Check if a user is a member of a group */
  public boolean isMember(String groupId, String userId) {
    //REVIEW: Use EXISTS rather than count, it's a much more efficient query
    String sql = "SELECT COUNT(*) FROM GROUP_MEMBERS WHERE GROUP_ID = ? AND USER_ID = ?";
    Integer count = jdbc.queryForObject(sql, Integer.class, groupId, userId);
    return count != null && count > 0;
  }

  /** Get all user-to-group assignments from DB Returns list of maps with USER_ID and GROUP_ID */
  public List<Map<String, Object>> getAllUserGroupAssignments() {
    String sql =
        """
        SELECT USER_ID, GROUP_ID
        FROM GROUP_MEMBERS
        """;

    List<Map<String, Object>> results = jdbc.queryForList(sql);
    //REVIEW: After refactoring go over the code and segment your logs into INFO and DEBUG. Determine which logs are worthwhile to always log and which ones
    //only in specialized cases when you want more visibility and turn on debug logging. This way you'll keep your service clean when it comes to logging and not
    //bog down the system with logs that are not always necessary. 
    log.info("Loaded {} user-group assignments from DB", results.size());
    return results;
  }

  /** Get all group members from DB Returns list of maps with GROUP_ID and USER_ID */
  public List<Map<String, Object>> getAllGroupMembers() {
    String sql =
        """
        SELECT GROUP_ID, USER_ID
        FROM GROUP_MEMBERS
        """;

    List<Map<String, Object>> results = jdbc.queryForList(sql);
    log.info("Loaded {} group member assignments from DB", results.size());
    return results;
  }

  /** Remove a user from all groups */
  public void removeUserFromAllGroups(String userId) {
    String sql = "DELETE FROM GROUP_MEMBERS WHERE USER_ID = ?";
    int rowsAffected = jdbc.update(sql, userId);
    log.info("Removed user {} from all groups, rows affected={}", userId, rowsAffected);
  }
}
