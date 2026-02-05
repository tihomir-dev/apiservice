package customer.apiservice.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import customer.apiservice.db.GroupMemberRepository;
import customer.apiservice.ias.ScimClient;
import java.net.http.HttpResponse;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserGroupAssignmentService {

  private static final Logger log = LoggerFactory.getLogger(UserGroupAssignmentService.class);

  private final ScimClient scimClient;
  private final GroupMemberRepository groupMemberRepository;
  private final SyncNotificationService syncNotificationService;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public UserGroupAssignmentService(
      ScimClient scimClient,
      GroupMemberRepository groupMemberRepository,
      SyncNotificationService syncNotificationService) {
    this.scimClient = scimClient;
    this.groupMemberRepository = groupMemberRepository;
    this.syncNotificationService = syncNotificationService;
  }

  /**
   * Sync groups assigned to each user from IAS to DB Fetches all users and their group memberships,
   * then updates the database
   */
  @Transactional
  public Map<String, Object> syncUserGroupAssignments() {
    log.info("=== Starting user group assignments sync from IAS ===");

    try {
      // Load all current user-group assignments from DB
      Map<String, List<String>> dbUserGroups = loadAllUserGroupAssignmentsFromDb();
      List<Map<String, Object>> detailedChanges = new ArrayList<>();
      log.info("Loaded user-group assignments for {} users from DB", dbUserGroups.size());

      HttpResponse<String> response = scimClient.getUsers("");

      if (response.statusCode() != 200) {
        throw new RuntimeException(
            "Failed to get users from IAS: " + response.statusCode() + " - " + response.body());
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> iasData = objectMapper.readValue(response.body(), Map.class);

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> iasUsers = (List<Map<String, Object>>) iasData.get("Resources");

      if (iasUsers == null) {
        iasUsers = new ArrayList<>();
      }

      int totalProcessed = 0;
      int actualChanges = 0;
      int errors = 0;

      for (Map<String, Object> iasUser : iasUsers) {
        try {
          String userId = (String) iasUser.get("id");

          @SuppressWarnings("unchecked")
          List<Map<String, Object>> iasGroups = (List<Map<String, Object>>) iasUser.get("groups");

          List<String> iasGroupIds = new ArrayList<>();
          if (iasGroups != null) {
            for (Map<String, Object> group : iasGroups) {
              String groupId = (String) group.get("value");
              if (groupId != null) {
                iasGroupIds.add(groupId);
              }
            }
          }

          // Check if user's group assignments changed
          List<String> dbGroupIds = dbUserGroups.getOrDefault(userId, new ArrayList<>());

          if (!groupAssignmentsMatch(iasGroupIds, dbGroupIds)) {
            // Assignments changed - remove old and add new
            Map<String, Object> change = new HashMap<>();
            change.put("userId", userId);
            change.put("previousGroups", new ArrayList<>(dbGroupIds));
            change.put("newGroups", new ArrayList<>(iasGroupIds));
            change.put("timestamp", System.currentTimeMillis());
            detailedChanges.add(change);
            groupMemberRepository.removeUserFromAllGroups(userId);

            for (String groupId : iasGroupIds) {
              groupMemberRepository.addMember(groupId, userId);
            }

            actualChanges++;
            log.debug("Updated group assignments for user: {}", userId);
          }

          totalProcessed++;

        } catch (Exception e) {
          log.error("Error syncing group assignments for user", e);
          errors++;
        }
      }

      log.info(
          "=== User group assignments sync completed: processed={}, changes={}, errors={} ===",
          totalProcessed,
          actualChanges,
          errors);

      Map<String, Object> result =
          Map.of(
              "success", true,
              "totalUsers", totalProcessed,
              "assignmentChanges", actualChanges,
              "failed", errors,
              "detailedChanges", detailedChanges);

      if (actualChanges > 0) {
        syncNotificationService.notifyUserGroupAssignmentSync(result);
      }

      return result;

    } catch (Exception e) {
      log.error("Failed to sync user group assignments from IAS", e);
      return Map.of("success", false, "error", e.getMessage());
    }
  }

  /**
   * Sync members assigned to each group from IAS to DB Fetches all groups and their members, then
   * updates the database
   */
  @Transactional
  public Map<String, Object> syncGroupMembers() {
    log.info("=== Starting group members sync from IAS ===");

    try {
      HttpResponse<String> response = scimClient.getGroups();

      if (response.statusCode() != 200) {
        throw new RuntimeException(
            "Failed to get groups from IAS: " + response.statusCode() + " - " + response.body());
      }

      @SuppressWarnings("unchecked")
      Map<String, Object> iasData = objectMapper.readValue(response.body(), Map.class);

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> iasGroups = (List<Map<String, Object>>) iasData.get("Resources");

      if (iasGroups == null) {
        iasGroups = new ArrayList<>();
      }

      // Load all current group members from DB
      Map<String, List<String>> dbGroupMembers = loadAllGroupMembersFromDb();
      log.info("Loaded members for {} groups from DB", dbGroupMembers.size());

      int totalGroups = 0;
      int actualChanges = 0;
      int errors = 0;

      List<Map<String, Object>> detailedChanges = new ArrayList<>();

      for (Map<String, Object> iasGroup : iasGroups) {
        try {
          String groupId = (String) iasGroup.get("id");

          @SuppressWarnings("unchecked")
          List<Map<String, Object>> iasMembers =
              (List<Map<String, Object>>) iasGroup.get("members");

          List<String> iasMemberIds = new ArrayList<>();
          if (iasMembers != null) {
            for (Map<String, Object> member : iasMembers) {
              String memberId = (String) member.get("value");
              if (memberId != null) {
                iasMemberIds.add(memberId);
              }
            }
          }

          // Check if group's members changed
          List<String> dbMemberIds = dbGroupMembers.getOrDefault(groupId, new ArrayList<>());

          if (!groupAssignmentsMatch(iasMemberIds, dbMemberIds)) {
            List<String> membersAdded = new ArrayList<>(iasMemberIds);
            membersAdded.removeAll(dbMemberIds);

            List<String> membersRemoved = new ArrayList<>(dbMemberIds);
            membersRemoved.removeAll(iasMemberIds);

            Map<String, Object> change = new HashMap<>();
            change.put("groupId", groupId);
            change.put("previousMembers", new ArrayList<>(dbMemberIds));
            change.put("newMembers", new ArrayList<>(iasMemberIds));
            change.put("membersAdded", membersAdded);
            change.put("membersRemoved", membersRemoved);
            change.put("timestamp", System.currentTimeMillis());
            detailedChanges.add(change);

            // Members changed - remove old and add new
            groupMemberRepository.removeAllMembers(groupId);

            for (String memberId : iasMemberIds) {
              groupMemberRepository.addMember(groupId, memberId);
            }

            actualChanges++;
            log.debug(
                "Updated members for group: {} (added: {}, removed: {})",
                groupId,
                membersAdded.size(),
                membersRemoved.size());
          }

          totalGroups++;

        } catch (Exception e) {
          log.error("Error syncing members for group", e);
          errors++;
        }
      }

      log.info(
          "=== Group members sync completed: totalGroups={}, changes={}, errors={} ===",
          totalGroups,
          actualChanges,
          errors);

      Map<String, Object> result =
          Map.of(
              "success", true,
              "totalGroups", totalGroups,
              "membershipChanges", actualChanges,
              "failed", errors,
              "detailedChanges", detailedChanges);

      if (actualChanges > 0) {
        syncNotificationService.notifyGroupMembersSync(result);
      }

      return result;

    } catch (Exception e) {
      log.error("Failed to sync group members from IAS", e);
      return Map.of("success", false, "error", e.getMessage());
    }
  }

  /** Returns a map of userId - List of groupIds */
  private Map<String, List<String>> loadAllUserGroupAssignmentsFromDb() {
    Map<String, List<String>> userGroups = new HashMap<>();

    try {
      List<Map<String, Object>> rows = groupMemberRepository.getAllUserGroupAssignments();
      for (Map<String, Object> row : rows) {
        String userId = (String) row.get("USER_ID");
        String groupId = (String) row.get("GROUP_ID");

        if (!userGroups.containsKey(userId)) {
          userGroups.put(userId, new ArrayList<>());
        }
        userGroups.get(userId).add(groupId);
      }
    } catch (Exception e) {
      log.warn("Failed to load user-group assignments from DB: {}", e.getMessage());
      return new HashMap<>();
    }

    return userGroups;
  }

  /** Returns a map of groupId - List of memberIds */
  private Map<String, List<String>> loadAllGroupMembersFromDb() {
    Map<String, List<String>> groupMembers = new HashMap<>();

    try {
      List<Map<String, Object>> rows = groupMemberRepository.getAllGroupMembers();
      for (Map<String, Object> row : rows) {
        String groupId = (String) row.get("GROUP_ID");
        String memberId = (String) row.get("USER_ID");

        if (!groupMembers.containsKey(groupId)) {
          groupMembers.put(groupId, new ArrayList<>());
        }
        groupMembers.get(groupId).add(memberId);
      }
    } catch (Exception e) {
      log.warn("Failed to load group members from DB: {}", e.getMessage());
      return new HashMap<>();
    }

    return groupMembers;
  }

  /** Compare two lists of IDs to see if they match */
  private boolean groupAssignmentsMatch(List<String> iasIds, List<String> dbIds) {
    if (iasIds.size() != dbIds.size()) {
      return false;
    }

    for (String id : iasIds) {
      if (!dbIds.contains(id)) {
        return false;
      }
    }

    return true;
  }
}
