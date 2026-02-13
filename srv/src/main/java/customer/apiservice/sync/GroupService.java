package customer.apiservice.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import customer.apiservice.db.GroupMemberRepository;
import customer.apiservice.db.GroupRepository;
import customer.apiservice.ias.ScimClient;
import java.net.http.HttpResponse;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class GroupService {

  private static final String CUSTOM_GROUP_EXT =
      "urn:sap:cloud:scim:schemas:extension:custom:2.0:Group";
  private static final Logger log = LoggerFactory.getLogger(GroupService.class);

  private final ScimClient scimClient;
  private final GroupRepository groupRepository;
  private final GroupMemberRepository groupMemberRepository;
  private final SyncNotificationService syncNotificationService;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public GroupService(
      ScimClient scimClient,
      GroupRepository groupRepository,
      GroupMemberRepository groupMemberRepository,
      SyncNotificationService syncNotificationService) {
    this.scimClient = scimClient;
    this.groupRepository = groupRepository;
    this.groupMemberRepository = groupMemberRepository;
    this.syncNotificationService = syncNotificationService;
  }

  /** Sync all groups from IAS to DB with detailed change tracking and deletion handling */
  @Transactional
  public Map<String, Object> syncGroupsFromIas() {
    log.info("=== Starting group sync from IAS ===");

    int fetched = 0;
    int inserted = 0;
    int updated = 0;
    int deleted = 0;
    int skipped = 0;
    int errors = 0;

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

      // Load all groups from DB once at the start
      Map<String, Map<String, Object>> dbGroups = loadAllGroupsFromDb();
      Set<String> iasGroupIds = new HashSet<>();
      List<Map<String, Object>> detailedChanges = new ArrayList<>();

      log.info("Loaded {} groups from DB", dbGroups.size());

      fetched = iasGroups.size();

      for (Map<String, Object> iasGroup : iasGroups) {
        try {
          String id = (String) iasGroup.get("id");
          String displayName = (String) iasGroup.get("displayName");

          @SuppressWarnings("unchecked")
          Map<String, Object> ext = (Map<String, Object>) iasGroup.get(CUSTOM_GROUP_EXT);

          String name = ext != null ? (String) ext.get("name") : null;
          String description = ext != null ? (String) ext.get("description") : null;
          Object iasLastModified =
              iasGroup.get("meta") != null
                  ? ((Map<String, Object>) iasGroup.get("meta")).get("lastModified")
                  : null;

          iasGroupIds.add(id);

          // Check if group exists
          if (!dbGroups.containsKey(id)) {
            // NEW group - doesn't exist in DB
            Map<String, Object> group = new HashMap<>();
            group.put("id", id);
            group.put("name", name);
            group.put("displayName", displayName);
            group.put("description", description);
            group.put("iasLastModified", iasLastModified);

            groupRepository.insertGroup(group);

            Map<String, Object> change = new HashMap<>();
            change.put("groupId", id);
            change.put("action", "INSERTED");
            change.put("displayName", displayName);
            change.put("timestamp", System.currentTimeMillis());
            detailedChanges.add(change);

            inserted++;
            log.debug("Inserted new group: {} ({})", id, displayName);

          } else {
            // existing group - check if it actually changed
            Map<String, Object> dbGroup = dbGroups.get(id);
            GroupChangeInfo changeInfo =
                checkGroupChanges(dbGroup, displayName, description);

            if (changeInfo.changedFields.isEmpty()) {
              skipped++;
              log.debug("Group not changed, skipping: {}", id);
              continue;
            }

            // Group changed, update it
            Map<String, Object> updates = new HashMap<>();
            updates.put("displayName", displayName);
            updates.put("description", description);
            updates.put("iasLastModified", iasLastModified);

            groupRepository.updateGroup(id, updates);

            Map<String, Object> change = new HashMap<>();
            change.put("groupId", id);
            change.put("action", "UPDATED");
            change.put("displayName", displayName);
            change.put("changedFields", new ArrayList<>(changeInfo.changedFields));
            change.put("timestamp", System.currentTimeMillis());
            detailedChanges.add(change);

            updated++;
            log.debug("Updated group: {} (changed fields: {})", id, changeInfo.changedFields);
          }

        } catch (Exception e) {
          log.error("Error syncing group from IAS", e);
          errors++;
        }
      }

      // Handle deletions: groups in DB but not in IAS
      for (String dbGroupId : dbGroups.keySet()) {
        if (!iasGroupIds.contains(dbGroupId)) {
          try {
            // Group was deleted from IAS - completely remove from DB
            // First remove all members from this group
            groupMemberRepository.removeAllMembers(dbGroupId);
            // Then delete the group
            groupRepository.deleteGroup(dbGroupId);

            Map<String, Object> change = new HashMap<>();
            change.put("groupId", dbGroupId);
            change.put("action", "DELETED");
            change.put("displayName", dbGroups.get(dbGroupId).get("DISPLAY_NAME"));
            change.put("timestamp", System.currentTimeMillis());
            detailedChanges.add(change);

            deleted++;
            log.debug("Deleted group from DB: {}", dbGroupId);

          } catch (Exception e) {
            log.error("Error deleting group: {}", dbGroupId, e);
            errors++;
          }
        }
      }

      log.info(
          "=== Group sync completed: fetched={}, inserted={}, updated={}, deleted={}, skipped={}, errors={} ===",
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
      result.put("totalGroups", fetched);
      result.put("detailedChanges", detailedChanges);

      // Notify about actual changes
      if ((inserted + updated + deleted) > 0) {
        syncNotificationService.notifyGroupSync(result);
      }

      return result;

    } catch (Exception e) {
      log.error("Failed to sync groups from IAS", e);
      return Map.of("success", false, "error", e.getMessage());
    }
  }

  /** Load all groups from DB */
  private Map<String, Map<String, Object>> loadAllGroupsFromDb() {
    Map<String, Map<String, Object>> groups = new HashMap<>();

    try {
      List<Map<String, Object>> rows = groupRepository.getAllGroups();
      for (Map<String, Object> row : rows) {
        String id = (String) row.get("ID");
        groups.put(id, row);
      }
    } catch (Exception e) {
      log.warn("Failed to load groups from DB: {}", e.getMessage());
      return new HashMap<>();
    }

    return groups;
  }

  /** Check if group changed and return what fields changed */
  private GroupChangeInfo checkGroupChanges(
      Map<String, Object> dbGroup, String newDisplayName, String newDescription) {
    GroupChangeInfo info = new GroupChangeInfo();

    String dbDisplayName = (String) dbGroup.get("DISPLAY_NAME");
    String dbDescription = (String) dbGroup.get("DESCRIPTION");

    if (!nullSafeEquals(newDisplayName, dbDisplayName)) {
      info.changedFields.add("displayName");
    }

    if (!nullSafeEquals(newDescription, dbDescription)) {
      info.changedFields.add("description");
    }

    return info;
  }

  private boolean nullSafeEquals(Object a, Object b) {
    if (a == null && b == null) return true;
    if (a == null || b == null) return false;
    return a.equals(b);
  }

  /** Helper class to track group changes */
  private static class GroupChangeInfo {
    Set<String> changedFields = new HashSet<>();
  }

  /** Create: IAS first, then DB */
  @Transactional
  public Map<String, Object> createGroup(String name, String displayName, String description)
      throws Exception {
    log.info("Creating group: displayName={}", displayName);

    HttpResponse<String> iasResponse = scimClient.createGroup(name, displayName, description);

    if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
      throw new RuntimeException(
          "Failed to create group in IAS: "
              + iasResponse.statusCode()
              + " - "
              + iasResponse.body());
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> iasGroup = objectMapper.readValue(iasResponse.body(), Map.class);

    String id = (String) iasGroup.get("id");

    String iasLastModified = null;
    if (iasGroup.get("meta") != null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> meta = (Map<String, Object>) iasGroup.get("meta");
      iasLastModified = (String) meta.get("lastModified");
    }

    log.info("Group created in IAS with ID: {}", id);

    Map<String, Object> group = new HashMap<>();
    group.put("id", id);
    group.put("name", name);
    group.put("displayName", displayName);
    group.put("description", description);
    group.put("iasLastModified", iasLastModified);

    groupRepository.insertGroup(group);

    log.info("Group saved to DB: ID={}", id);

    return groupRepository.findById(id).orElseThrow();
  }

  /** UPDATE: IAS first, then DB */
  @Transactional
  public Map<String, Object> updateGroup(String id, String newDisplayName, String newDescription)
      throws Exception {
    log.info("Updating group: id={}, displayName={}", id, newDisplayName);

    HttpResponse<String> iasResponse = scimClient.updateGroup(id, newDisplayName, newDescription);

    if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
      throw new RuntimeException(
          "Failed to update group in IAS: "
              + iasResponse.statusCode()
              + " - "
              + iasResponse.body());
    }

    String iasLastModified = null;
    String responseBody = iasResponse.body();

    if (responseBody != null && !responseBody.trim().isEmpty()) {
      @SuppressWarnings("unchecked")
      Map<String, Object> iasGroup = objectMapper.readValue(responseBody, Map.class);

      if (iasGroup.get("meta") != null) {
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) iasGroup.get("meta");
        iasLastModified = (String) meta.get("lastModified");
      }
    }

    log.info("Group updated in IAS: ID={}", id);

    Map<String, Object> updates = new HashMap<>();
    updates.put("displayName", newDisplayName);
    if (newDescription != null) {
      updates.put("description", newDescription);
    }
    if (iasLastModified != null) {
      updates.put("iasLastModified", iasLastModified);
    }

    groupRepository.updateGroup(id, updates);

    log.info("Group updated in DB: ID={}", id);

    return groupRepository.findById(id).orElseThrow();
  }

  /** DELETE: IAS first, then DB */
  @Transactional
  public void deleteGroup(String id) throws Exception {
    log.info("Deleting group: id={}", id);

    HttpResponse<String> iasResponse = scimClient.deleteGroup(id);

    if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
      throw new RuntimeException(
          "Failed to delete group in IAS: "
              + iasResponse.statusCode()
              + " - "
              + iasResponse.body());
    }

    log.info("Group deleted from IAS: ID={}", id);

    groupMemberRepository.removeAllMembers(id);
    groupRepository.deleteGroup(id);

    log.info("Group deleted from DB: ID={}", id);
  }

  /** Add members: IAS first, then DB */
  @Transactional
  public Map<String, Object> addMembers(String groupId, List<String> userIds) throws Exception {
    log.info("Adding members to group: groupId={}, userIds={}", groupId, userIds);

    HttpResponse<String> iasResponse = scimClient.addGroupMembers(groupId, userIds);

    if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
      throw new RuntimeException(
          "Failed to add members in IAS: " + iasResponse.statusCode() + " - " + iasResponse.body());
    }

    log.info("Members added in IAS for group: {}", groupId);

    List<String> added = new ArrayList<>();
    List<String> alreadyMembers = new ArrayList<>();

    for (String userId : userIds) {
      if (groupMemberRepository.isMember(groupId, userId)) {
        alreadyMembers.add(userId);
      } else {
        groupMemberRepository.addMember(groupId, userId);
        added.add(userId);
      }
    }

    log.info("Members added in DB for group: {}", groupId);

    return Map.of(
        "groupId", groupId,
        "added", added,
        "alreadyMembers", alreadyMembers);
  }

  /** REMOVE MEMBER: IAS first, then DB */
  @Transactional
  public void removeMember(String groupId, String userId) throws Exception {
    log.info("Removing member from group: groupId={}, userId={}", groupId, userId);

    HttpResponse<String> iasResponse = scimClient.removeGroupMember(groupId, userId);

    if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
      throw new RuntimeException(
          "Failed to remove member in IAS: "
              + iasResponse.statusCode()
              + " - "
              + iasResponse.body());
    }

    log.info("Member removed from IAS: groupId={}, userId={}", groupId, userId);

    groupMemberRepository.removeMember(groupId, userId);

    log.info("Member removed from DB: groupId={}, userId={}", groupId, userId);
  }

  @Transactional
  public void removeMembers(String groupId, List<String> userIds) throws Exception {
    log.info("Removing members from group: groupId={}, userIds={}", groupId, userIds);

    for (String userId : userIds) {
      HttpResponse<String> iasResponse = scimClient.removeGroupMember(groupId, userId);

      if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
        throw new RuntimeException(
            "Failed to remove member in IAS: userId="
                + userId
                + ", statusCode="
                + iasResponse.statusCode()
                + " - "
                + iasResponse.body());
      }

      log.info("Member removed from IAS: groupId={}, userId={}", groupId, userId);
    }

    groupMemberRepository.removeMembers(groupId, userIds);

    log.info("Members removed from DB: groupId={}, userIds={}", groupId, userIds);
  }

  private String generateNameFromDisplayName(String displayName) {
    return displayName.toLowerCase().replaceAll("[^a-z0-9]+", "-").replaceAll("^-|-$", "");
  }
}