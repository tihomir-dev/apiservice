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

  /** Sync all groups from IAS to DB tracks changes (compares with DB) */
  @Transactional
  public Map<String, Object> syncGroupsFromIas() {
    log.info("=== Starting group sync from IAS ===");

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

      // OPTIMIZATION: Load all groups from DB once at the start
      Map<String, Map<String, Object>> dbGroups = loadAllGroupsFromDb();
      log.info("Loaded {} groups from DB", dbGroups.size());

      int totalFetched = iasGroups.size();
      int actualChanges = 0;
      int skipped = 0;
      int errors = 0;

      for (Map<String, Object> iasGroup : iasGroups) {
        try {
          String id = (String) iasGroup.get("id");
          String displayName = (String) iasGroup.get("displayName");

          Map<String, Object> ext = (Map<String, Object>) iasGroup.get(CUSTOM_GROUP_EXT);

          String name = ext != null ? (String) ext.get("name") : null;
          String description = ext != null ? (String) ext.get("description") : null;

          // Check if group exists
          if (!dbGroups.containsKey(id)) {
            // NEW group - doesn't exist in DB
            Map<String, Object> group = new HashMap<>();
            group.put("id", id);
            group.put("name", name);
            group.put("displayName", displayName);
            group.put("description", description);
            group.put(
                "iasLastModified",
                iasGroup.get("meta") != null
                    ? ((Map<String, Object>) iasGroup.get("meta")).get("lastModified")
                    : null);

            groupRepository.insertGroup(group);
            actualChanges++;
            log.debug("Inserted new group: {}", id);

          } else {
            // existing group - check if it actually changed
            Map<String, Object> dbGroup = dbGroups.get(id);

            boolean hasChanged = groupHasChanged(dbGroup, displayName, description);

            if (!hasChanged) {
              skipped++;
              log.debug("Group not changed, skipping: {}", id);
              continue;
            }

            // Group changed, update it
            Map<String, Object> updates = new HashMap<>();
            updates.put("displayName", displayName);
            updates.put("description", description);
            updates.put(
                "iasLastModified",
                iasGroup.get("meta") != null
                    ? ((Map<String, Object>) iasGroup.get("meta")).get("lastModified")
                    : null);

            groupRepository.updateGroup(id, updates);
            actualChanges++;
            log.debug("Updated group: {}", id);
          }

        } catch (Exception e) {
          log.error("Error syncing group", e);
          errors++;
        }
      }

      log.info(
          "=== Group sync completed: fetched={}, changes={}, skipped={}, errors={} ===",
          totalFetched,
          actualChanges,
          skipped,
          errors);

      Map<String, Object> result =
          Map.of(
              "success", true,
              "totalGroups", totalFetched,
              "inserted", actualChanges,
              "updated", 0,
              "failed", errors);

      // Notify about actual changes
      if (actualChanges > 0) {
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
    String query =
        """
        SELECT "ID", "DISPLAY_NAME", "DESCRIPTION"
        FROM "GROUPS"
        """;

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

  /** Check if group changed */
  private boolean groupHasChanged(
      Map<String, Object> dbGroup, String newDisplayName, String newDescription) {
    String dbDisplayName = (String) dbGroup.get("DISPLAY_NAME");
    String dbDescription = (String) dbGroup.get("DESCRIPTION");

    if (!nullSafeEquals(newDisplayName, dbDisplayName)) {
      return true;
    }

    if (!nullSafeEquals(newDescription, dbDescription)) {
      return true;
    }

    return false; // No changes
  }

  private boolean nullSafeEquals(Object a, Object b) {
    if (a == null && b == null) return true;
    if (a == null || b == null) return false;
    return a.equals(b);
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

  /** add members: IAS first, then DB */
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
