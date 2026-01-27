package customer.apiservice.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import customer.apiservice.db.GroupMemberRepository;
import customer.apiservice.db.GroupRepository;
import customer.apiservice.ias.ScimClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.*;

@Service
public class GroupService {

  private static final String CUSTOM_GROUP_EXT = "urn:sap:cloud:scim:schemas:extension:custom:2.0:Group";

  private static final Logger log = LoggerFactory.getLogger(GroupService.class);

  private final ScimClient scimClient;
  private final GroupRepository groupRepository;
  private final GroupMemberRepository groupMemberRepository;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public GroupService(
      ScimClient scimClient,
      GroupRepository groupRepository,
      GroupMemberRepository groupMemberRepository
  ) {
    this.scimClient = scimClient;
    this.groupRepository = groupRepository;
    this.groupMemberRepository = groupMemberRepository;
  }

  /**
   *  Sync all groups from IAS to DB
   */
  @Transactional
  public Map<String, Object> syncGroupsFromIas() {
    log.info("=== Starting initial group sync from IAS ===");

    try {
      // 1. Get all groups from IAS
      HttpResponse<String> response = scimClient.getGroups();

      if (response.statusCode() != 200) {
        throw new RuntimeException("Failed to get groups from IAS: " + response.statusCode() + " - " + response.body());
      }

      // 2. Parse IAS response
      @SuppressWarnings("unchecked")
      Map<String, Object> iasData = objectMapper.readValue(response.body(), Map.class);
      
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> iasGroups = (List<Map<String, Object>>) iasData.get("Resources");

      if (iasGroups == null) {
        iasGroups = new ArrayList<>();
      }

      //log.info("Found {} groups in IAS", iasGroups.size());

      int inserted = 0;
      int updated = 0;
      int errors = 0;

      // 3. Sync each group to DB
      for (Map<String, Object> iasGroup : iasGroups) {
        try {
          String id = (String) iasGroup.get("id");
          String displayName = (String) iasGroup.get("displayName");
          Map<String, Object> ext = (Map<String, Object>) iasGroup.get("urn:sap:cloud:scim:schemas:extension:custom:2.0:Group");
          
          String name = ext != null ? (String) ext.get("name") : null;
          String description = ext != null ? (String) ext.get("description") : null;


          
          // Extract members
          //@SuppressWarnings("unchecked")
          List<Map<String, Object>> iasMembers = (List<Map<String, Object>>) iasGroup.get("members");

          // Check if group already exists in DB
          Optional<Map<String, Object>> existing = groupRepository.findById(id);

          if (existing.isEmpty()) {
            // Insert new group
            Map<String, Object> group = new HashMap<>();
            group.put("id", id);
            group.put("name", name);
            group.put("displayName", displayName);
            group.put("description", description);
            group.put("iasLastModified", iasGroup.get("meta") != null ? 
                ((Map<String, Object>) iasGroup.get("meta")).get("lastModified") : null);

            groupRepository.insertGroup(group);
            inserted++;

            // Sync members
            if (iasMembers != null) {
              syncGroupMembers(id, iasMembers);
            }

          } else {
            // Update existing group
            Map<String, Object> updates = new HashMap<>();
            updates.put("displayName", displayName);
            updates.put("description", description);
            updates.put("iasLastModified", iasGroup.get("meta") != null ? 
                ((Map<String, Object>) iasGroup.get("meta")).get("lastModified") : null);

            groupRepository.updateGroup(id, updates);
            updated++;

            // Sync members
            if (iasMembers != null) {
              // Remove all existing members and re-add
              groupMemberRepository.removeAllMembers(id);
              syncGroupMembers(id, iasMembers);
            }
          }

        } catch (Exception e) {
          log.error("Error syncing group", e);
          errors++;
        }
      }

      log.info("=== Group sync completed: inserted={}, updated={}, errors={} ===", inserted, updated, errors);

      return Map.of(
          "success", true,
          "totalGroups", iasGroups.size(),
          "inserted", inserted,
          "updated", updated,
          "errors", errors
      );

    } catch (Exception e) {
      log.error("Failed to sync groups from IAS", e);
      return Map.of(
          "success", false,
          "error", e.getMessage()
      );
    }
  }

  /**
   * CREATE: IAS first, then DB
   */
  @Transactional
  public Map<String, Object> createGroup(String name, String displayName, String description) throws Exception {
    log.info("Creating group: displayName={}", displayName);

    // 1. Create in IAS first
    HttpResponse<String> iasResponse = scimClient.createGroup(name, displayName, description);

    if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
      throw new RuntimeException("Failed to create group in IAS: " + iasResponse.statusCode() + " - " + iasResponse.body());
    }

    // Parse IAS response to get the generated ID
   // @SuppressWarnings("unchecked")
    Map<String, Object> iasGroup = objectMapper.readValue(iasResponse.body(), Map.class);

    String id = (String) iasGroup.get("id");

    String iasLastModified = null;
    
    if (iasGroup.get("meta") != null) {
      Map<String, Object> meta = (Map<String, Object>) iasGroup.get("meta");
      iasLastModified = (String) meta.get("lastModified");
    }

    log.info("Group created in IAS with ID: {}", id);

    // 3. Save to DB with IAS-generated ID
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

  /**
   * UPDATE: IAS first, then DB
   */
  @Transactional
  public Map<String, Object> updateGroup(String id, String newDisplayName, String newDescription) throws Exception {
    log.info("Updating group: id={}, displayName={}", id, newDisplayName);

    // Update in IAS first
    
    HttpResponse<String> iasResponse = scimClient.updateGroup(id, newDisplayName, newDescription);

    if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
      throw new RuntimeException("Failed to update group in IAS: " + iasResponse.statusCode() + " - " + iasResponse.body());
    }

    // Parse IAS response
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

    // 3. Update in DB
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

  /**
   * DELETE: IAS first, then DB
   */
  @Transactional
  public void deleteGroup(String id) throws Exception {
    log.info("Deleting group: id={}", id);

    // 1. Delete from IAS first
    HttpResponse<String> iasResponse = scimClient.deleteGroup(id);

    if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
      throw new RuntimeException("Failed to delete group in IAS: " + iasResponse.statusCode() + " - " + iasResponse.body());
    }

    log.info("Group deleted from IAS: ID={}", id);

    // 2. Delete from DB
    groupMemberRepository.removeAllMembers(id);
    groupRepository.deleteGroup(id);

    log.info("Group deleted from DB: ID={}", id);
  }

  /**
   * ADD MEMBERS: IAS first, then DB
   */
  @Transactional
  public Map<String, Object> addMembers(String groupId, List<String> userIds) throws Exception {
    log.info("Adding members to group: groupId={}, userIds={}", groupId, userIds);

    // 1. Add to IAS first
    HttpResponse<String> iasResponse = scimClient.addGroupMembers(groupId, userIds);

    if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
      throw new RuntimeException("Failed to add members in IAS: " + iasResponse.statusCode() + " - " + iasResponse.body());
    }

    log.info("Members added in IAS for group: {}", groupId);

    // 2. Add to DB
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
        "alreadyMembers", alreadyMembers
    );
  }

  /**
   * REMOVE MEMBER: IAS first, then DB
   */
  @Transactional
  public void removeMember(String groupId, String userId) throws Exception {
    log.info("Removing member from group: groupId={}, userId={}", groupId, userId);

    // 1. Remove from IAS first
    HttpResponse<String> iasResponse = scimClient.removeGroupMember(groupId, userId);

    if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
      throw new RuntimeException("Failed to remove member in IAS: " + iasResponse.statusCode() + " - " + iasResponse.body());
    }

    log.info("Member removed from IAS: groupId={}, userId={}", groupId, userId);

    // 2. Remove from DB
    groupMemberRepository.removeMember(groupId, userId);

    log.info("Member removed from DB: groupId={}, userId={}", groupId, userId);
  }

  // HELPER METHODS

  /**
   * Sync members from IAS to DB
   */
  private void syncGroupMembers(String groupId, List<Map<String, Object>> iasMembers) {
    for (Map<String, Object> member : iasMembers) {
      try {
        String userId = (String) member.get("value");
        if (!groupMemberRepository.isMember(groupId, userId)) {
          groupMemberRepository.addMember(groupId, userId);
        }
      } catch (Exception e) {
        log.warn("Failed to sync member for group {}: {}", groupId, e.getMessage());
      }
    }
  }

  /**
   * Generate a URL-friendly name from displayName
   */
  private String generateNameFromDisplayName(String displayName) {
    // Convert "My Group" to "my-group"
    return displayName.toLowerCase()
        .replaceAll("[^a-z0-9]+", "-")
        .replaceAll("^-|-$", "");
  }
}