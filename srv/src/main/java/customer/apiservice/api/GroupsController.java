package customer.apiservice.api;

import customer.apiservice.db.GroupRepository;
import customer.apiservice.db.GroupMemberRepository;
import customer.apiservice.sync.GroupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController 
@RequestMapping("/groups")
public class GroupsController {

  private static final Logger log = LoggerFactory.getLogger(GroupsController.class);
  
  private final GroupRepository groupRepository;
  private final GroupMemberRepository groupMemberRepository;
  private final GroupService groupService;

  public GroupsController(
      GroupRepository groupRepository,
      GroupMemberRepository groupMemberRepository,
      GroupService groupService
  ) {
    this.groupRepository = groupRepository;
    this.groupMemberRepository = groupMemberRepository;
    this.groupService = groupService;
  }

  /**
   * POST /groups/sync - Initial load from IAS to DB
   */
  @PostMapping("/sync")
  public ResponseEntity<Map<String, Object>> syncFromIas() {
    log.info("POST /groups/sync - Starting initial sync from IAS");
    
    try {
      Map<String, Object> result = groupService.syncGroupsFromIas();
      return ResponseEntity.ok(result);
      
    } catch (Exception e) {
      log.error("Error syncing groups from IAS", e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("error", "Failed to sync groups", "message", e.getMessage()));
    }
  }

  /**
   * GET /groups - List all groups (from DB)
   */
  @GetMapping
  public ResponseEntity<Map<String, Object>> listGroups(
      @RequestParam(name = "startIndex", required = false, defaultValue = "1") int startIndex,
      @RequestParam(name = "count", defaultValue = "100") int count,
      @RequestParam(name = "search", required = false) String search
  ) {
    log.info("GET /groups - startIndex={}, count={}, search={}", startIndex, count, search);
    
    try {
      List<Map<String, Object>> groups = groupRepository.findGroups(startIndex, count, search);
      int totalResults = groupRepository.countGroups(search);
      
      Map<String, Object> response = new HashMap<>();
      response.put("totalResults", totalResults);
      response.put("startIndex", startIndex);
      response.put("itemsPerPage", groups.size());
      response.put("Resources", groups);
      
      return ResponseEntity.ok(response);
      
    } catch (Exception e) {
      log.error("Error listing groups", e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("error", "Failed to list groups", "message", e.getMessage()));
    }
  }

  /**
   * GET /groups/{id} - Get single group (from DB)
   */
  @GetMapping("/{id}")
  public ResponseEntity<Map<String, Object>> getGroup(@PathVariable("id") String id) {
    log.info("GET /groups/{}", id);
    
    try {
      Optional<Map<String, Object>> group = groupRepository.findById(id);
      
      if (group.isEmpty()) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(Map.of("error", "Group not found", "id", id));
      }
      
      return ResponseEntity.ok(group.get());
      
    } catch (Exception e) {
      log.error("Error getting group: " + id, e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("error", "Failed to get group", "message", e.getMessage()));
    }
  }

  /**
   * POST /groups - Create new group (IAS first, then DB)
   */
  @PostMapping
  public ResponseEntity<Map<String, Object>> createGroup(@RequestBody Map<String, Object> request) {
    log.info("POST /groups - {}", request);
    
    try {
      String name = (String) request.get("name");
      String displayName = (String) request.get("displayName");
      String description = (String) request.get("description");
      
      if (displayName == null || displayName.trim().isEmpty()) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
            .body(Map.of("error", "displayName is required"));
      }

      if(name == null || name.trim().isEmpty()){
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(Map.of("error", "name is required"));
      }

      Optional<Map<String, Object>> existing = groupRepository.findByName(name.trim());
      if (existing.isPresent()) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .body(Map.of("error", "Group with this name already exists", "name", name.trim()));
      }
      
      // Create in IAS first, then save to DB
      Map<String, Object> group = groupService.createGroup(name.trim(), displayName.trim(), description);
      
      return ResponseEntity.status(HttpStatus.CREATED).body(group);
      
    } catch (Exception e) {
      log.error("Error creating group", e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("error", "Failed to create group", "message", e.getMessage()));
    }
  }

  /**
   * PUT /groups/{id} - Update group (IAS first, then DB)
   */
  @PutMapping("/{id}")
  public ResponseEntity<Map<String, Object>> updateGroup(
      @PathVariable("id") String id,
      @RequestBody Map<String, Object> updates
  ) {
    log.info("PUT /groups/{} - {}", id, updates);
    
    try {
      // Check if group exists
      Optional<Map<String, Object>> existing = groupRepository.findById(id);
      if (existing.isEmpty()) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(Map.of("error", "Group not found", "id", id));
      }
      
      String newDisplayName = (String) updates.get("displayName");
      String newDescription = (String) updates.get("description");
      
      if (newDisplayName == null || newDisplayName.trim().isEmpty()) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
            .body(Map.of("error", "displayName is required"));
      }
      
      // Update in IAS first, then DB
      Map<String, Object> group = groupService.updateGroup(id, newDisplayName, newDescription);
      
      return ResponseEntity.ok(group);
      
    } catch (Exception e) {
      log.error("Error updating group: " + id, e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("error", "Failed to update group", "message", e.getMessage()));
    }
  }

  /**
   * DELETE /groups/{id} - Delete group (IAS first, then DB)
   */
  @DeleteMapping("/{id}")
  public ResponseEntity<Map<String, Object>> deleteGroup(@PathVariable("id") String id) {
    log.info("DELETE /groups/{}", id);
    
    try {
      // Check if group exists
      Optional<Map<String, Object>> existing = groupRepository.findById(id);
      if (existing.isEmpty()) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(Map.of("error", "Group not found", "id", id));
      }
      
      // Delete from IAS first, then DB
      groupService.deleteGroup(id);
      
      return ResponseEntity.ok(Map.of("message", "Group deleted successfully", "id", id));
      
    } catch (Exception e) {
      log.error("Error deleting group: " + id, e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("error", "Failed to delete group", "message", e.getMessage()));
    }
  }

  /**
   * GET /groups/{id}/members - List all members (from DB)
   */
  @GetMapping("/{id}/members")
  public ResponseEntity<Map<String, Object>> getGroupMembers(@PathVariable("id") String id) {
   
    
    try {
      Optional<Map<String, Object>> group = groupRepository.findById(id);
      if (group.isEmpty()) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(Map.of("error", "Group not found", "id", id));
      }
      
      List<Map<String, Object>> members = groupMemberRepository.findMembersByGroupId(id);
      
      Map<String, Object> response = new HashMap<>();
      response.put("groupId", id);
      response.put("groupName", group.get().get("NAME"));
      response.put("totalMembers", members.size());
      response.put("members", members);
      
      return ResponseEntity.ok(response);
      
    } catch (Exception e) {
      log.error("Error getting group members: " + id, e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("error", "Failed to get group members", "message", e.getMessage()));
    }
  }

  /**
   * POST /groups/{id}/members - Add members (IAS first, then DB)
   */
  @PostMapping("/{id}/members")
  public ResponseEntity<Map<String, Object>> addGroupMembers(
      @PathVariable("id") String id,
      @RequestBody Map<String, Object> request
  ) {
    log.info("POST /groups/{}/members - {}", id, request);
    
    try {
      Optional<Map<String, Object>> group = groupRepository.findById(id);
      if (group.isEmpty()) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(Map.of("error", "Group not found", "id", id));
      }
      
    
      List<String> userIds = (List<String>) request.get("userIds");
      
      if (userIds == null || userIds.isEmpty()) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
            .body(Map.of("error", "userIds array is required and must not be empty"));
      }
      
      // Add to IAS first, then DB
      Map<String, Object> result = groupService.addMembers(id, userIds);
      
      return ResponseEntity.ok(result);
      
    } catch (Exception e) {
      log.error("Error adding group members: " + id, e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("error", "Failed to add group members", "message", e.getMessage()));
    }
  }

  /**
   * DELETE /groups/{id}/members/{userId} - Remove member (IAS first, then DB)
   */
  @DeleteMapping("/{id}/members/{userId}")
  public ResponseEntity<Map<String, Object>> removeGroupMember(
      @PathVariable("id") String id,
      @PathVariable("userId") String userId
  ) {
    log.info("DELETE /groups/{}/members/{}", id, userId);
    
    try {
      Optional<Map<String, Object>> group = groupRepository.findById(id);
      if (group.isEmpty()) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(Map.of("error", "Group not found", "id", id));
      }
      
      // Check if user is a member
      if (!groupMemberRepository.isMember(id, userId)) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(Map.of("error", "User is not a member of this group", "groupId", id, "userId", userId));
      }
      
      // Remove from IAS first, then DB
      groupService.removeMember(id, userId);
      
      return ResponseEntity.ok(Map.of(
          "message", "Member removed successfully",
          "groupId", id,
          "userId", userId
      ));
      
    } catch (Exception e) {
      log.error("Error removing group member: group={}, user={}", id, userId, e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Map.of("error", "Failed to remove group member", "message", e.getMessage()));
    }
  }
}