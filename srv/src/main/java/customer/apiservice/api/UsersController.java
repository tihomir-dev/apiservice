package customer.apiservice.api;

import customer.apiservice.db.UserRepository;
import customer.apiservice.sync.UserSyncService;
import customer.apiservice.db.GroupMemberRepository;
import customer.apiservice.util.FieldValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import customer.apiservice.ias.ScimClient;
import java.net.http.HttpResponse;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;
import org.springframework.http.HttpStatus;


@RestController
@RequestMapping("/users")
public class UsersController {

  
  private static final Logger log = LoggerFactory.getLogger(UsersController.class);
  
  private final UserRepository users;
  private final UserSyncService syncService;
  private final ScimClient scimClient; 
  private final GroupMemberRepository groupMemberRepository;

  public UsersController(UserRepository users, UserSyncService syncService, ScimClient scimClient, GroupMemberRepository groupMemberRepository) {
    this.users = users;
    this.syncService = syncService;
    this.scimClient = scimClient; 
    this.groupMemberRepository = groupMemberRepository;
  }


  @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Map<String, Object>> getUsers(
      @RequestParam(name = "startIndex", required = false, defaultValue = "1") int startIndex,
      @RequestParam(name = "count", required = false, defaultValue = "100") int count,
      @RequestParam(name = "search", required = false) String search,
      @RequestParam(name = "email", required = false) String email,
      @RequestParam(name = "status", required = false) String status,
      @RequestParam(name = "userType", required = false) String userType,
      @RequestParam(name = "country", required = false) String country
  ) {
    // DEBUG: Log all incoming parameters
    log.info("=== CONTROLLER DEBUG ===");
    log.info("startIndex: {}", startIndex);
    log.info("count: {}", count);
    log.info("search: {}", search);
    log.info("email: {}", email);
    log.info("status: {}", status);
    log.info("userType: {}", userType);
     log.info("country: {}", country);
    log.info("========================");

    int total = users.countUsers(search, email, status, userType, country);
    List<Map<String, Object>> rows = users.findUsers(startIndex, count, search, email, status, userType, country);

    Map<String, Object> body = new LinkedHashMap<>();
    body.put("version", "FILTERS_V1");
    body.put("total", total);
    body.put("startIndex", Math.max(1, startIndex));
    body.put("count", Math.min(Math.max(1, count), 200));
    body.put("items", rows);

    // Echo filters back
    Map<String, Object> filters = new LinkedHashMap<>();
    if (search != null) filters.put("search", search);
    if (email != null) filters.put("email", email);
    if (status != null) filters.put("status", status);
    if (userType != null) filters.put("userType", userType);
    if (country != null) filters.put("country", country);
    body.put("filters", filters);

    return ResponseEntity.ok(body);
  }


  /**
   * GET /users/{id}
  
   */
  @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Map<String, Object>> getUserById(@PathVariable("id") String id) {
    return users.findById(id)
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.status(404).body(Map.of("error", "User not found", "id", id)));
  }

  /**
   * POST /users/sync
   
   */
  @PostMapping(value = "/sync", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Map<String, Object>> syncUsers() {
    return ResponseEntity.ok(syncService.syncAllUsers());
  }

  //Create User

@PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, 
               produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Map<String, Object>> createUser(@RequestBody Map<String, Object> userData) {
    log.info("=== CREATE USER REQUEST ===");
    log.info("Request body: {}", userData);
    
    try {
    
      // Validate required fields using FieldValidator
        ResponseEntity<Map<String, Object>> requiredError = FieldValidator.checkRequired(
            userData, 
            "lastName", 
            "email", 
            "userType", 
            "loginName"
        );
        if (requiredError != null) {
            return requiredError;
        }
        
        // Validate email format
        ResponseEntity<Map<String, Object>> emailError = FieldValidator.checkEmail((String) userData.get("email"));
        if (emailError != null) {
            return emailError;
        }
      
      if (!userData.containsKey("status") || userData.get("status") == null || 
          ((String) userData.get("status")).trim().isEmpty()) {
        userData.put("status", "ACTIVE");
      }
      
      log.info("Creating user in IAS...");
      HttpResponse<String> iasResponse = scimClient.createUser(userData);
      
      if (iasResponse.statusCode() != 201) {
        log.error("IAS user creation failed: {} - {}", iasResponse.statusCode(), iasResponse.body());
        return ResponseEntity
            .status(iasResponse.statusCode())
            .body(Map.of(
                "error", "Failed to create user in IAS",
                "statusCode", iasResponse.statusCode(),
                "details", iasResponse.body()
            ));
      }
      
      log.info("User created in IAS successfully, parsing response...");
      ObjectMapper om = new ObjectMapper();
      JsonNode iasUser = om.readTree(iasResponse.body());
      
      //  Extract user data and insert into local DB
      Map<String, Object> userToStore = extractUserDataFromScim(iasUser);
      log.info("Inserting user into local DB with ID: {}", userToStore.get("id"));
      users.insertUser(userToStore);
      
      log.info("User created successfully: {}", userToStore.get("id"));
      return ResponseEntity.status(201).body(userToStore);
      
    } catch (IllegalArgumentException e) {
      // Validation error
      log.error("Validation error: {}", e.getMessage());
      return ResponseEntity
          .status(400)
          .body(Map.of("error", "Validation failed", "message", e.getMessage()));
          
    } catch (Exception e) {
      log.error("Unexpected error creating user", e);
      return ResponseEntity
          .status(500)
          .body(Map.of("error", "Internal server error", "message", e.getMessage()));
    }
  } 

  /**
   * Extracts user data from SCIM response and maps to DB format
   * This mirrors the logic in UserSyncService but for a single user
   */
  private Map<String, Object> extractUserDataFromScim(JsonNode scimUser) {
    Map<String, Object> user = new LinkedHashMap<>();
    
    // Required fields
    user.put("id", scimUser.path("id").asText(null));
    user.put("loginName", scimUser.path("userName").asText(null));
    user.put("email", getFirstEmail(scimUser));
    user.put("lastName", scimUser.path("name").path("familyName").asText(null));
    user.put("userType", scimUser.path("userType").asText("employee"));
    user.put("status", getStatus(scimUser));
    
    // Optional fields
    user.put("firstName", scimUser.path("name").path("givenName").asText(null));
    
    // SAP extension fields
    JsonNode sapExt = scimUser.path("urn:ietf:params:scim:schemas:extension:sap:2.0:User");
    user.put("validFrom", parseDate(sapExt.path("validFrom").asText(null)));
    user.put("validTo", parseDate(sapExt.path("validTo").asText(null)));
    
    // Enterprise extension
    JsonNode entExt = scimUser.path("urn:ietf:params:scim:schemas:extension:enterprise:2.0:User");
    user.put("company", entExt.path("organization").asText(null));
    
    // Address
    JsonNode addresses = scimUser.path("addresses");
    if (addresses.isArray() && addresses.size() > 0) {
      JsonNode address = addresses.get(0);
      user.put("country", address.path("country").asText(null));
      user.put("city", address.path("locality").asText(null));
    } else {
      user.put("country", null);
      user.put("city", null);
    }
    
    // IAS metadata
    user.put("iasLastModified", parseTimestamp(scimUser.path("meta").path("lastModified").asText(null)));
    
    return user;
  }

  /**
   * Get first email from SCIM emails array
   */
  private String getFirstEmail(JsonNode user) {
    JsonNode emails = user.path("emails");
    if (emails.isArray() && emails.size() > 0) {
      return emails.get(0).path("value").asText(null);
    }
    return null;
  }

  /**
   * Get status from SCIM active field or SAP extension
   */
  private String getStatus(JsonNode user) {
    JsonNode activeNode = user.get("active");
    if (activeNode != null && !activeNode.isNull()) {
      return activeNode.asBoolean(true) ? "ACTIVE" : "INACTIVE";
    }
    return "ACTIVE"; // default
  }

  /**
   * Parse ISO date string to LocalDate (for validFrom/validTo)
   */
  private LocalDate parseDate(String isoDate) {
    if (isoDate == null || isoDate.trim().isEmpty()) return null;
    try {
      return OffsetDateTime.parse(isoDate).toLocalDate();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Parse ISO timestamp to SQL Timestamp (for iasLastModified)
   */
  private Timestamp parseTimestamp(String isoInstant) {
    if (isoInstant == null || isoInstant.trim().isEmpty()) return null;
    try {
      return Timestamp.from(Instant.parse(isoInstant));
    } catch (Exception e) {
      return null;
    }
  }

  /**
 * Update a user
 * PATCH /users/{id}
 */
 @PatchMapping(value = "/{id}", 
              consumes = MediaType.APPLICATION_JSON_VALUE,
              produces = MediaType.APPLICATION_JSON_VALUE)
public ResponseEntity<Map<String, Object>> updateUser(
        @PathVariable("id") String id,
        @RequestBody Map<String, Object> updates) {
    
    try {


        // Check if user exists in DB
       Optional<Map<String, Object>> existingUserOpt = users.findById(id);
        if (existingUserOpt.isEmpty()) {  
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of(
                    "error", "User not found",
                    "id", id
                ));
        }
        
        
        
        if (updates.containsKey("id")) {
            return ResponseEntity.badRequest()
                .body(Map.of(
                    "error", "id cannot be modified",
                    "field", "id"
                ));
        }
        ResponseEntity<Map<String, Object>> emailError = FieldValidator.checkEmail((String) updates.get("email"));
        if (emailError != null) {
            return emailError;
        }
        
        // Validate status if provided
        ResponseEntity<Map<String, Object>> statusError = FieldValidator.checkStatus((String) updates.get("status"));
        if (statusError != null) {
            return statusError;
        }
        
      
        // Update user in IAS
        Map<String, Object> iasResponse = scimClient.patchUser(id, updates);
        
        //  Update user in database
        users.updateUser(id, updates);
        
        //  Fetch and return updated user
       Optional<Map<String, Object>> updatedUserOpt = users.findById(id);
       Map<String, Object> updatedUser = updatedUserOpt.orElse(Map.of("id", id));  

       return ResponseEntity.ok(updatedUser); 

        
    } catch (Exception e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(Map.of(
                "error", "Failed to update user",
                "message", e.getMessage(),
                "id", id
            ));
    }
    
} 


    /**
 * Delete a user
 * DELETE /users/{id}
 */
@DeleteMapping("/{id}")
public ResponseEntity<Map<String, Object>> deleteUser(@PathVariable("id") String id) {
    try {
        // Check if user exists in database
        Optional<Map<String, Object>> existingUserOpt = users.findById(id);
        if (existingUserOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of(
                    "error", "User not found",
                    "id", id
                ));
        }

        Map<String, Object> existingUser = existingUserOpt.get();
        String loginName = (String) existingUser.get("LOGIN_NAME");

        //  Delete user from IAS
        HttpResponse<String> iasResponse = scimClient.deleteUser(id);
        
        if (iasResponse.statusCode() >= 300) {
            return ResponseEntity.status(iasResponse.statusCode())
                .body(Map.of(
                    "error", "Failed to delete user from IAS",
                    "statusCode", iasResponse.statusCode(),
                    "message", iasResponse.body()
                ));
        }

        // Delete user from database
        int rowsDeleted = users.deleteUser(id);

        if (rowsDeleted == 0) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of(
                    "error", "User deleted from IAS but failed to delete from database",
                    "id", id
                ));
        }

        // Return success response
        return ResponseEntity.ok(Map.of(
            "message", "User deleted successfully",
            "id", id,
            "loginName", loginName,
            "deletedFromIAS", iasResponse.statusCode() == 204 || iasResponse.statusCode() == 200,
            "deletedFromDatabase", true
        ));

    } catch (Exception e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(Map.of(
                "error", "Failed to delete user",
                "message", e.getMessage(),
                "id", id
            ));
    }
}
@GetMapping("/{id}/groups")
public ResponseEntity<Map<String, Object>> getUserGroups(@PathVariable("id") String userId) {
  log.info("GET /users/{}/groups", userId);
  
  try {
    // Check if user exists
    Optional<Map<String, Object>> user = users.findById(userId);
    if (user.isEmpty()) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body(Map.of("error", "User not found", "userId", userId));
    }
    
    // Get user's groups
    List<Map<String, Object>> groups = groupMemberRepository.findGroupsByUserId(userId);
    
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("userId", userId);
    response.put("totalGroups", groups.size());
    response.put("groups", groups);
    
    return ResponseEntity.ok(response);
    
  } catch (Exception e) {
    log.error("Error getting user groups: " + userId, e);
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
        .body(Map.of("error", "Failed to get user groups", "message", e.getMessage()));
  }
}

@PostMapping("/{id}/groups")
public ResponseEntity<Map<String, Object>> addUserToGroups(
    @PathVariable("id") String id,
    @RequestBody Map<String, Object> request
) {
    log.info("POST /users/{}/groups - {}", id, request);
    
    try {
        Optional<Map<String, Object>> user = users.findById(id);
        if (user.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "User not found", "id", id));
        }
        ResponseEntity<Map<String, Object>> groupIdsError = FieldValidator.checkRequired(request, "groupIds");
        if (groupIdsError != null) {
            return groupIdsError;
        }
        
        List<String> groupIds = (List<String>) request.get("groupIds");
                
        // Add user to each group in IAS
        List<String> added = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        
        for (String groupId : groupIds) {
            log.info("Adding user {} to group {}", id, groupId);
            
            // addGroupMembers(groupId, List<userIds>)
            // We're adding ONE user to ONE group
            HttpResponse<String> iasResponse = scimClient.addGroupMembers(groupId, List.of(id));
            
            log.info("IAS Response status: {}", iasResponse.statusCode());
            log.info("IAS Response body: {}", iasResponse.body());
            
            if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
                log.error("Failed to add user to group in IAS: {} - {}", iasResponse.statusCode(), iasResponse.body());
                failed.add(groupId);
            } else {
                log.info("User {} successfully added to group {}", id, groupId);
                added.add(groupId);
                
                // 2. Add to DB
                if (!users.isUserInGroup(id, groupId)) {
                    users.addUserToGroup(id, groupId);
                }
            }
        }
        
        return ResponseEntity.ok(Map.of(
            "userId", id,
            "added", added,
            "failed", failed
        ));
        
    } catch (Exception e) {
        log.error("Error adding user to groups: " + id, e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(Map.of("error", "Failed to add user to groups", "message", e.getMessage()));
    }
}

@DeleteMapping("/{id}/groups")
public ResponseEntity<Map<String, Object>> removeUserFromGroups(
    @PathVariable("id") String id,
    @RequestBody Map<String, Object> request
) {
    log.info("DELETE /users/{}/groups - {}", id, request);
    
    try {
        Optional<Map<String, Object>> user = users.findById(id);
        if (user.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "User not found", "id", id));
        }
        
        List<String> groupIds = (List<String>) request.get("groupIds");
        
        if (groupIds == null || groupIds.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", "groupIds array is required and must not be empty"));
        }
        
        // 1. Remove user from each group in IAS
        List<String> removed = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        
        for (String groupId : groupIds) {
            log.info("Removing user {} from group {}", id, groupId);
            
            // removeGroupMember(groupId, userId)
            HttpResponse<String> iasResponse = scimClient.removeGroupMember(groupId, id);
            
            log.info("IAS Response status: {}", iasResponse.statusCode());
            log.info("IAS Response body: {}", iasResponse.body());
            
            if (iasResponse.statusCode() < 200 || iasResponse.statusCode() >= 300) {
                log.error("Failed to remove user from group in IAS: {} - {}", iasResponse.statusCode(), iasResponse.body());
                failed.add(groupId);
            } else {
                log.info("User {} successfully removed from group {}", id, groupId);
                removed.add(groupId);
                
                // 2. Remove from DB
                users.removeUserFromGroup(id, groupId);
            }
        }
        
        return ResponseEntity.ok(Map.of(
            "userId", id,
            "removed", removed,
            "failed", failed
        ));
        
    } catch (Exception e) {
        log.error("Error removing user from groups: " + id, e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(Map.of("error", "Failed to remove user from groups", "message", e.getMessage()));
    }
}



}