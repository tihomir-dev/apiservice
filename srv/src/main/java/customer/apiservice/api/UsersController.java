package customer.apiservice.api;

import customer.apiservice.db.UserRepository;
import customer.apiservice.sync.UserSyncService;
import customer.apiservice.db.GroupMemberRepository;
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
      @RequestParam(name = "userType", required = false) String userType
  ) {
    // DEBUG: Log all incoming parameters
    log.info("=== CONTROLLER DEBUG ===");
    log.info("startIndex: {}", startIndex);
    log.info("count: {}", count);
    log.info("search: {}", search);
    log.info("email: {}", email);
    log.info("status: {}", status);
    log.info("userType: {}", userType);
    log.info("========================");

    int total = users.countUsers(search, email, status, userType);
    List<Map<String, Object>> rows = users.findUsers(startIndex, count, search, email, status, userType);

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
    
      validateRequiredFieldsForCreate(userData);
      
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
      
      // 6. Extract user data and insert into local DB
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

    private void validateRequiredFieldsForCreate(Map<String, Object> userData) {
    List<String> missingFields = new ArrayList<>();
    
    if (!hasValue(userData, "lastName")) {
      missingFields.add("lastName");
    }
    if (!hasValue(userData, "email")) {
      missingFields.add("email");
    }
    if (!hasValue(userData, "userType")) {
      missingFields.add("userType");
    }
    if (!hasValue(userData, "loginName")) {
      missingFields.add("loginName");
    }
    
    if (!missingFields.isEmpty()) {
      throw new IllegalArgumentException("Missing required fields: " + String.join(", ", missingFields));
    }
  }

  /**
   * Helper to check if a field has a non-empty value
   */
  private boolean hasValue(Map<String, Object> map, String key) {
    Object value = map.get(key);
    if (value == null) return false;
    if (value instanceof String) {
      return !((String) value).trim().isEmpty();
    }
    return true;
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


        // Validation: Check if user exists in DB
       Optional<Map<String, Object>> existingUserOpt = users.findById(id);
        if (existingUserOpt.isEmpty()) {  // ← Use .isEmpty() instead of == null
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
        
        // Validation: Email format if provided
        if (updates.containsKey("email")) {
            String email = (String) updates.get("email");
            if (email != null && !email.matches("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$")) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "error", "Invalid email format",
                        "field", "email",
                        "value", email
                    ));
            }
        }
        
        // Validation: Status values
        if (updates.containsKey("status")) {
            String status = (String) updates.get("status");
            if (status != null && 
                !status.equalsIgnoreCase("ACTIVE") && 
                !status.equalsIgnoreCase("INACTIVE")) {
                return ResponseEntity.badRequest()
                    .body(Map.of(
                        "error", "Status must be ACTIVE or INACTIVE",
                        "field", "status",
                        "value", status
                    ));
            }
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
}