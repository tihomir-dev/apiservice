package customer.apiservice.api;

import customer.apiservice.db.UserRepository;
import customer.apiservice.sync.UserSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/users")
public class UsersController {

  // ADD THIS LINE - Logger declaration
  private static final Logger log = LoggerFactory.getLogger(UsersController.class);
  
  private final UserRepository users;
  private final UserSyncService syncService;

  public UsersController(UserRepository users, UserSyncService syncService) {
    this.users = users;
    this.syncService = syncService;
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
}