package customer.apiservice.api;
import customer.apiservice.sync.UserSyncService;
import customer.apiservice.ias.ScimClient;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

import java.net.http.HttpResponse;

@RestController
@RequestMapping("/users")
public class UsersController {

  private final ScimClient scim;
  private final UserSyncService syncService;


  public UsersController(ScimClient scim, UserSyncService syncService) {
    this.scim = scim;
    this.syncService = syncService;
  }

 
@GetMapping(value = "/{id}", produces = "application/scim+json")
public ResponseEntity<String> getUserById(@PathVariable(name = "id", required = true) String id) {


  HttpResponse<String> resp = scim.getUserById(id);

  return ResponseEntity
      .status(resp.statusCode())
      .header("Content-Type", "application/scim+json")
      .body(resp.body());
}


@GetMapping(produces = "application/scim+json")
public ResponseEntity<String> getUsers() {

  HttpResponse<String> resp = scim.getUsers();

  return ResponseEntity
      .status(resp.statusCode())
      .header("Content-Type", "application/scim+json")
      .body(resp.body());
}


@PostMapping(value = "/sync", produces = "application/json")
public ResponseEntity<Map<String, Object>> syncUsers() {
  return ResponseEntity.ok(syncService.syncAllUsers());
}


}
