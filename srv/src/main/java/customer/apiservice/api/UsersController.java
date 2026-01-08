package customer.apiservice.api;

import customer.apiservice.ias.ScimClient;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.http.HttpResponse;

@RestController
@RequestMapping("/users")
public class UsersController {

  private final ScimClient scim;

  public UsersController(ScimClient scim) {
    this.scim = scim;
  }

@GetMapping(value = "/{id}", produces = "application/scim+json")
public ResponseEntity<String> getUserById(@PathVariable(name = "id", required = true) String id) {


  HttpResponse<String> resp = scim.getUserById(id);

  return ResponseEntity
      .status(resp.statusCode())
      .header("Content-Type", "application/scim+json")
      .body(resp.body());
}

}
