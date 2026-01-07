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

  @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getUserById(@PathVariable("id") String id) {

    HttpResponse<String> resp = scim.getUserById(id);

    int status = resp.statusCode();
    String body = resp.body() == null ? "" : resp.body();

    if (body.isBlank() && status / 100 != 2) {
      body = "{\"error\":\"IAS SCIM call failed\",\"status\":" + status + "}";
    }

    return ResponseEntity.status(status)
        .contentType(MediaType.APPLICATION_JSON)
        .body(body);
  }
}
