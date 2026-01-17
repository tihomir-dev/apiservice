package customer.apiservice.ias;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.ArrayList;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class ScimClient {

  private final HttpClient http = HttpClient.newBuilder()
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build();

  private final IasTokenService tokenService;
  private final ObjectMapper objectMapper = new ObjectMapper();  


  @Value("${IAS_SCIM_BASE_URL}")
  private String scimBaseUrl;

  public ScimClient(IasTokenService tokenService) {
    this.tokenService = tokenService;
  }

  public HttpResponse<String> getUserById(String id) {
    try {
      String token = tokenService.getAccessToken();

      String base = scimBaseUrl.endsWith("/")
          ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
          : scimBaseUrl;

      URI uri = URI.create(base + "/Users/" + id);

      HttpRequest req = HttpRequest.newBuilder(uri)
          .header("Accept", "application/scim+json, application/json")
          .header("Authorization", "Bearer " + token)
          .GET()
          .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM call failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}"
      );
    }
  }

  public HttpResponse<String> getUsers(String query) {
  try {
    String token = tokenService.getAccessToken();

    String base = scimBaseUrl.endsWith("/")
        ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
        : scimBaseUrl;

    String q = (query == null || query.isBlank()) ? "" : ("?" + query);
    URI uri = URI.create(base + "/Users" + q);

    HttpRequest req = HttpRequest.newBuilder(uri)
        .header("Accept", "application/scim+json, application/json")
        .header("Authorization", "Bearer " + token)
        .GET()
        .build();

    return http.send(req, HttpResponse.BodyHandlers.ofString());

  } catch (Exception e) {
    return new SimpleStringResponse(
        500,
        "{\"error\":\"SCIM call failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}"
    );
  }
}


  public HttpResponse<String> getUsers() {
  try {
    String token = tokenService.getAccessToken();

    String base = scimBaseUrl.endsWith("/")
        ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
        : scimBaseUrl;

    URI uri = URI.create(base + "/Users");   // <-- no "/{id}"

    HttpRequest req = HttpRequest.newBuilder(uri)
        .header("Accept", "application/scim+json, application/json")
        .header("Authorization", "Bearer " + token)
        .GET()
        .build();

    return http.send(req, HttpResponse.BodyHandlers.ofString());

  } catch (Exception e) {
    return new SimpleStringResponse(
        500,
        "{\"error\":\"SCIM call failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}"
    );
  }
}

  


  private static String escapeJson(String s) {
    if (s == null) return "";
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static final class SimpleStringResponse implements HttpResponse<String> {
    private final int status;
    private final String body;

    SimpleStringResponse(int status, String body) {
      this.status = status;
      this.body = body == null ? "" : body;
    }

    @Override public int statusCode() { return status; }
    @Override public String body() { return body; }

    @Override public HttpRequest request() { return null; }
    @Override public java.util.Optional<HttpResponse<String>> previousResponse() { return java.util.Optional.empty(); }
    @Override public java.net.http.HttpHeaders headers() { return java.net.http.HttpHeaders.of(java.util.Map.of(), (a,b) -> true); }
    @Override public URI uri() { return null; }
    @Override public HttpClient.Version version() { return HttpClient.Version.HTTP_1_1; }
    @Override public java.util.Optional<javax.net.ssl.SSLSession> sslSession() { return java.util.Optional.empty(); }
  }


  /**
 * Creates a new user in IAS via SCIM API
 * @param userData Map containing user fields from the UI
 * @return HttpResponse with the created user (including IAS-generated ID)
 */
public HttpResponse<String> createUser(Map<String, Object> userData) {
    try {
        String token = tokenService.getAccessToken();

        String base = scimBaseUrl.endsWith("/")
            ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
            : scimBaseUrl;

        URI uri = URI.create(base + "/Users");

        // Build SCIM JSON payload
        String scimJson = buildScimUserJson(userData);

        HttpRequest req = HttpRequest.newBuilder(uri)
            .header("Accept", "application/scim+json, application/json")
            .header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/scim+json")
            .POST(HttpRequest.BodyPublishers.ofString(scimJson))
            .build();

        return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
        return new SimpleStringResponse(
            500,
            "{\"error\":\"SCIM create user failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}"
        );
    }
}

/**
 * Builds SCIM-compliant JSON for user creation
 */
private String buildScimUserJson(Map<String, Object> userData) {
    StringBuilder json = new StringBuilder();
    json.append("{");
    
    // Required: schemas
    json.append("\"schemas\":[\"urn:ietf:params:scim:schemas:core:2.0:User\"");
    
   
    boolean hasSapFields = userData.containsKey("validFrom") || userData.containsKey("validTo");
    if (hasSapFields) {
        json.append(",\"urn:ietf:params:scim:schemas:extension:sap:2.0:User\"");
    }
    
  
    boolean hasEnterpriseFields = userData.containsKey("company");
    if (hasEnterpriseFields) {
        json.append(",\"urn:ietf:params:scim:schemas:extension:enterprise:2.0:User\"");
    }
    
    json.append("],");

    // Required: userName (loginName)
    String loginName = (String) userData.get("loginName");
    json.append("\"userName\":").append(quote(loginName)).append(",");

    // Required: name
    String lastName = (String) userData.get("lastName");
    String firstName = (String) userData.get("firstName");
    json.append("\"name\":{");
    json.append("\"familyName\":").append(quote(lastName));
    if (firstName != null && !firstName.trim().isEmpty()) {
        json.append(",\"givenName\":").append(quote(firstName));
    }
    json.append("},");

    // Required: emails
    String email = (String) userData.get("email");
    json.append("\"emails\":[{\"value\":").append(quote(email)).append(",\"primary\":true}],");

    // Required: userType
    String userType = (String) userData.get("userType");
    json.append("\"userType\":").append(quote(userType)).append(",");

    // Status (active true/false based on ACTIVE/INACTIVE)
    String status = (String) userData.get("status");
    boolean active = !"INACTIVE".equalsIgnoreCase(status);
    json.append("\"active\":").append(active);


    // Optional: addresses (city, country) - WORK ADDRESS
    String city = (String) userData.get("city");
    String country = (String) userData.get("country");
    if ((city != null && !city.trim().isEmpty()) || (country != null && !country.trim().isEmpty())) {
        json.append(",\"addresses\":[{");
        json.append("\"type\":\"work\",\"primary\":false");  // ← ADD TYPE!
    if (city != null && !city.trim().isEmpty()) {
        json.append(",\"locality\":").append(quote(city));
    }
    if (country != null && !country.trim().isEmpty()) {
        json.append(",\"country\":").append(quote(country));
    }
    json.append("}]");
}

    // SAP Extension: validFrom, validTo
    if (hasSapFields) {
        json.append(",\"urn:ietf:params:scim:schemas:extension:sap:2.0:User\":{");
        boolean firstSapField = true;
        
        if (userData.containsKey("validFrom") && userData.get("validFrom") != null) {
            json.append("\"validFrom\":").append(quote(userData.get("validFrom").toString()));
            firstSapField = false;
        }
        
        if (userData.containsKey("validTo") && userData.get("validTo") != null) {
            if (!firstSapField) json.append(",");
            json.append("\"validTo\":").append(quote(userData.get("validTo").toString()));
        }
        
        json.append("}");
    }

    // Enterprise Extension: company
    if (hasEnterpriseFields) {
        String company = (String) userData.get("company");
        json.append(",\"urn:ietf:params:scim:schemas:extension:enterprise:2.0:User\":{");
        json.append("\"organization\":").append(quote(company));
        json.append("}");
    }

    json.append("}");
    return json.toString();
}

/**
 * Helper to quote and escape JSON strings
 */
private String quote(String s) {
    if (s == null) return "null";
    return "\"" + escapeJson(s) + "\"";
}   

    public Map<String, Object> patchUser(String userId, Map<String, Object> updates) throws Exception {
    String token = tokenService.getAccessToken();

    List<Map<String, Object>> operations = new ArrayList<>();

    // Handle loginName (userName)
    if (updates.containsKey("loginName")) {
        operations.add(Map.of(
            "op", "replace",
            "path", "userName",
            "value", updates.get("loginName")
        ));
    }

    if (updates.containsKey("lastName")) {
        operations.add(Map.of(
            "op", "replace",
            "path", "name.familyName",
            "value", updates.get("lastName")
        ));
    }

    if (updates.containsKey("firstName")) {
        operations.add(Map.of(
            "op", "replace",
            "path", "name.givenName",
            "value", updates.get("firstName")
        ));
    }

    if (updates.containsKey("email")) {
        operations.add(Map.of(
            "op", "replace",
            "path", "emails[0].value",
            "value", updates.get("email")
        ));
    }

    if (updates.containsKey("status")) {
        String status = (String) updates.get("status");
        boolean active = "ACTIVE".equalsIgnoreCase(status);
        operations.add(Map.of(
            "op", "replace",
            "path", "active",
            "value", active
        ));
    }

    if (updates.containsKey("userType")) {
        operations.add(Map.of(
            "op", "replace",
            "path", "userType",
            "value", updates.get("userType")
        ));
    }

    if (updates.containsKey("company")) {
        operations.add(Map.of(
            "op", "replace",
            "path", "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User:organization",
            "value", updates.get("company")
        ));
    }

    if (updates.containsKey("city")) {
        operations.add(Map.of(
            "op", "replace",
            "path", "addresses[type eq \"work\"].locality",
            "value", updates.get("city")
        ));
    }

    if (updates.containsKey("country")) {
        operations.add(Map.of(
            "op", "replace",
            "path", "addresses[type eq \"work\"].country",
            "value", updates.get("country")
        ));
    }

    if (updates.containsKey("validFrom")) {
        operations.add(Map.of(
            "op", "replace",
            "path", "urn:ietf:params:scim:schemas:extension:sap:2.0:User:validFrom",
            "value", updates.get("validFrom")
        ));
    }

    if (updates.containsKey("validTo")) {
        operations.add(Map.of(
            "op", "replace",
            "path", "urn:ietf:params:scim:schemas:extension:sap:2.0:User:validTo",
            "value", updates.get("validTo")
        ));
    }

    Map<String, Object> patchRequest = Map.of(
        "schemas", List.of("urn:ietf:params:scim:api:messages:2.0:PatchOp"),
        "Operations", operations
    );

    // Convert to JSON using Jackson
    String patchJson = objectMapper.writeValueAsString(patchRequest);

    String base = scimBaseUrl.endsWith("/")
        ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
        : scimBaseUrl;

    URI uri = URI.create(base + "/Users/" + userId);

    HttpRequest req = HttpRequest.newBuilder(uri)
        .header("Accept", "application/scim+json, application/json")
        .header("Authorization", "Bearer " + token)
        .header("Content-Type", "application/scim+json")
        .method("PATCH", HttpRequest.BodyPublishers.ofString(patchJson))
        .build();

    HttpResponse<String> response = http.send(req, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() >= 200 && response.statusCode() < 300) {
      
        String responseBody = response.body();
        
        if (responseBody == null || responseBody.trim().isEmpty()) {
          
            return Map.of(
                "success", true,
                "statusCode", response.statusCode(),
                "message", "User updated successfully (no response body)"
            );
        }
        
        // Parse JSON response back to Map using Jackson
        return objectMapper.readValue(responseBody, Map.class);
    } else {
        throw new RuntimeException("PATCH failed with status " + response.statusCode() + ": " + response.body());
    }
}

    /**
 * Delete a user from IAS via SCIM API
 * @param userId The IAS user ID
 * @return HttpResponse with the result
 */
public HttpResponse<String> deleteUser(String userId) {
    try {
        String token = tokenService.getAccessToken();

        String base = scimBaseUrl.endsWith("/")
            ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
            : scimBaseUrl;

        URI uri = URI.create(base + "/Users/" + userId);

        HttpRequest req = HttpRequest.newBuilder(uri)
            .header("Accept", "application/scim+json, application/json")
            .header("Authorization", "Bearer " + token)
            .DELETE()
            .build();

        return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
        return new SimpleStringResponse(
            500,
            "{\"error\":\"SCIM delete user failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}"
        );
    }
}

    // ========== GROUP METHODS ==========

  /**
   * Get all groups from IAS
   */
  public HttpResponse<String> getGroups() {
    try {
      String token = tokenService.getAccessToken();

      String base = scimBaseUrl.endsWith("/")
          ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
          : scimBaseUrl;

      URI uri = URI.create(base + "/Groups");

      HttpRequest req = HttpRequest.newBuilder(uri)
          .header("Accept", "application/scim+json, application/json")
          .header("Authorization", "Bearer " + token)
          .GET()
          .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM call failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}"
      );
    }
  }

  /**
   * Create a new group in IAS
   */
  public HttpResponse<String> createGroup(String name, String displayName, String description) {
    try {
      String token = tokenService.getAccessToken();

      String base = scimBaseUrl.endsWith("/")
          ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
          : scimBaseUrl;

      URI uri = URI.create(base + "/Groups");

      // Build SCIM JSON
      String scimJson = "{" +
        "\"schemas\":[\"urn:ietf:params:scim:schemas:core:2.0:Group\",\"urn:sap:cloud:scim:schemas:extension:custom:2.0:Group\"]," +
        "\"displayName\":" + quote(displayName) + "," +
        "\"urn:sap:cloud:scim:schemas:extension:custom:2.0:Group\":{" +
        "\"name\":" + quote(name) + "," +
        "\"description\":" + quote(description) +
        "}" +
        "}";


      HttpRequest req = HttpRequest.newBuilder(uri)
          .header("Accept", "application/scim+json, application/json")
          .header("Authorization", "Bearer " + token)
          .header("Content-Type", "application/scim+json")
          .POST(HttpRequest.BodyPublishers.ofString(scimJson))
          .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM create group failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}"
      );
    }
  }

  /**
   * Update a group in IAS (PATCH)
   */
  public HttpResponse<String> updateGroup(String groupId, String newDisplayName, String newDescription) {
    try {
      String token = tokenService.getAccessToken();

      String base = scimBaseUrl.endsWith("/")
          ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
          : scimBaseUrl;

      URI uri = URI.create(base + "/Groups/" + groupId);
      String patchJson;

      // Build PATCH JSON
      if (newDescription == null || newDescription.trim().isEmpty()){
       patchJson = objectMapper.writeValueAsString(Map.of(
          "schemas", List.of("urn:ietf:params:scim:api:messages:2.0:PatchOp"),
          "Operations", List.of(
              Map.of(
                  "op", "replace",
                  "path", "displayName",
                  "value", newDisplayName
              )
          )
      ));
      }else{
         patchJson = objectMapper.writeValueAsString(Map.of(
        "schemas", List.of("urn:ietf:params:scim:api:messages:2.0:PatchOp"),
        "Operations", List.of(
          Map.of(
              "op", "replace",
              "path", "displayName",
              "value", newDisplayName
          ),
          Map.of(
              "op", "replace",
              "path", "urn:sap:cloud:scim:schemas:extension:custom:2.0:Group:description",
              "value", newDescription
          )
      )
  ));
      }

      HttpRequest req = HttpRequest.newBuilder(uri)
          .header("Accept", "application/scim+json, application/json")
          .header("Authorization", "Bearer " + token)
          .header("Content-Type", "application/scim+json")
          .method("PATCH", HttpRequest.BodyPublishers.ofString(patchJson))
          .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM update group failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}"
      );
    }
  }

  /**
   * Delete a group from IAS
   */
  public HttpResponse<String> deleteGroup(String groupId) {
    try {
      String token = tokenService.getAccessToken();

      String base = scimBaseUrl.endsWith("/")
          ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
          : scimBaseUrl;

      URI uri = URI.create(base + "/Groups/" + groupId);

      HttpRequest req = HttpRequest.newBuilder(uri)
          .header("Accept", "application/scim+json, application/json")
          .header("Authorization", "Bearer " + token)
          .DELETE()
          .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM delete group failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}"
      );
    }
  }

  /**
   * Add members to a group in IAS
   */
  public HttpResponse<String> addGroupMembers(String groupId, List<String> userIds) {
    try {
      String token = tokenService.getAccessToken();

      String base = scimBaseUrl.endsWith("/")
          ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
          : scimBaseUrl;

      URI uri = URI.create(base + "/Groups/" + groupId);

      // Build members array
      List<Map<String, String>> members = new ArrayList<>();
      for (String userId : userIds) {
        members.add(Map.of("value", userId));
      }

      String patchJson = objectMapper.writeValueAsString(Map.of(
          "schemas", List.of("urn:ietf:params:scim:api:messages:2.0:PatchOp"),
          "Operations", List.of(
              Map.of(
                  "op", "add",
                  "path", "members",
                  "value", members
              )
          )
      ));

      HttpRequest req = HttpRequest.newBuilder(uri)
          .header("Accept", "application/scim+json, application/json")
          .header("Authorization", "Bearer " + token)
          .header("Content-Type", "application/scim+json")
          .method("PATCH", HttpRequest.BodyPublishers.ofString(patchJson))
          .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM add members failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}"
      );
    }
  }

  /**
   * Remove a member from a group in IAS
   */
  public HttpResponse<String> removeGroupMember(String groupId, String userId) {
    try {
      String token = tokenService.getAccessToken();

      String base = scimBaseUrl.endsWith("/")
          ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
          : scimBaseUrl;

      URI uri = URI.create(base + "/Groups/" + groupId);

      String patchJson = objectMapper.writeValueAsString(Map.of(
          "schemas", List.of("urn:ietf:params:scim:api:messages:2.0:PatchOp"),
          "Operations", List.of(
              Map.of(
                  "op", "remove",
                  "path", "members[value eq \"" + userId + "\"]"
              )
          )
      ));

      HttpRequest req = HttpRequest.newBuilder(uri)
          .header("Accept", "application/scim+json, application/json")
          .header("Authorization", "Bearer " + token)
          .header("Content-Type", "application/scim+json")
          .method("PATCH", HttpRequest.BodyPublishers.ofString(patchJson))
          .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM remove member failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}"
      );
    }
  }







}

