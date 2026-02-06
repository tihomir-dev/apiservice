package customer.apiservice.ias;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ScimClient {

  private final HttpClient http =
      HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();

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

      String base =
          scimBaseUrl.endsWith("/")
              ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
              : scimBaseUrl;

      URI uri = URI.create(base + "/Users/" + id);

      HttpRequest req =
          HttpRequest.newBuilder(uri)
              .header("Accept", "application/scim+json, application/json")
              .header("Authorization", "Bearer " + token)
              .GET()
              .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM call failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}");
    }
  }

  public HttpResponse<String> getUsers(String query) {
    try {
      //REVIEW: extract token and base initialization in a private method just so you can remove redundant repetition in every method- 
      //cleans things up
      String token = tokenService.getAccessToken();

      String base =
          scimBaseUrl.endsWith("/")
              ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
              : scimBaseUrl;

      String q = (query == null || query.isBlank()) ? "" : ("?" + query);
      URI uri = URI.create(base + "/Users" + q);

      HttpRequest req =
          HttpRequest.newBuilder(uri)
              .header("Accept", "application/scim+json, application/json")
              .header("Authorization", "Bearer " + token)
              .GET()
              .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM call failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}");
    }
  }

  public HttpResponse<String> getUsers() {
    try {
      String token = tokenService.getAccessToken();

      String base =
          scimBaseUrl.endsWith("/")
              ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
              : scimBaseUrl;

      URI uri = URI.create(base + "/Users");

      HttpRequest req =
          HttpRequest.newBuilder(uri)
              .header("Accept", "application/scim+json, application/json")
              .header("Authorization", "Bearer " + token)
              .GET()
              .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM call failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}");
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

    @Override
    public int statusCode() {
      return status;
    }

    @Override
    public String body() {
      return body;
    }

    @Override
    public HttpRequest request() {
      return null;
    }

    @Override
    public java.util.Optional<HttpResponse<String>> previousResponse() {
      return java.util.Optional.empty();
    }

    @Override
    public java.net.http.HttpHeaders headers() {
      return java.net.http.HttpHeaders.of(java.util.Map.of(), (a, b) -> true);
    }

    @Override
    public URI uri() {
      return null;
    }

    @Override
    public HttpClient.Version version() {
      return HttpClient.Version.HTTP_1_1;
    }

    @Override
    public java.util.Optional<javax.net.ssl.SSLSession> sslSession() {
      return java.util.Optional.empty();
    }
  }

  /** Creates a new user in IAS via SCIM API */
  public HttpResponse<String> createUser(Map<String, Object> userData) {
    try {
      String token = tokenService.getAccessToken();

      String base =
          scimBaseUrl.endsWith("/")
              ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
              : scimBaseUrl;

      URI uri = URI.create(base + "/Users");

      // Build SCIM JSON payload
      String scimJson = buildScimUserJson(userData);

      HttpRequest req =
          HttpRequest.newBuilder(uri)
              .header("Accept", "application/scim+json, application/json")
              .header("Authorization", "Bearer " + token)
              .header("Content-Type", "application/scim+json")
              .POST(HttpRequest.BodyPublishers.ofString(scimJson))
              .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM create user failed\",\"message\":\""
              + escapeJson(e.getMessage())
              + "\"}");
    }
  }

  /** Builds SCIM JSON for user creation */
  private String buildScimUserJson(Map<String, Object> userData) {
    StringBuilder json = new StringBuilder();
    //You can use Jackson ObjectMapper , string builder for a json is risky and prone to fail if not careful. 
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
    //REVIEW: null handling?
    boolean active = !"INACTIVE".equalsIgnoreCase(status);
    json.append("\"active\":").append(active);

    // Optional: addresses (city, country) - WORK ADDRESS
    String city = (String) userData.get("city");
    String country = (String) userData.get("country");
    if ((city != null && !city.trim().isEmpty())
        || (country != null && !country.trim().isEmpty())) {
      json.append(",\"addresses\":[{");
      json.append("\"type\":\"work\",\"primary\":false");
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

  /** Helper to quote and escape JSON strings */
  private String quote(String s) {
    if (s == null) return "null";
    return "\"" + escapeJson(s) + "\"";
  }

  public Map<String, Object> patchUser(String userId, Map<String, Object> updates)
      throws Exception {
    String token = tokenService.getAccessToken();

    List<Map<String, Object>> operations = new ArrayList<>();
    //REVIEW: Check if updates has any data, otherwise you're sending empty PATCHes 
    // Handle loginName (userName)
    if (updates.containsKey("loginName")) {
      Object loginNameValue = updates.get("loginName");
      if (loginNameValue != null) {
        Map<String, Object> op = new HashMap<>();
        op.put("op", "replace");
        op.put("path", "userName");
        op.put("value", loginNameValue);
        operations.add(op);
      }
    }

    if (updates.containsKey("lastName")) {
      Object lastNameValue = updates.get("lastName");
      if (lastNameValue != null) {
        Map<String, Object> op = new HashMap<>();
        op.put("op", "replace");
        op.put("path", "name.familyName");
        op.put("value", lastNameValue);
        operations.add(op);
      }
    }

    if (updates.containsKey("firstName")) {
      Object firstNameValue = updates.get("firstName");
      if (firstNameValue != null) {
        Map<String, Object> op = new HashMap<>();
        op.put("op", "replace");
        op.put("path", "name.givenName");
        op.put("value", firstNameValue);
        operations.add(op);
      }
    }

    if (updates.containsKey("email")) {
      Object emailValue = updates.get("email");
      if (emailValue != null) {
        Map<String, Object> op = new HashMap<>();
        op.put("op", "replace");
        op.put("path", "emails[primary eq true].value");
        op.put("value", emailValue);
        operations.add(op);
      }
    }

    if (updates.containsKey("status")) {
      String status = (String) updates.get("status");
      if (status != null) {
        boolean active = "ACTIVE".equalsIgnoreCase(status);
        Map<String, Object> op = new HashMap<>();
        op.put("op", "replace");
        op.put("path", "active");
        op.put("value", active);
        operations.add(op);
      }
    }

    if (updates.containsKey("userType")) {
      Object userTypeValue = updates.get("userType");
      if (userTypeValue != null) {
        Map<String, Object> op = new HashMap<>();
        op.put("op", "replace");
        op.put("path", "userType");
        op.put("value", userTypeValue);
        operations.add(op);
      }
    }

    if (updates.containsKey("company")) {
      Object companyValue = updates.get("company");
      if (companyValue != null) {
        Map<String, Object> op = new HashMap<>();
        op.put("op", "replace");
        op.put("path", "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User:organization");
        op.put("value", companyValue);
        operations.add(op);
      }
    }

    if (updates.containsKey("city")) {
      Object cityValue = updates.get("city");
      if (cityValue != null) {
        Map<String, Object> op = new HashMap<>();
        op.put("op", "replace");
        op.put("path", "addresses[type eq \"work\"].locality");
        op.put("value", cityValue);
        operations.add(op);
      }
    }

    if (updates.containsKey("country")) {
      Object countryValue = updates.get("country");
      if (countryValue != null) {
        Map<String, Object> op = new HashMap<>();
        op.put("op", "replace");
        op.put("path", "addresses[type eq \"work\"].country");
        op.put("value", countryValue);
        operations.add(op);
      }
    }

    if (updates.containsKey("validFrom")) {
      Object validFromValue = updates.get("validFrom");
      if (validFromValue != null) {
        Map<String, Object> op = new HashMap<>();
        op.put("op", "replace");
        op.put("path", "urn:ietf:params:scim:schemas:extension:sap:2.0:User:validFrom");
        op.put("value", validFromValue);
        operations.add(op);
      }
    }
    if (updates.containsKey("validTo")) {
      Object validToValue = updates.get("validTo");
      if (validToValue != null) {
        Map<String, Object> op = new HashMap<>();
        op.put("op", "replace");
        op.put("path", "urn:ietf:params:scim:schemas:extension:sap:2.0:User:validTo");
        op.put("value", validToValue);
        operations.add(op);
      }
    }

    Map<String, Object> patchRequest = new HashMap<>();
    patchRequest.put("schemas", List.of("urn:ietf:params:scim:api:messages:2.0:PatchOp"));
    patchRequest.put("Operations", operations);

    String patchJson = objectMapper.writeValueAsString(patchRequest);

    String base =
        scimBaseUrl.endsWith("/")
            ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
            : scimBaseUrl;

    URI uri = URI.create(base + "/Users/" + userId);

    HttpRequest req =
        HttpRequest.newBuilder(uri)
            .header("Accept", "application/scim+json, application/json")
            .header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/scim+json")
            .method("PATCH", HttpRequest.BodyPublishers.ofString(patchJson))
            .build();

    HttpResponse<String> response = http.send(req, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() >= 200 && response.statusCode() < 300) {

      String responseBody = response.body();

      if (responseBody == null || responseBody.trim().isEmpty()) {

        Map<String, Object> successResponse = new HashMap<>();
        successResponse.put("success", true);
        successResponse.put("statusCode", response.statusCode());
        successResponse.put("message", "User updated successfully (no response body)");
        return successResponse;
      }

      return objectMapper.readValue(responseBody, Map.class);
    } else {
      //REVIEW: You can retry if the exception is 401 - just fetch a new token if the last one was not properly initialized or it has expired
      //REVIEW: You can return an error HTTPResponse instead instead of just throwing
      throw new RuntimeException(
          "PATCH failed with status " + response.statusCode() + ": " + response.body());
    }
  }

  /** Delete a user from IAS via SCIM API */
  public HttpResponse<String> deleteUser(String userId) {
    try {
      String token = tokenService.getAccessToken();
      //REVIEW: Extract this in a method maybe? It repeats in every method and makes one slight adjustment to the string if needed a pain
      String base =
          scimBaseUrl.endsWith("/")
              ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
              : scimBaseUrl;

      URI uri = URI.create(base + "/Users/" + userId);

      HttpRequest req =
          HttpRequest.newBuilder(uri)
              .header("Accept", "application/scim+json, application/json")
              .header("Authorization", "Bearer " + token)
              .DELETE()
              .build();
      //REVIEW: Add timeouts to http.sends
      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM delete user failed\",\"message\":\""
              + escapeJson(e.getMessage())
              + "\"}");
    }
  }

  // GROUP METHODS

  /** Get all groups from IAS */
  public HttpResponse<String> getGroups() {
    try {
      String token = tokenService.getAccessToken();

      String base =
          scimBaseUrl.endsWith("/")
              ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
              : scimBaseUrl;

      URI uri = URI.create(base + "/Groups");

      HttpRequest req =
          HttpRequest.newBuilder(uri)
              .header("Accept", "application/scim+json, application/json")
              .header("Authorization", "Bearer " + token)
              .GET()
              .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM call failed\",\"message\":\"" + escapeJson(e.getMessage()) + "\"}");
    }
  }

  /** Create a new group in IAS */
  public HttpResponse<String> createGroup(String name, String displayName, String description) {
    try {
      String token = tokenService.getAccessToken();

      String base =
          scimBaseUrl.endsWith("/")
              ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
              : scimBaseUrl;

      URI uri = URI.create(base + "/Groups");
      //REVIEW: Better use JacksonMaps or POJOs for such Json building
      // Build SCIM JSON
      String scimJson =
          "{\"schemas\":[\"urn:ietf:params:scim:schemas:core:2.0:Group\",\"urn:sap:cloud:scim:schemas:extension:custom:2.0:Group\"],"
              + "\"displayName\":"
              + quote(displayName)
              + ","
              + "\"urn:sap:cloud:scim:schemas:extension:custom:2.0:Group\":{"
              + "\"name\":"
              + quote(name)
              + ","
              + "\"description\":"
              + quote(description)
              + "}"
              + "}";

      HttpRequest req =
          HttpRequest.newBuilder(uri)
              .header("Accept", "application/scim+json, application/json")
              .header("Authorization", "Bearer " + token)
              .header("Content-Type", "application/scim+json")
              .POST(HttpRequest.BodyPublishers.ofString(scimJson))
              .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM create group failed\",\"message\":\""
              + escapeJson(e.getMessage())
              + "\"}");
    }
  }

  /** Update a group in IAS (PATCH) */
  public HttpResponse<String> updateGroup(
      String groupId, String newDisplayName, String newDescription) {
    try {
      String token = tokenService.getAccessToken();

      String base =
          scimBaseUrl.endsWith("/")
              ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
              : scimBaseUrl;

      URI uri = URI.create(base + "/Groups/" + groupId);
      String patchJson;

      // Build PATCH JSON
      if (newDescription == null || newDescription.trim().isEmpty()) {
        patchJson =
            objectMapper.writeValueAsString(
                Map.of(
                    "schemas", List.of("urn:ietf:params:scim:api:messages:2.0:PatchOp"),
                    "Operations",
                        List.of(
                            Map.of(
                                "op", "replace",
                                "path", "displayName",
                                "value", newDisplayName))));
      } else {
        patchJson =
            objectMapper.writeValueAsString(
                Map.of(
                    "schemas", List.of("urn:ietf:params:scim:api:messages:2.0:PatchOp"),
                    "Operations",
                        List.of(
                            Map.of(
                                "op", "replace",
                                "path", "displayName",
                                "value", newDisplayName),
                            Map.of(
                                "op", "replace",
                                "path",
                                    "urn:sap:cloud:scim:schemas:extension:custom:2.0:Group:description",
                                "value", newDescription))));
      }

      HttpRequest req =
          HttpRequest.newBuilder(uri)
              .header("Accept", "application/scim+json, application/json")
              .header("Authorization", "Bearer " + token)
              .header("Content-Type", "application/scim+json")
              .method("PATCH", HttpRequest.BodyPublishers.ofString(patchJson))
              .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      //SimpleString response may cause issues with methods from different libraries excepting an actual HttpResponse - try to return a 
      //wrapper object with status and body
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM update group failed\",\"message\":\""
              + escapeJson(e.getMessage())
              + "\"}");
    }
  }

  /** Delete a group from IAS */
  public HttpResponse<String> deleteGroup(String groupId) {
    try {
      String token = tokenService.getAccessToken();

      String base =
          scimBaseUrl.endsWith("/")
              ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
              : scimBaseUrl;

      URI uri = URI.create(base + "/Groups/" + groupId);

      HttpRequest req =
          HttpRequest.newBuilder(uri)
              .header("Accept", "application/scim+json, application/json")
              .header("Authorization", "Bearer " + token)
              .DELETE()
              .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM delete group failed\",\"message\":\""
              + escapeJson(e.getMessage())
              + "\"}");
    }
  }

  /** Add members to a group in IAS */
  public HttpResponse<String> addGroupMembers(String groupId, List<String> userIds) {
    try {
      String token = tokenService.getAccessToken();

      String base =
          scimBaseUrl.endsWith("/")
              ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
              : scimBaseUrl;

      URI uri = URI.create(base + "/Groups/" + groupId);

      // Build members array
      List<Map<String, String>> members = new ArrayList<>();
      for (String userId : userIds) {
        members.add(Map.of("value", userId));
      }

      String patchJson =
          objectMapper.writeValueAsString(
              Map.of(
                  "schemas", List.of("urn:ietf:params:scim:api:messages:2.0:PatchOp"),
                  "Operations",
                      List.of(
                          Map.of(
                              "op", "add",
                              "path", "members",
                              "value", members))));

      HttpRequest req =
          HttpRequest.newBuilder(uri)
              .header("Accept", "application/scim+json, application/json")
              .header("Authorization", "Bearer " + token)
              .header("Content-Type", "application/scim+json")
              .method("PATCH", HttpRequest.BodyPublishers.ofString(patchJson))
              .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM add members failed\",\"message\":\""
              + escapeJson(e.getMessage())
              + "\"}");
    }
  }

  /** Remove a member from a group in IAS */
  public HttpResponse<String> removeGroupMember(String groupId, String userId) {
    try {
      String token = tokenService.getAccessToken();

      String base =
          scimBaseUrl.endsWith("/")
              ? scimBaseUrl.substring(0, scimBaseUrl.length() - 1)
              : scimBaseUrl;

      URI uri = URI.create(base + "/Groups/" + groupId);

      String patchJson =
          objectMapper.writeValueAsString(
              Map.of(
                  "schemas", List.of("urn:ietf:params:scim:api:messages:2.0:PatchOp"),
                  "Operations",
                      List.of(
                          Map.of("op", "remove", "path", "members[value eq \"" + userId + "\"]"))));

      HttpRequest req =
          HttpRequest.newBuilder(uri)
              .header("Accept", "application/scim+json, application/json")
              .header("Authorization", "Bearer " + token)
              .header("Content-Type", "application/scim+json")
              .method("PATCH", HttpRequest.BodyPublishers.ofString(patchJson))
              .build();

      return http.send(req, HttpResponse.BodyHandlers.ofString());

    } catch (Exception e) {
      return new SimpleStringResponse(
          500,
          "{\"error\":\"SCIM remove member failed\",\"message\":\""
              + escapeJson(e.getMessage())
              + "\"}");
    }
  }
}
