package customer.apiservice.ias;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@Component
public class ScimClient {

  private final HttpClient http = HttpClient.newBuilder()
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build();

  private final IasTokenService tokenService;

  @Value("${IAS_SCIM_BASE_URL}")
  private String scimBaseUrl; // e.g. https://.../scim/v2

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
}
