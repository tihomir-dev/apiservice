package customer.apiservice.ias;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class IasTokenService {

  private final HttpClient http = HttpClient.newHttpClient();
  private final ObjectMapper om = new ObjectMapper();

  @Value("${IAS_TOKEN_URL}")
  private String tokenUrl;

  @Value("${IAS_CLIENT_ID}")
  private String clientId;

  @Value("${IAS_CLIENT_SECRET}")
  private String clientSecret;

  private volatile String cachedToken;
  private volatile Instant expiresAt;

  public String getAccessToken() {
    if (cachedToken != null
        && expiresAt != null
        && Instant.now().isBefore(expiresAt.minusSeconds(30))) {
      return cachedToken;
    }
    return fetchNewToken();
  }

  private synchronized String fetchNewToken() {
    if (cachedToken != null
        && expiresAt != null
        && Instant.now().isBefore(expiresAt.minusSeconds(30))) {
      return cachedToken;
    }

    try {
      String body = "grant_type=client_credentials";

      String basic =
          Base64.getEncoder()
              .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

      HttpRequest req =
          HttpRequest.newBuilder()
              .uri(URI.create(tokenUrl))
              .header("Authorization", "Basic " + basic)
              .header("Content-Type", "application/x-www-form-urlencoded")
              .header("Accept", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();

      HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

      if (resp.statusCode() / 100 != 2) {
        throw new RuntimeException("IAS token error " + resp.statusCode() + ": " + resp.body());
      }

      JsonNode json = om.readTree(resp.body());
      String token = json.path("access_token").asText(null);
      long expiresIn = json.path("expires_in").asLong(300);

      if (token == null || token.isBlank()) {
        throw new RuntimeException("IAS token response missing access_token: " + resp.body());
      }

      cachedToken = token;
      expiresAt = Instant.now().plusSeconds(expiresIn);
      return token;

    } catch (Exception e) {
      throw new RuntimeException("Failed to obtain IAS access token", e);
    }
  }
}
