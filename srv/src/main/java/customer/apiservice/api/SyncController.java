package customer.apiservice.api;

import customer.apiservice.sync.SyncNotificationService;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/sync")
public class SyncController {

  @Autowired private SyncNotificationService syncNotificationService;

  @GetMapping("/notification")
  public ResponseEntity<Map<String, Object>> getNotification() {
    return ResponseEntity.ok(syncNotificationService.getNotification());
  }

  @PostMapping("/notification/clear")
  public ResponseEntity<Void> clearNotification() {
    syncNotificationService.clearNotifications();
    return ResponseEntity.ok().build();
  }
}
