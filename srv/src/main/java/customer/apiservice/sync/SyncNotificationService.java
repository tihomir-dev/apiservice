package customer.apiservice.sync;

import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Service;

@Service
public class SyncNotificationService {
  //REVIEW: At runtime you can just create a centralized in-memory storage of all the data Map<String, Map<String,Object>> syncResults = new ConcurrentHashMap<>(); 
  // The idea is that you can use syncResults.clear(); in the clear method , return syncResults.values().stream().anyMatch(Objects::nonNull); in the hasChanges method
  //etc
  private Map<String, Object> lastUserSync;
  private Map<String, Object> lastGroupSync;
  private Map<String, Object> lastUserGroupAssignmentSync;
  private Map<String, Object> lastGroupMembersSync;
  private boolean hasChanges = false;

  public Map<String, Object> getNotification() {
    Map<String, Object> notification = new HashMap<>();
    notification.put("hasChanges", hasChanges);
    //REVIEW: 
    /*
    return syncResults.entrySet().stream()
    .filter(e -> e.getValue() != null)
    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    */
    if (hasChanges) {
      // Only add non-null sync data
      if (lastUserSync != null) {
        notification.put("users", lastUserSync);
      }
      if (lastGroupSync != null) {
        notification.put("groups", lastGroupSync);
      }
      if (lastUserGroupAssignmentSync != null) {
        notification.put("userGroupAssignments", lastUserGroupAssignmentSync);
      }
      if (lastGroupMembersSync != null) {
        notification.put("groupMembers", lastGroupMembersSync);
      }
    }

    return notification;
  }

  public void notifyUserSync(Map<String, Object> result) {
    this.lastUserSync = result;
    this.hasChanges = true;
  }

  public void notifyGroupSync(Map<String, Object> result) {
    this.lastGroupSync = result;
    this.hasChanges = true;
  }
  
  public void clearNotifications() {
    this.hasChanges = false;
    this.lastUserSync = null;
    this.lastGroupSync = null;
    this.lastUserGroupAssignmentSync = null;
    this.lastGroupMembersSync = null;
  }

  public void notifyUserGroupAssignmentSync(Map<String, Object> result) {
    this.lastUserGroupAssignmentSync = result;
    this.hasChanges = true;
  }

  public void notifyGroupMembersSync(Map<String, Object> result) {
    this.lastGroupMembersSync = result;
    this.hasChanges = true;
  }
}
