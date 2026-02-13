package customer.apiservice.scheduler;

import customer.apiservice.sync.GroupService;
import customer.apiservice.sync.SyncNotificationService;
import customer.apiservice.sync.UserGroupAssignmentService;
import customer.apiservice.sync.UserSyncService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SyncScheduler {

  @Autowired private UserSyncService userSyncService;

  @Autowired private GroupService groupService;

  @Autowired private SyncNotificationService syncNotificationService;

  @Autowired private UserGroupAssignmentService userGroupAssignmentService;

  /** Sync users and groups from IAS every minute */
  @Scheduled(fixedRate = 60000)
  public void syncUsersAndGroups() {
    log.info("=== Starting scheduled sync of users and groups from IAS ===");
    
    try {
      userSyncService.syncAllUsers();
      groupService.syncGroupsFromIas();
      userGroupAssignmentService.syncUserGroupAssignments();
      //userGroupAssignmentService.syncGroupMembers();
    } catch (Exception e) {
      log.error("Error during scheduled sync of users and groups", e);
    }
  }
}
