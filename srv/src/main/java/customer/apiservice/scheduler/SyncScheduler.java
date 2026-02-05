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
    long startTime = System.currentTimeMillis();

    try {
      log.info("Syncing users from IAS...");
      var userResult = userSyncService.syncAllUsers();
      log.info("Users sync completed: {}", userResult);

      log.info("Syncing groups from IAS...");
      var groupResult = groupService.syncGroupsFromIas();
      log.info("Groups sync completed: {}", groupResult);

      log.info("Syncing user group assignments from IAS...");
      var userGroupAssignmentResult = userGroupAssignmentService.syncUserGroupAssignments();
      log.info("User group assignments sync completed: {}", userGroupAssignmentResult);

      log.info("Syncing group members from IAS...");
      var groupMembersResult = userGroupAssignmentService.syncGroupMembers();
      log.info("Group members sync completed: {}", groupMembersResult);

      long duration = System.currentTimeMillis() - startTime;
      log.info("=== Scheduled sync completed in {} ms ===", duration);

    } catch (Exception e) {
      log.error("Error during scheduled sync of users and groups", e);
    }
  }
}
