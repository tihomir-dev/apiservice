package customer.apiservice.scheduler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;
import customer.apiservice.sync.GroupService;  
import customer.apiservice.sync.UserSyncService;

@Slf4j
@Component
public class SyncScheduler {

    @Autowired
    private UserSyncService userSyncService;

    @Autowired
    private GroupService groupService;

    /**
     * Sync users and groups from IAS every 5 minutes
     */
    @Scheduled(fixedRate = 300000) // 300000ms = 5 minutes
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

            long duration = System.currentTimeMillis() - startTime;
            log.info("=== Scheduled sync completed in {} ms ===", duration);

        } catch (Exception e) {
            log.error("Error during scheduled sync of users and groups", e);
        }
    }
}