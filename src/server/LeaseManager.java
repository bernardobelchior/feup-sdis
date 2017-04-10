package server;

import server.messaging.Channel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static server.Server.*;
import static server.Utils.randomBetween;
import static server.messaging.MessageBuilder.createMessage;

class LeaseManager {
    /**
     * Lease duration in seconds.
     */
    private static final int LEASE_DURATION = 60;

    /**
     * Maximum time to wait for a lease renewal.
     */
    private static final int LEASE_TIMEOUT = 1; // In seconds

    /**
     * Minimum lease delay to reply, in order to avoid multicast congestion.
     */
    private static final int LEASE_MIN_DELAY = 0; // In milliseconds

    /**
     * Maximum lease delay to reply, in order to avoid multicast congestion.
     */
    private static final int LEASE_MAX_DELAY = 400; // In milliseconds

    /**
     * Controller.
     */
    private final Controller controller;

    /**
     * Control channel.
     */
    private final Channel controlChannel;

    /**
     * Thread pool that handles lease requests.
     */
    private final ScheduledExecutorService leaseScheduledExecutor = Executors.newScheduledThreadPool(5);

    /**
     * Maps fileId to lease validity.
     */
    private final ConcurrentHashMap<String, Boolean> fileLeaseValidity = new ConcurrentHashMap<>();

    LeaseManager(Controller controller, Channel controlChannel) {
        this.controller = controller;
        this.controlChannel = controlChannel;
    }

    /**
     * Starts the lease of the file with fileId.
     *
     * @param fileId FileId
     */
    public void startLease(String fileId) {
        if (!fileLeaseValidity.containsKey(fileId)) {
            fileLeaseValidity.put(fileId, true);
            leaseScheduledExecutor.schedule(() -> sendLeaseMessage(fileId), LEASE_DURATION, TimeUnit.SECONDS);
        }
    }

    /**
     * Sends a message to renew lease.
     *
     * @param fileId FileId
     */
    private void sendLeaseMessage(String fileId) {
        fileLeaseValidity.put(fileId, false);

        try {
            Thread.sleep(randomBetween(LEASE_MIN_DELAY, LEASE_MAX_DELAY));
        } catch (InterruptedException ignored) {
        }

        if (!fileLeaseValidity.get(fileId)) {
            controlChannel.sendMessage(createMessage(
                    DELETE_GET_LEASE,
                    Double.toString(getProtocolVersion()),
                    Integer.toString(getServerId()),
                    fileId
            ));
        }

        leaseScheduledExecutor.schedule(() -> verifyLease(fileId), LEASE_TIMEOUT, TimeUnit.SECONDS);
    }

    /**
     * Verifies the lease state.
     *
     * @param fileId File id
     */
    private void verifyLease(String fileId) {
        /* If the lease is valid, then schedule the next check. Otherwise, end the lease. */
        if (fileLeaseValidity.getOrDefault(fileId, false)) {
            leaseScheduledExecutor.schedule(() -> sendLeaseMessage(fileId), LEASE_DURATION, TimeUnit.SECONDS);
        } else {
            System.out.println("Lease license expired for fileId " + fileId + ".");
            controller.fileManager.deleteFile(fileId);
        }
    }

    /**
     * Renews the lease for fileId.
     *
     * @param fileId File id
     */
    public void leaseRenew(String fileId) {
        if (!fileLeaseValidity.getOrDefault(fileId, false)) {
            fileLeaseValidity.put(fileId, true);
            System.out.println("Renewed license for fileId " + fileId + ".");
        }
    }

    /**
     * Ends the lease for fileId.
     *
     * @param fileId File Id.
     */
    public void leaseEnd(String fileId) {
        fileLeaseValidity.remove(fileId);
    }

}
