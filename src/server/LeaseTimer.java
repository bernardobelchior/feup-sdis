package server;

import server.messaging.Channel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static server.Server.*;
import static server.Utils.randomBetween;
import static server.messaging.MessageBuilder.createMessage;

public class LeaseTimer {
    private static final int LEASE_TIMER = 60; // In seconds
    private static final int LEASE_TIMEOUT = 1; // In seconds
    private static final int LEASE_MIN_DELAY = 0; // In milliseconds
    private static final int LEASE_MAX_DELAY = 400; // In milliseconds

    private Controller controller;
    private final Channel controlChannel;
    private final ScheduledExecutorService leaseScheduledExecutor = Executors.newScheduledThreadPool(5);
    private final ConcurrentHashMap<String, Boolean> filesLeaseState = new ConcurrentHashMap<>();

    LeaseTimer(Controller controller, Channel controlChannel) {
        this.controller = controller;
        this.controlChannel = controlChannel;
    }

    public void startLease(String fileId) {
        if (!filesLeaseState.containsKey(fileId)) {
            filesLeaseState.put(fileId, true);
            leaseScheduledExecutor.schedule(() -> sendLeaseMessage(fileId), LEASE_TIMER, TimeUnit.SECONDS);
        }
    }

    private void sendLeaseMessage(String fileId) {
        filesLeaseState.put(fileId, false);

        try {
            Thread.sleep(randomBetween(LEASE_MIN_DELAY, LEASE_MAX_DELAY));
        } catch (InterruptedException ignored) {
        }

        if (!filesLeaseState.get(fileId)) {
            controlChannel.sendMessage(createMessage(
                    DELETE_GET_LEASE,
                    Double.toString(getProtocolVersion()),
                    Integer.toString(getServerId()),
                    fileId
            ));
        }

        leaseScheduledExecutor.schedule(() -> verifyLease(fileId), LEASE_TIMEOUT, TimeUnit.SECONDS);
    }

    private void verifyLease(String fileId) {
        if (!filesLeaseState.getOrDefault(fileId, false)) {
            controller.deleteFile(fileId);
            System.out.println("Lease license expired for fileId " + fileId + ".");
            return;
        }

        leaseScheduledExecutor.schedule(() -> sendLeaseMessage(fileId), LEASE_TIMER, TimeUnit.SECONDS);
    }

    public void leaseRenewed(String fileId) {
        if (!filesLeaseState.getOrDefault(fileId, false)) {
            filesLeaseState.put(fileId, true);
            System.out.println("Renewed license for fileId " + fileId + ".");
        }
    }

    public void leaseEnded(String fileId) {
        filesLeaseState.remove(fileId);
    }

}
