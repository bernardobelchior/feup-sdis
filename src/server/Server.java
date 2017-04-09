package server;

import server.messaging.Channel;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Server {
    // File Deletion
    public static final String DELETE_INIT = "DELETE";
    public static final String DELETE_GET_LEASE = "GETLEASE";
    public static final String DELETE_ACCEPT_LEASE = "LEASEOK";

    // Space Reclaiming
    public static final String RECLAIM_SUCESS = "REMOVED";
    public static final int RECLAIM_REPLY_MIN_DELAY = 0;
    public static final int RECLAIM_REPLY_MAX_DELAY = 400;

    /* Sizes in bytes */
    public static final int MAX_HEADER_SIZE = 512;
    public static final int CHUNK_SIZE = 64 * 1000;

    /**
     * Directory in which all other directories will be saved.
     */
    static String BASE_DIR;

    /**
     * Directory in which restored files will be saved.
     */
    public static final String RESTORED_DIR = "RestoredFiles/";

    /**
     * Directory in which chunks will be stored.
     */
    static final String CHUNK_DIR = "Chunks/";

    /**
     * Protocol version.
     */
    private static double protocolVersion;

    /**
     * Server Id.
     */
    private static int serverId;

    public static void main(String[] args) throws InstantiationException {
        /* Needed for Mac OS X */
        System.setProperty("java.net.preferIPv4Stack", "true");

        protocolVersion = Double.parseDouble(args[0]);
        serverId = Integer.parseInt(args[1]);
        String serviceAccessPoint = args[2];

        BASE_DIR = serverId + "/";

        System.out.println("Starting server with id " + serverId + " and protocol version " + protocolVersion + ".");
        System.out.println("Access Point: " + serviceAccessPoint);

        Channel controlChannel = new Channel(args[3], args[4]);
        Channel backupChannel = new Channel(args[5], args[6]);
        Channel recoveryChannel = new Channel(args[7], args[8]);
        FileManager fileManager = new FileManager((long) (Math.pow(1000, 2) * 8), serverId + "/", CHUNK_DIR, RESTORED_DIR); /* 8 Megabytes */

        Controller controller = new Controller(controlChannel, backupChannel, recoveryChannel, fileManager);

        InitiatorPeer initiatorPeer = null;

        try {
            initiatorPeer = new InitiatorPeer(controller);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(serviceAccessPoint, initiatorPeer);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    public static double getProtocolVersion() {
        return protocolVersion;
    }

    public static int getServerId() {
        return serverId;
    }
}
