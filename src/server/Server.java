package server;

import server.messaging.Channel;

import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Server {
    /* All time-related constants are in milliseconds */
    // Chunk Backup
    public static final String BACKUP_INIT = "PUTCHUNK";
    public static final String BACKUP_SUCCESS = "STORED";
    public static final int BACKUP_TIMEOUT = 1000;
    public static final int MAX_BACKUP_ATTEMPTS = 5;
    public static final int BACKUP_REPLY_MIN_DELAY = 0;
    public static final int BACKUP_REPLY_MAX_DELAY = 400;

    // Chunk Restore
    public static final String RESTORE_INIT = "GETCHUNK";
    public static final String RESTORE_SUCCESS = "CHUNK";
    public static final int RESTORE_TIMEOUT = 1000;
    public static final int RESTORE_REPLY_MIN_DELAY = 0;
    public static final int RESTORE_REPLY_MAX_DELAY = 400;

    // File Deletion
    public static final String DELETE_INIT = "DELETE";

    // Space Reclaiming
    public static final String RECLAIM_INIT = "RECLAIM"; //TODO: Define implementation
    public static final String RECLAIM_SUCESS = "REMOVED";
    public static final int RECLAIM_REPLY_MIN_DELAY = 0;
    public static final int RECLAIM_REPLY_MAX_DELAY = 400;


    public static final byte CR = 0xD;
    public static final byte LF = 0xA;
    public static final String CRLF = "" + (char) CR + (char) LF;

    /* Sizes in bytes */
    public static final int MAX_HEADER_SIZE = 512;
    public static final int CHUNK_SIZE = 64 * 1000;

    public static final String RESTORED_DIR = "RestoredFiles/";
    public static final String CHUNK_DIR = "Chunks/";
    public static String BASE_DIR;

    private static double protocolVersion;
    private static int serverId;

    public static void main(String[] args) {
        /* Needed for Mac OS X */
        System.setProperty("java.net.preferIPv4Stack", "true");

        protocolVersion = Double.parseDouble(args[0]);
        serverId = Integer.parseInt(args[1]);
        String serviceAccessPoint = args[2];

        BASE_DIR = serverId + "/";

        System.out.println("Starting server with id " + serverId + ".");
        System.out.println("Access Point: " + serviceAccessPoint);

        Channel controlChannel = new Channel(args[3], args[4]);
        Channel backupChannel = new Channel(args[5], args[6]);
        Channel recoveryChannel = new Channel(args[7], args[8]);

        Controller controller = new Controller(controlChannel, backupChannel, recoveryChannel);

        InitiatorPeer initiatorPeer = null;

        try {
            initiatorPeer = new InitiatorPeer(controller);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
            Registry registry = LocateRegistry.getRegistry();
            registry.bind(serviceAccessPoint, initiatorPeer);
        } catch (RemoteException | AlreadyBoundException e) {
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
