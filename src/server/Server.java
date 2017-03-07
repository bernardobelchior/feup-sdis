package server;

import server.channel.BackupChannel;
import server.channel.ControlChannel;
import server.channel.RecoveryChannel;

import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public class Server {
    /* All time-related constants are in milliseconds */
    // Chunk Backup
    public static final String BACKUP_INIT = "PUTCHUNK";
    public static final String BACKUP_SUCCESS = "STORED";
    public static final int BACKUP_TIMEOUT = 1000;
    public static final int BACKUP_REPLY_DELAY = 400;

    // Chunk Restore
    public static final String RESTORE_INIT = "GETCHUNK";
    public static final String RESTORE_SUCCESS = "CHUNK";

    // File Deletion
    public static final String DELETE_INIT = "DELETE";

    // Space Reclaiming
    public static final String RECLAIM_INIT = "RECLAIM"; //TODO: Define implementation
    public static final String RECLAIM_SUCESS = "REMOVED";


    public static final String CRLF = "" + (char) 0xD + (char) 0xA;

    /* Sizes in bytes */
    public static final int MAX_HEADER_SIZE = 512;
    public static final int CHUNK_SIZE = 64 * 1024;

    public static void main(String[] args) {
        /* Needed for Mac OS X */
        System.setProperty("java.net.preferIPv4Stack", "true");

        String protocolVersion = args[0];
        int serverId = Integer.parseInt(args[1]);
        String serviceAccessPoint = args[2];

        Peer peer = new Peer(protocolVersion, serverId);

        ControlChannel controlChannel = new ControlChannel(peer, args[3], args[4]);
        BackupChannel backupChannel = new BackupChannel(peer, args[5], args[6]);
        RecoveryChannel recoveryChannel = new RecoveryChannel(peer, args[7], args[8]);

        InitiatorPeer initiatorPeer = null;

        try {
            initiatorPeer = new InitiatorPeer(controlChannel, backupChannel, recoveryChannel);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
            LocateRegistry.getRegistry().bind(serviceAccessPoint, initiatorPeer);
        } catch (RemoteException | AlreadyBoundException e) {
            e.printStackTrace();
        }

    }
}
