package server;

import common.Common;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Server {
    // Chunk Backup
    public static final String BACKUP_INIT = "PUTCHUNK";
    public static final String BACKUP_SUCCESS = "STORED";

    // Chunk Restore
    public static final String RESTORE_INIT = "GETCHUNK";
    public static final String RESTORE_SUCCESS = "CHUNK";

    // File Deletion
    public static final String DELETE_INIT = "DELETE";

    // Space Reclaiming
    public static final String RECLAIM_INIT = "REMOVED"; //TODO: Define implementation
    public static final String RECLAIM_SUCESS = "REMOVED";


    public static final String CRLF = "" + (char) 0xD + (char) 0xA;

    public static final int CHUNK_SIZE = 64*1024;

    public static void main(String[] args) {

        System.setProperty("java.net.preferIPv4Stack", "true");
        String protocolVersion = args[0];
        int serverId = Integer.parseInt(args[1]);
        String serviceAccessPoint = args[2];

        MulticastSocket controlChannel = Server.createMulticastSocket(args[3], args[4]);
        MulticastSocket backupChannel = Server.createMulticastSocket(args[5], args[6]);
        MulticastSocket recoveryChannel = Server.createMulticastSocket(args[7], args[8]);

        Peer peer = null;

        try {
            peer = new Peer(protocolVersion, serverId, controlChannel, backupChannel, recoveryChannel);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
            Registry registry = LocateRegistry.getRegistry();
            registry.bind(serviceAccessPoint, peer);
        } catch (RemoteException | AlreadyBoundException e) {
            e.printStackTrace();
        }

    }

    private static MulticastSocket createMulticastSocket(String addressStr, String portStr) {
        InetAddress address = Common.parseAddress(addressStr);
        int port = Integer.parseInt(portStr);

        MulticastSocket socket = null;
        try {
            socket = new MulticastSocket(port);
            socket.joinGroup(address);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return socket;
    }
}
