package server;

import java.net.InetAddress;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import common.Common;
import common.IBackupServer;

public class Peer {
    public static void main(String[] args) {

        int protocolVersion = Integer.parseInt(args[0]);

        int serverId = Integer.parseInt(args[1]);

        String serviceAccessPoint = args[2];

        InetAddress controlChannelAddr = Common.parseAddress(args[3]);
        int controlChannelPort = Integer.parseInt(args[4]);

        InetAddress dataChannelAddr = Common.parseAddress(args[5]);
        int dataChannelPort = Integer.parseInt(args[6]);

        InetAddress dataRecoveryChannelAddr = Common.parseAddress(args[7]);
        int dataRecoveryChannelPort = Integer.parseInt(args[8]);

        InitiatorPeer initiatorPeer = null;

        try {
           initiatorPeer = new InitiatorPeer();
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
           IBackupServer stub = (IBackupServer) UnicastRemoteObject.exportObject(initiatorPeer,0);
            Registry registry = LocateRegistry.getRegistry();
            registry.bind(serviceAccessPoint,stub);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        }
    }

}

