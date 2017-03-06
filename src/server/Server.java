package server;

import common.Common;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Created by bernardo on 3/6/17.
 */
public class Server {
    public static void main(String[] args) {
        String protocolVersion = args[0];
        int serverId = Integer.parseInt(args[1]);
        String serviceAccessPoint = args[2];

        MulticastSocket controlChannel = Server.createMulticastSocket(args[3], args[4]);
        MulticastSocket backupChannel = Server.createMulticastSocket(args[5], args[6]);
        MulticastSocket recoveryChannel = Server.createMulticastSocket(args[7], args[8]);

        InitiatorPeer initiatorPeer = null;

        try {
            initiatorPeer = new InitiatorPeer(controlChannel, backupChannel, recoveryChannel);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
            Registry registry = LocateRegistry.getRegistry();
            registry.bind(serviceAccessPoint, initiatorPeer);
        } catch (RemoteException | AlreadyBoundException e) {
            e.printStackTrace();
        }

        new Peer(protocolVersion, serverId, controlChannel, backupChannel, recoveryChannel);
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
