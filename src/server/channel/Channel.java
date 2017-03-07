package server.channel;

import common.Common;
import server.Peer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class Channel {
    protected Peer peer;
    protected MulticastSocket socket;

    public Channel(Peer peer, String address, String port) {
        this.peer = peer;
        socket = createMulticastSocket(address, port);
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
