package server.channel;

import common.Common;
import server.Peer;
import server.Server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;

import static server.MessageBuilder.createMessageWithoutTerminator;

public class Channel {
    protected Peer peer;
    protected MulticastSocket socket;

    public Channel(Peer peer, String address, String port) {
        this.peer = peer;
        socket = createMulticastSocket(address, port);
    }

    protected byte[] createMessage(String messageType, String protocolVersion, int serverId, String fileId, int chunkNo, int replicationDegree, byte[] body) {
        //return (createMessageWithoutTerminator(messageType, protocolVersion, serverId, fileId, chunkNo, replicationDegree) + Server.CRLF + Server.CRLF).getBytes();
        return null;
    }

    protected byte[] createMessage(String messageType, String protocolVersion, int serverId, String fileId, int chunkNo, int replicationDegree) {
        return (createMessageWithoutTerminator(messageType, protocolVersion, serverId, fileId, chunkNo, replicationDegree) + Server.CRLF + Server.CRLF).getBytes();
    }

    protected byte[] createMessage(String messageType, String protocolVersion, int serverId, String fileId, int chunkNo) {
        return (createMessageWithoutTerminator(messageType, protocolVersion, serverId, fileId, chunkNo) + Server.CRLF + Server.CRLF).getBytes();
    }

    protected byte[] createMessage(String messageType, String protocolVersion, int serverId, String fileId) {
        return (createMessageWithoutTerminator(messageType, protocolVersion, serverId, fileId) + Server.CRLF + Server.CRLF).getBytes();
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
