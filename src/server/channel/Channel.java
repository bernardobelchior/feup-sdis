package server.channel;

import common.Common;
import server.Peer;
import server.Server;

import java.io.ByteArrayOutputStream;
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

    /**
     * Creates the message that only uses a header.
     * Header format:
     * <MessageType> <Version> <SenderId> <FileId> <ChunkNo> <ReplicationDeg> <CRLF>
     *
     * @param headerFields Every field of the header, in the correct sequence.
     * @return Message
     */
    protected static byte[] createMessage(String... headerFields) {
        return (String.join(" ", headerFields) + Server.CRLF + Server.CRLF).getBytes();
    }

    /**
     * Creates a message with header and body
     *
     * @param body         Message body
     * @param headerFields Header fields in sequence.
     * @return Message
     */
    protected static byte[] createMessage(byte[] body, String... headerFields) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        byte[] header = createMessage(headerFields);
        byteArrayOutputStream.write(header, 0, header.length);
        byteArrayOutputStream.write(body, 0, body.length);

        return byteArrayOutputStream.toByteArray();
    }
}
