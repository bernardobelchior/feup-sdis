package server.channel;

import common.Common;
import server.Peer;
import server.Server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Arrays;

public class Channel {
    protected Peer peer;
    protected MulticastSocket socket;

    public Channel(Peer peer, String address, String port) {
        this.peer = peer;
        socket = createMulticastSocket(address, port);
        listen();
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

    protected void sendMessage(byte[] message) {
        DatagramPacket packet = new DatagramPacket(message, message.length);

        try {
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        return (String.join(" ", headerFields) + " " + Server.CRLF + Server.CRLF).getBytes();
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

    private void listen() {
        byte[] buffer = new byte[Server.MAX_HEADER_SIZE + Server.CHUNK_SIZE];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        try {
            socket.receive(packet);
            byte[] message = Arrays.copyOf(buffer, packet.getLength());
            new Thread(() -> peer.processMessage(message)).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
