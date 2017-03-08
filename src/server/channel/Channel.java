package server.channel;

import common.Common;
import server.Server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Arrays;

public class Channel {
    private MulticastSocket socket;
    private ChannelManager channelManager;

    public Channel(String address, String port) {
        socket = createMulticastSocket(address, port);
    }

    public void setManager(ChannelManager channelManager){
        this.channelManager = channelManager;
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

    public void sendMessage(byte[] message) {
        DatagramPacket packet = new DatagramPacket(message, message.length);

        try {
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void listen() {
        byte[] buffer = new byte[Server.MAX_HEADER_SIZE + Server.CHUNK_SIZE];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        try {
            socket.receive(packet);
            byte[] message = Arrays.copyOf(buffer, packet.getLength());
            channelManager.processMessage(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
