package server.messaging;

import common.Common;
import server.Controller;
import server.Server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class Channel {
    private MulticastSocket socket;
    private Controller controller;
    private InetAddress address;
    private int port;

    public Channel(String address, String port) {
        socket = createMulticastSocket(address, port);
    }

    public void setController(Controller controller) {
        this.controller = controller;
    }

    private MulticastSocket createMulticastSocket(String addressStr, String portStr) {
        address = Common.parseAddress(addressStr);
        port = Integer.parseInt(portStr);

        MulticastSocket socket = null;
        try {
            socket = new MulticastSocket(port);
            socket.setTimeToLive(1);
            socket.joinGroup(address);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return socket;
    }

    public void sendMessage(byte[] message) {
        DatagramPacket packet = new DatagramPacket(message, message.length, address, port);
        System.out.println(new String(message));

        try {
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void listen() {
        new Thread(() -> {
            while (true) {
                byte[] buffer = new byte[Server.MAX_HEADER_SIZE + Server.CHUNK_SIZE];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                try {
                    socket.receive(packet);
                    controller.processMessage(packet.getData(), packet.getLength());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
