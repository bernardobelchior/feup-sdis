package server.messaging;

import common.Common;
import server.Controller;
import server.Server;
import server.Utils;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    /**
     * Sends message with random delay between {min} and {max} milliseconds.
     *
     * @param message Message to send.
     * @param min     Minimum delay in milliseconds.
     * @param max     Maximum delay in milliseconds.
     */
    public void sendMessageWithRandomDelay(byte[] message, int min, int max) {
        Executors.newSingleThreadScheduledExecutor().schedule(
                () -> sendMessage(message),
                Utils.randomBetween(min, max),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Sends message.
     *
     * @param message Message to send.
     */
    public void sendMessage(byte[] message) {
        DatagramPacket packet = new DatagramPacket(message, message.length, address, port);

        try {
            socket.send(packet);
        } catch (IOException ignored) {
        }
    }

    /**
     * Sends a message to a specified address and port.
     *
     * @param message Message to send.
     * @param sender  Sender address and port.
     */
    public void sendMessageTo(byte[] message, SocketAddress sender) {
        DatagramPacket packet = new DatagramPacket(message, message.length, sender);

        try {
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Listens to the socket for messages.
     */
    public void listen() {
        new Thread(() -> {
            while (true) {
                byte[] buffer = new byte[Server.MAX_HEADER_SIZE + Server.CHUNK_SIZE];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                try {
                    socket.receive(packet);
                    controller.processMessage(packet.getData(), packet.getLength(), packet.getSocketAddress());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
