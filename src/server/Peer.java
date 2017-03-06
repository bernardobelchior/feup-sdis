package server;

import java.net.MulticastSocket;

public class Peer {
    private final String protocolVersion;
    private final int serverId;
    private final MulticastSocket controlChannel;
    private final MulticastSocket backupChannel;
    private final MulticastSocket recoveryChannel;

    public Peer(String protocolVersion, int serverId, MulticastSocket controlChannel, MulticastSocket backupChannel, MulticastSocket recoveryChannel) {
        this.protocolVersion = protocolVersion;
        this.serverId = serverId;
        this.controlChannel = controlChannel;
        this.backupChannel = backupChannel;
        this.recoveryChannel = recoveryChannel;
    }
}

