package server.channel;

import server.Peer;

public class ControlChannel extends Channel {
    public ControlChannel(Peer peer, String address, String port) {
        super(peer, address, port);
    }
}
