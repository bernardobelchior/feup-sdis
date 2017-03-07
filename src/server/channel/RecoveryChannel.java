package server.channel;

import server.Peer;

public class RecoveryChannel extends Channel{
    public RecoveryChannel(Peer peer, String address, String port) {
        super(peer, address, port);
    }
}
