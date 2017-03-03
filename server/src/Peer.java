import java.net.InetAddress;

public class Peer {
    public static void main(String[] args) {
        int serverId = Integer.parseInt(args[0]);

        InetAddress controlChannelAddr = Common.parseAddress(args[1]);
        int controlChannelPort = Integer.parseInt(args[2]);

        InetAddress dataChannelAddr = Common.parseAddress(args[3]);
        int dataChannelPort = Integer.parseInt(args[4]);

        InetAddress dataRecoveryChannelAddr = Common.parseAddress(args[5]);
        int dataRecoveryChannelPort = Integer.parseInt(args[6]);
    }
}
