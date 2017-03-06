import java.net.InetAddress;

public class Peer {
    public static void main(String[] args) {

        int protocolVersion = Integer.parseInt(args[0]);

        int serverId = Integer.parseInt(args[1]);

        String serviceAccessPoint = args[2];

        InetAddress controlChannelAddr = Common.parseAddress(args[3]);
        int controlChannelPort = Integer.parseInt(args[4]);

        InetAddress dataChannelAddr = Common.parseAddress(args[5]);
        int dataChannelPort = Integer.parseInt(args[6]);

        InetAddress dataRecoveryChannelAddr = Common.parseAddress(args[7]);
        int dataRecoveryChannelPort = Integer.parseInt(args[8]);
    }
}
