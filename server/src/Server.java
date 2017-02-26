import java.net.InetAddress;
import java.net.UnknownHostException;

public class Server {
    public static void main(String[] args) {

        int serverId = Integer.parseInt(args[0]);

        InetAddress controlChannelAddr = Server.parseAddress(args[1]);
        int controlChannelPort = Integer.parseInt(args[2]);


        InetAddress dataChannelAddr = Server.parseAddress(args[3]);
        int dataChannelPort = Integer.parseInt(args[4]);

        InetAddress dataRecoveryChannelAddr = Server.parseAddress(args[5]);
        int dataRecoveryChannelPort = Integer.parseInt(args[6]);
    }

    private static InetAddress parseAddress(String addressStr) {
        InetAddress address = null;

        try {
            address = InetAddress.getByName(addressStr);
        } catch (UnknownHostException e) {
            System.err.println("Error parsing address: " + addressStr);
            e.printStackTrace();
        }

        return address;
    }
}
