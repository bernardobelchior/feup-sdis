package common;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Common {
    /**
     * Parses address and handles exceptions.
     *
     * @param addressStr Address to parse.
     * @return InetAddress corresponding to the {@addressStr}.
     */
    public static InetAddress parseAddress(String addressStr) {
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
