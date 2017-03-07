package server;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {

    public static byte[] sha256(String input) {
        MessageDigest digest = null;

        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        byte[] hash = new byte[0];
        try {
            hash = digest.digest(input.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return hash;
    }
}
