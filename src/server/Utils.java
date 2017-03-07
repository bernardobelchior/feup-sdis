package server;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {
    // Chunk Backup
    public static final String BACKUP_INIT = "PUTCHUNK";
    public static final String BACKUP_SUCCESS = "STORED";

    // Chunk Restore
    public static final String RESTORE_INIT = "GETCHUNK";
    public static final String RESTORE_SUCCESS = "CHUNK";

    // File Deletion
    public static final String DELETE_INIT = "DELETE";

    // Space Reclaiming
    public static final String RECLAIM_INIT = "REMOVED"; //TODO: Define implementation
    public static final String RECLAIM_SUCESS = "REMOVED";

    public static final String CRLF = "" + (char) 0xD + (char) +0xA;

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
