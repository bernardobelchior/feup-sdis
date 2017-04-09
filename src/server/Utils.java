package server;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 * Utilities class.
 */
public class Utils {
    /**
     * Hashes input with the SHA-256 algorithm.
     *
     * @param input Input
     * @return Hashed input.
     */
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

    /**
     * Returns a random int between min and max.
     *
     * @param min Lower bound
     * @param max Upper bound
     * @return Random number in the specified interval.
     */
    public static int randomBetween(int min, int max) {
        return new Random().nextInt(max - min) + min;
    }

    /**
     * Gets a chunkId from fileId and ChunkNo.
     *
     * @param fileId  FileId
     * @param chunkNo Chunk number.
     * @return Returns chunk Id.
     */
    public static String getChunkId(String fileId, int chunkNo) {
        return fileId + chunkNo;
    }
}
