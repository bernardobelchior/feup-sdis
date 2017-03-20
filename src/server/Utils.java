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
     * Parses replication degree received from a header field.
     *
     * @param replicationDegree Replication degree field.
     * @return Returns replicationDegree ranging from 0 to 9.
     * @throws InvalidHeaderException Thrown when the replicationDegree is not within range.
     */
    public static int parseReplicationDegree(String replicationDegree) throws InvalidHeaderException {
        if (replicationDegree.length() > 1)
            throw new InvalidHeaderException("Replication degree field cannot be larger than 1 character. Received: " + replicationDegree);

        try {
            return Integer.parseUnsignedInt(replicationDegree);
        } catch (NumberFormatException e) {
            throw new InvalidHeaderException("Invalid replication degree " + replicationDegree);
        }
    }

    /**
     * Parses chunk number from a header field.
     *
     * @param chunkNo Chunk number field.
     * @return Returns chunk number ranging from 0 to 999 999.
     * @throws InvalidHeaderException Thrown when the replicationDegree is not within range.
     */
    public static int parseChunkNo(String chunkNo) throws InvalidHeaderException {
        if (chunkNo.length() > 6)
            throw new InvalidHeaderException("Chunk number field cannot be larger than 6 characters. Received: " + chunkNo);

        try {
            return Integer.parseUnsignedInt(chunkNo);
        } catch (NumberFormatException e) {
            throw new InvalidHeaderException("Invalid chunk number " + chunkNo);
        }
    }

    /**
     * Checks fileId validity.
     *
     * @param fileId fileId
     * @throws InvalidHeaderException Thrown if fileId is not a 64-byte String.
     */
    public static void checkFileIdValidity(String fileId) throws InvalidHeaderException {
        if (fileId.length() != 64)
            throw new InvalidHeaderException("FileId has invalid length.");
    }

    /**
     * Parses the senderId.
     *
     * @param senderId senderId
     * @return Sender Id.
     * @throws InvalidHeaderException Thrown if the senderId is not a valid number.
     */
    public static int parseSenderId(String senderId) throws InvalidHeaderException {
        try {
            return Integer.parseUnsignedInt(senderId);
        } catch (NumberFormatException e) {
            throw new InvalidHeaderException("Invalid Sender Id " + senderId);
        }
    }

    /**
     * Returns a random int between min and max.
     * @param min Lower bound
     * @param max Upper bound
     * @return Random number in the specified interval.
     */
    public static int randomBetween(int min, int max) {
        return new Random().nextInt(max - min) + min;
    }
}
