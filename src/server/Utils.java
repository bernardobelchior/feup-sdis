package server;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import static server.Server.BASE_DIR;
import static server.Server.CHUNK_DIR;

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
            return Integer.parseInt(replicationDegree);
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
     * Validates the server version.
     *
     * @param serverVersion Server version string
     * @throws InvalidHeaderException In case the header is malformed
     */
    public static double parseServerVersion(String serverVersion) throws InvalidHeaderException {
        try {
            return Double.parseDouble(serverVersion);
        } catch (NumberFormatException e) {
            throw new InvalidHeaderException("Invalid server version.");
        }
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
     * Gets Path to file and creates it.
     *
     * @param fileId  File Id
     * @param chunkNo Chunk number
     * @return Path to file.
     * @throws IOException
     */
    public static Path getChunkPath(String fileId, int chunkNo) throws IOException {
        return getFile(CHUNK_DIR + fileId + chunkNo).toPath();
    }

    /**
     * Gets file with filepath and its parent directories and takes in account the BASE_DIR.
     *
     * @param filepath Path to file.
     * @return File
     * @throws IOException
     */
    public static File getFile(String filepath) {
        return new File(BASE_DIR + filepath);
    }

    public static long getDirectorySize(String path) throws IOException {
        long size = 0;

        File directory = new File(path);
        if (!directory.exists())
            return 0;

        File[] files = directory.listFiles();
        for (File file : files) {
            if (file.isFile())
                size += Files.size(file.toPath());
        }

        return size;
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
