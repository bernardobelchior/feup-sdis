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
     */
    public static Path getChunkPath(String fileId, int chunkNo) {
        return getFile(CHUNK_DIR + fileId + chunkNo).toPath();
    }

    /**
     * Gets file with filepath and its parent directories and takes in account the BASE_DIR.
     *
     * @param filepath Path to file.
     * @return File
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
