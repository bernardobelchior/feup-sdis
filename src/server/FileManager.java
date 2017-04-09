package server;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ConcurrentSkipListSet;

import static server.Utils.getChunkPath;

public class FileManager {
    private final String baseDir;
    private final String chunkDir;
    private final String restoredDir;
    private Controller controller;

    /**
     * Max storage size allowed, in bytes
     */
    private long maxStorageSize;

    /**
     * Storage size used to store chunks. Value is updated on backup and delete protocol
     */
    private volatile long usedSpace;

    FileManager(long maxStorageSize, String baseDir, String chunkDir, String restoredDir) {
        this.maxStorageSize = maxStorageSize;
        this.baseDir = baseDir;
        this.chunkDir = chunkDir;
        this.restoredDir = restoredDir;
    }

    /**
     * Gets the directory size, recursively
     *
     * @param path Path to check.
     * @return Returns directory size in bytes.
     * @throws IOException In case the directory could not be accessed.
     */
    public static long getDirectorySize(String path) throws IOException {
        long size = 0;

        File directory = new File(path);
        if (!directory.exists())
            return 0;

        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile())
                    size += Files.size(file.toPath());
            }
        }

        return size;
    }


    /**
     * Deletes a chunk
     *
     * @param fileId     File Id
     * @param chunkNo    Chunk number
     * @param controller
     */
    void deleteChunk(String fileId, Integer chunkNo, Controller controller) {
        System.out.println("Deleting chunkNo " + chunkNo + " from file" + fileId);

        /* Deletes chunk and then updates the used space. */
        try {
            long fileSize = Files.size(getChunkPath(fileId, chunkNo));
            Files.deleteIfExists(getChunkPath(fileId, chunkNo));
            decreaseUsedSpace(fileSize);
        } catch (IOException e) {
            System.out.println("Unsuccessful deletion of chunkNo " + chunkNo + " from fileId " + fileId + ".");
            return;
        }

        controller.localChunkDeleted(fileId, chunkNo);
        System.out.println("Successful deletion of chunkNo " + chunkNo + " from fileId " + fileId + ".");
    }

    /**
     * Checks if there is space available to store file with size {@param size}
     *
     * @param size Size of file to check
     * @return Returns true if the sum of the current space in use and the size is less than or equal to {@param maxStorageSize}.
     */
    synchronized boolean hasSpaceAvailable(long size) {
        return usedSpace + size <= maxStorageSize;
    }

    /**
     * Checks if there is space available.
     *
     * @return Returns true if the current space in use is less than or equal to {@param maxStorageSize}.
     */
    synchronized boolean hasSpaceAvailable() {
        return hasSpaceAvailable(0);
    }


    public synchronized boolean increaseUsedSpace(long size) {
        if (!hasSpaceAvailable(size))
            return false;

        usedSpace += size;
        System.out.println("Increased. New used space: " + usedSpace);
        return true;
    }

    public synchronized boolean decreaseUsedSpace(long size) {
        usedSpace -= size;
        System.out.println("Decreased. New used space: " + usedSpace);
        return true;
    }

    public void setController(Controller controller) {
        this.controller = controller;
    }

    public void setMaxStorageSize(long maxStorageSize) {
        this.maxStorageSize = maxStorageSize;
    }

    public long getUsedSpace() {
        return usedSpace;
    }

    public long getMaxStorageSize() {
        return maxStorageSize;
    }

    /**
     * Sets used space.
     *
     * @param usedSpace In bytes.
     */
    public void setUsedSpace(long usedSpace) {
        this.usedSpace = usedSpace;
    }

    /**
     * Deletes every chunk of the given file.
     *
     * @param fileId File id.
     */
    public boolean deleteFile(String fileId) {
        ConcurrentSkipListSet<Integer> chunksToDelete = controller.getStoredChunksOfFile(fileId);

        chunksToDelete.forEach(chunkNo -> deleteChunk(fileId, chunkNo, controller));

        if (chunksToDelete.isEmpty()) {
            controller.fileDeleted(fileId);
            return true;
        } else
            return false;
    }

    /**
     * Stores a chunk
     *
     * @param fileId               File Id
     * @param chunkNo              Chunk number
     * @param byteArrayInputStream byteArrayInputStream containing chunk body
     * @throws IOException In case the file could not be deleted.
     */
    boolean storeChunk(String fileId, int chunkNo, ByteArrayInputStream byteArrayInputStream) throws IOException {
        int chunkSize = byteArrayInputStream.available();

        if (!hasSpaceAvailable(chunkSize))
            return false;

        Path chunkPath = getChunkPath(fileId, chunkNo);
        if (chunkPath.toFile().exists())
            chunkPath.toFile().delete();

        Files.copy(byteArrayInputStream, chunkPath, StandardCopyOption.REPLACE_EXISTING);
        increaseUsedSpace(chunkSize);
        return true;
    }
}
