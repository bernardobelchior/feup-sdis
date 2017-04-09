package server;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static server.Server.getProtocolVersion;
import static server.Server.getServerId;

public class FileManager {
    /**
     * Directory in which chunks will be stored.
     */
    private static final String CHUNK_DIR = "Chunks";
    /**
     * Directory in which restored files will be saved.
     */
    private static final String RESTORED_DIR = "RestoredFiles";

    private final String baseDir;
    private final String chunkDir;
    private final String restoredDir;
    private final String metadataFileName;
    private Controller controller;

    /**
     * Max storage size allowed, in bytes
     */
    private long maxStorageSize;

    /**
     * Storage size used to store chunks. Value is updated on backup and delete protocol
     */
    private volatile long usedSpace;

    FileManager(long maxStorageSize, String baseDir) {
        this.maxStorageSize = maxStorageSize;
        this.baseDir = baseDir + "/";
        this.chunkDir = this.baseDir + CHUNK_DIR;
        this.restoredDir = this.baseDir + RESTORED_DIR;
        metadataFileName = "." + getServerId();
        usedSpace = 0;

        initializeDirs();
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
     * Gets Path to file and creates it.
     *
     * @param fileId  File Id
     * @param chunkNo Chunk number
     * @return Path to file.
     */
    public Path getChunkPath(String fileId, int chunkNo) {
        return getFile(chunkDir + fileId + chunkNo).toPath();
    }

    /**
     * Gets file with filepath and its parent directories and takes in account the BASE_DIR.
     *
     * @param filepath Path to file.
     * @return File
     */
    public File getFile(String filepath) {
        return new File(baseDir + filepath);
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
        return true;
    }

    public synchronized boolean decreaseUsedSpace(long size) {
        usedSpace -= size;
        return true;
    }

    public void setController(Controller controller) {
        this.controller = controller;
    }

    public synchronized void setMaxStorageSize(long maxStorageSize) {
        this.maxStorageSize = maxStorageSize;
    }

    public synchronized long getUsedSpace() {
        return usedSpace;
    }

    public synchronized long getMaxStorageSize() {
        return maxStorageSize;
    }

    /**
     * Sets used space.
     *
     * @param usedSpace In bytes.
     */
    public synchronized void setUsedSpace(long usedSpace) {
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

    public File getRestoredFilePath(String filename) {
        return getFile(restoredDir + filename);
    }

    /**
     * Saves server metadata
     */
    void saveServerMetadata() {
        ObjectOutputStream objectOutputStream;
        try {
            objectOutputStream = new ObjectOutputStream(new FileOutputStream(getFile(metadataFileName)));
        } catch (IOException e) {
            System.err.println("Could not open configuration file for writing.");
            return;
        }

        try {
            objectOutputStream.writeLong(getMaxStorageSize());
            objectOutputStream.writeObject(controller.getStoredChunks());
            objectOutputStream.writeObject(controller.getDesiredReplicationDegrees());
            objectOutputStream.writeObject(controller.getChunkCurrentReplicationDegree());
            objectOutputStream.writeObject(controller.getBackedUpFiles());
            System.out.println("Server metadata successfully saved.");
        } catch (IOException e) {
            System.err.println("Could not write to configuration file.");
        }
    }

    /**
     * Loads server metadata.
     *
     * @return True if the metadata was correctly loaded.
     */
    @SuppressWarnings("unchecked")
    boolean loadServerMetadata() {
        ObjectInputStream objectInputStream;

        try {
            objectInputStream = new ObjectInputStream(new FileInputStream(getFile(metadataFileName)));
        } catch (IOException e) {
            System.err.println("Could not open configuration file for reading.");
            return false;
        }

        long maxStorageSize;
        try {
            maxStorageSize = objectInputStream.readLong();
            controller.setStoredChunks((ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>>) objectInputStream.readObject());
            controller.setDesiredReplicationDegrees((ConcurrentHashMap<String, Integer>) objectInputStream.readObject());
            controller.setChunkCurrentReplicationDegree((ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>>) objectInputStream.readObject());
            controller.setBackedUpFiles((ConcurrentHashMap<String, String>) objectInputStream.readObject());
        } catch (IOException e) {
            System.err.println("Could not read from configuration file.");
            return false;
        } catch (ClassNotFoundException e) {
            System.err.println("Unknown content in configuration file.");
            return false;
        }

        long usedSpace;
        try {
            usedSpace = getDirectorySize(chunkDir);
        } catch (IOException e) {
            System.err.println("Could not get server actual size.");
            return false;
        }

        if (getProtocolVersion() > 1)
            controller.leaseStoredFiles();

        setMaxStorageSize(maxStorageSize);
        setUsedSpace(usedSpace);
        System.out.println("Server metadata loaded successfully.");
        return true;
    }

    public byte[] loadChunk(String fileId, int chunkNo) throws IOException {
        return Files.readAllBytes(getChunkPath(fileId, chunkNo));
    }

    void initializeDirs() {
        new File(baseDir).mkdir();
        new File(chunkDir).mkdir();
        new File(restoredDir).mkdir();
    }
}
