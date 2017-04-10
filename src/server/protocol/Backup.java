package server.protocol;

import server.Controller;
import server.messaging.MessageBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.*;

import static server.FileManager.generateFileId;
import static server.Server.*;

public class Backup {
    /* All time-related constants are in milliseconds */
    // Chunk Backup
    public static final String BACKUP_INIT = "PUTCHUNK";
    public static final String BACKUP_SUCCESS = "STORED";
    private static final int BACKUP_TIMEOUT = 1000;
    private static final int MAX_BACKUP_ATTEMPTS = 5;
    public static final int BACKUP_REPLY_MIN_DELAY = 0;
    public static final int BACKUP_REPLY_MAX_DELAY = 400;

    private final int desiredReplicationDegree;
    String fileId;
    File file;
    private final int MAX_BACKUP_THREADS = 10;
    final ExecutorService threadPool = Executors.newFixedThreadPool(MAX_BACKUP_THREADS);

    Controller controller;
    private String filename;

    public Backup(String filename, int desiredReplicationDegree) {
        this.desiredReplicationDegree = desiredReplicationDegree;
        this.filename = filename;
        file = new File(filename);
        fileId = generateFileId(filename, file);
    }

    /**
     * Starts the file backup process.
     *
     * @param controller Controller that handles message delivering.
     * @return Returns true if the process is successful, returning false otherwise.
     */
    public boolean start(Controller controller) {
        this.controller = controller;

        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            System.out.println("Could not open file to backup.");
            return false;
        }

        if (getProtocolVersion() > 1) {
            controller.getIncompleteTasks().put(getFileId(), new ConcurrentSkipListSet<>());
            int numChunks;
            try {
                numChunks = (int) Math.ceil((inputStream.available() + 1) / CHUNK_SIZE) + 1;
            } catch (IOException e) {
                System.out.println("Could not get file size. Backup unsuccessful.");
                return false;
            }

            for (int i = 0; i < numChunks; i++)
                controller.addChunkToIncompleteTask(fileId, i);
            controller.saveIncompleteTasks();
        }

        ArrayList<Future<Boolean>> backedUpChunks = new ArrayList<>();
        try {
            int chunkNo = 0;
            int bytesRead;
            int oldBytesRead = 0;
            byte[] chunk = new byte[CHUNK_SIZE];
            while ((bytesRead = inputStream.read(chunk)) != -1) {
                backedUpChunks.add(backupChunk(chunkNo, chunk, bytesRead));
                chunkNo++;
                chunk = new byte[CHUNK_SIZE];
                oldBytesRead = bytesRead;
            }

            if (oldBytesRead == CHUNK_SIZE)
                backedUpChunks.add(backupChunk(chunkNo, chunk, 0));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (!waitForChunks(backedUpChunks) && getProtocolVersion()>1.0) {
            controller.removeFileFromIncompleteTask(fileId);
            controller.saveIncompleteTasks();
            return false;
        }

        if(getProtocolVersion()>1.0) {
            controller.removeFileFromIncompleteTask(fileId);
            controller.saveIncompleteTasks();
        }
        System.out.println("File backup successful.");
        threadPool.shutdown();
        return true;
    }

    /**
     * Waits for chunks.
     *
     * @param backedUpChunks Backed up chunks.
     * @return
     */
    public static boolean waitForChunks(ArrayList<Future<Boolean>> backedUpChunks) {
        for (Future<Boolean> result : backedUpChunks) {
            try {
                if (!result.get(1, TimeUnit.MINUTES)) {
                    System.out.println("Could not backup a chunk. Backup failed.");
                    return false;
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                System.out.println("Backup took too long. Assuming it failed.");
                return false;
            }
        }

        return true;
    }

    /**
     * Backs up a specific chunk.
     *
     * @param controller Controller
     * @param fileId     File Id
     * @param chunkNo    Chunk number to be backed up.
     * @param chunk      Chunk content.
     * @param size       Chunk size.
     */
    public static boolean backupChunk(Controller controller, String fileId, int chunkNo, byte[] chunk, int size) {
        byte[] effectiveChunk = chunk;

        if (size != CHUNK_SIZE)
            effectiveChunk = Arrays.copyOf(chunk, size);

        int desiredReplicationDegree = controller.getDesiredReplicationDegree(fileId);

        byte[] message = MessageBuilder.createMessage(
                effectiveChunk,
                BACKUP_INIT,
                Double.toString(getProtocolVersion()),
                Integer.toString(getServerId()),
                fileId,
                Integer.toString(chunkNo),
                Integer.toString(desiredReplicationDegree));

        int attempts = 0;
        do {
            controller.sendToBackupChannel(message);

            try {
                Thread.sleep(BACKUP_TIMEOUT * 2 ^ attempts);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            attempts++;
        } while (controller.getCurrentReplicationDegree(fileId, chunkNo) < desiredReplicationDegree
                && attempts < MAX_BACKUP_ATTEMPTS);

        if (attempts >= MAX_BACKUP_ATTEMPTS) {
            System.out.println("Max backup attempts reached. Stopping backup process...");
            return false;
        }

        System.out.println("Backup of chunk number " + chunkNo + " successful with replication degree of at least " + controller.getCurrentReplicationDegree(fileId, chunkNo) + ".");
        return true;
    }

    /**
     * Backs up a specific chunk.
     *
     * @param chunkNo Chunk number to be backed up.
     * @param chunk   Chunk content.
     * @param size    Chunk size.
     */
    Future<Boolean> backupChunk(int chunkNo, byte[] chunk, int size) {
        return threadPool.submit(() -> backupChunk(controller, fileId, chunkNo, chunk, size));
    }

    /**
     * Get file id.
     *
     * @return file id.
     */
    public String getFileId() {
        return fileId;
    }

    /**
     * Get desired replication degree.
     *
     * @return Desired replication degree.
     */
    public int getDesiredReplicationDegree() {
        return desiredReplicationDegree;
    }

    public String getFilename() {
        return filename;
    }
}
