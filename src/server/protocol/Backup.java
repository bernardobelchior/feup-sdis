package server.protocol;

import server.Controller;
import server.Utils;
import server.messaging.MessageBuilder;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.*;

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
    private final String filename;


    private final int desiredReplicationDegree;
    private final String fileId;
    private final File file;
    private final int MAX_BACKUP_THREADS = 10;
    private ConcurrentHashMap<Integer, Integer> chunksReplicationDegree;
    private final ExecutorService threadPool = Executors.newFixedThreadPool(MAX_BACKUP_THREADS);

    private Controller controller;

    public Backup(String filename, int desiredReplicationDegree) {
        this.filename = filename;
        this.desiredReplicationDegree = desiredReplicationDegree;
        file = new File(filename);
        fileId = generateFileId();
    }

    /**
     * Starts the file backup process.
     *
     * @param controller              Controller that handles message delivering.
     * @param chunksReplicationDegree Chunks current replication degree.
     * @return Returns true if the process is successful, returning false otherwise.
     */
    public boolean start(Controller controller, ConcurrentHashMap<Integer, Integer> chunksReplicationDegree) {
        this.controller = controller;
        this.chunksReplicationDegree = chunksReplicationDegree;

        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            System.out.println("Could not open file to backup.");
            return false;
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

        for (Future<Boolean> result : backedUpChunks) {
            try {
                if (!result.get(1, TimeUnit.MINUTES)) {
                    System.out.println("Could not backup a chunk. File backup failed.");
                    return false;
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                System.out.println("File backup took too long. Assuming it failed.");
                return false;
            }
        }

        System.out.println("File backup successful.");
        threadPool.shutdown();
        return true;
    }

    /**
     * Backs up a specific chunk.
     *
     * @param chunkNo Chunk number to be backed up.
     * @param chunk   Chunk content.
     * @param size    Chunk size.
     */
    private Future<Boolean> backupChunk(int chunkNo, byte[] chunk, int size) {
        return threadPool.submit(() -> {
            byte[] effectiveChunk = chunk;

               /* Add chunk to Incompleted Tasks HashMap */
            if(getProtocolVersion() > 1.0){
                System.out.println("GUARDAR NAS TASKS..." + chunkNo + " do file " + fileId);
                controller.getIncompletedTasks().putIfAbsent(getFileId(), new ConcurrentSkipListSet<>());
                controller.getIncompletedTasks().get(getFileId()).add(chunkNo);
            }

            if (size != CHUNK_SIZE)
                effectiveChunk = Arrays.copyOf(chunk, size);

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
            } while (chunksReplicationDegree.getOrDefault(chunkNo, 0) < desiredReplicationDegree
                    && attempts < MAX_BACKUP_ATTEMPTS);

            if (attempts >= MAX_BACKUP_ATTEMPTS) {
                System.out.println("Max backup attempts reached. Stopping backup process...");
                return false;
            }

            System.out.println("Backup of chunk number " + chunkNo + " successful with replication degree of at least " + chunksReplicationDegree.getOrDefault(chunkNo, 0) + ".");
            return true;
        });
    }

    /**
     * Generates File ID from its filename, last modified and permissions.
     *
     * @return File ID
     */

    private String generateFileId() {
        String bitString = filename + Long.toString(file.lastModified()) + Boolean.toString(file.canRead()) + Boolean.toString(file.canWrite()) + Boolean.toString(file.canExecute());
        return DatatypeConverter.printHexBinary(Utils.sha256(bitString));
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
     * Get file name.
     *
     * @return File name.
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Get desired replication degree.
     *
     * @return Desired replication degree.
     */
    public int getDesiredReplicationDegree() {
        return desiredReplicationDegree;
    }
}
