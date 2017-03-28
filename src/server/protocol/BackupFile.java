package server.protocol;

import server.Controller;
import server.Server;
import server.Utils;
import server.messaging.MessageBuilder;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.*;

import static server.Server.*;

public class BackupFile {
    private final String filename;


    private final int desiredReplicationDegree;
    private final String fileId;
    private final File file;
    private ConcurrentHashMap<Integer, Integer> chunksReplicationDegree;

    private final int MAX_BACKUP_THREADS = 10;

    private ExecutorService threadPool = Executors.newFixedThreadPool(MAX_BACKUP_THREADS);

    private Controller controller;

    public BackupFile(String filename, int desiredReplicationDegree) {
        this.filename = filename;
        this.desiredReplicationDegree = desiredReplicationDegree;
        file = new File(filename);
        fileId = generateFileId();
    }

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

        try {
            int chunkNo = 0;
            int bytesRead;
            int oldBytesRead = 0;
            byte[] chunk = new byte[CHUNK_SIZE];
            while ((bytesRead = inputStream.read(chunk)) != -1) {
                backupChunk(chunkNo, chunk, bytesRead);
                chunkNo++;
                chunk = new byte[CHUNK_SIZE];
                oldBytesRead = bytesRead;
            }

            if (oldBytesRead == CHUNK_SIZE)
                backupChunk(chunkNo, chunk, 0);

        } catch (IOException e) {
            e.printStackTrace();
        }

        threadPool.shutdown();
        try {
            if (threadPool.awaitTermination(1, TimeUnit.MINUTES)) {
                System.out.println("File backup successful.");
                return true;
            } else {
                System.out.println("File backup took too long. Assuming it failed.");
                return false;
            }
        } catch (InterruptedException e) {
            return false;
        }

    }

    private void backupChunk(int chunkNo, byte[] chunk, int size) {
        threadPool.submit(
                new Thread(() -> {
                    byte[] effectiveChunk = chunk;

                    if (size != CHUNK_SIZE)
                        effectiveChunk = Arrays.copyOf(chunk, size);

                    byte[] message = MessageBuilder.createMessage(
                            effectiveChunk,
                            Server.BACKUP_INIT,
                            Double.toString(getProtocolVersion()),
                            Integer.toString(getServerId()),
                            fileId,
                            Integer.toString(chunkNo),
                            Integer.toString(desiredReplicationDegree));

                    int attempts = 0;
                    do {
                        controller.sendToBackupChannel(message);

                        try {
                            Thread.sleep(Server.BACKUP_TIMEOUT * 2 ^ attempts);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        attempts++;
                    }
                    while (chunksReplicationDegree.getOrDefault(chunkNo, 0) < desiredReplicationDegree && attempts < Server.MAX_BACKUP_ATTEMPTS);

                    if (attempts >= Server.MAX_BACKUP_ATTEMPTS) {
                        System.out.println("Max backup attempts reached. Stopping backup process...");
                    } else
                        System.out.println("Backup of chunk number " + chunkNo + " successful with replication degree of at least " + chunksReplicationDegree.getOrDefault(chunkNo, 0) + ".");
                }));
        System.out.println("Thread number " + chunkNo + " submitted.");
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

    public String getFileId() {
        return fileId;
    }

    public String getFilename() {
        return filename;
    }

    public int getDesiredReplicationDegree() {
        return desiredReplicationDegree;
    }
}
