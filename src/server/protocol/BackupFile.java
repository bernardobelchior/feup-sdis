package server.protocol;

import server.Server;
import server.Utils;
import server.Controller;
import server.messaging.MessageBuilder;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import static server.Server.*;

public class BackupFile {
    private final String filename;
    private final int desiredReplicationDegree;
    private final String fileId;
    private final File file;
    private ConcurrentHashMap<Integer, Integer> chunksReplicationDegree;

    private Controller controller;

    public BackupFile(String filename, int desiredReplicationDegree) {
        this.filename = filename;
        this.desiredReplicationDegree = desiredReplicationDegree;
        file = new File(filename);
        fileId = generateFileId();
    }

    public void start(Controller controller, ConcurrentHashMap<Integer, Integer> chunksReplicationDegree) {
        this.controller = controller;
        this.chunksReplicationDegree = chunksReplicationDegree;

        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
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
    }

    private void backupChunk(int chunkNo, byte[] chunk, int size) {
        new Thread(() -> {
            byte[] effectiveChunk = chunk;

            if (size != CHUNK_SIZE)
                effectiveChunk = Arrays.copyOf(chunk, size);

            int attempts = 0;
            do {
                byte[] message = MessageBuilder.createMessage(effectiveChunk,
                        Server.BACKUP_INIT,
                        getProtocolVersion(),
                        Integer.toString(getServerId()),
                        fileId,
                        Integer.toString(chunkNo),
                        Integer.toString(desiredReplicationDegree));

                controller.sendToBackupChannel(message);

                try {
                    Thread.sleep(Server.BACKUP_TIMEOUT * 2 ^ attempts);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                attempts++;
            }
            while (chunksReplicationDegree.get(chunkNo) < desiredReplicationDegree && attempts < Server.MAX_BACKUP_ATTEMPTS);
        }).start();
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
}
