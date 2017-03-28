package server.protocol;

import server.Controller;
import server.Server;
import server.messaging.MessageBuilder;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static server.Server.*;
import static server.Utils.getFile;

public class RecoverFile {
    private final String filename;
    private final String fileId;
    private Controller controller;
    private ConcurrentHashMap<Integer, byte[]> receivedChunks;
    private int currentChunk = 0;
    private static final int CHUNKS_PER_REQUEST = 10;
    private static final int MAX_THREADS_PER_RECOVERY = 20;
    private static final int WAIT_FOR_CHUNKS = 500;

    private final ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREADS_PER_RECOVERY);

    public RecoverFile(String filename, String fileId) {
        this.filename = filename;
        this.fileId = fileId;
        receivedChunks = new ConcurrentHashMap<>();
    }

    public boolean start(Controller controller) {
        this.controller = controller;


        while (!receivedLastChunk()) {
            System.out.println("Requesting chunks " + currentChunk + " to " + (currentChunk + CHUNKS_PER_REQUEST - 1) + " for fileId " + fileId + "...");

            /* Requests CHUNKS_PER_REQUEST chunks at a time where i is the chunk number. */
            for (int i = currentChunk; i < currentChunk + CHUNKS_PER_REQUEST; i++)
                requestChunk(i);

            currentChunk += CHUNKS_PER_REQUEST;

            while (receivedChunks.size() < currentChunk && !receivedLastChunk()) {
                System.out.println("Waiting for other chunks...");
                try {
                    Thread.sleep(WAIT_FOR_CHUNKS);
                } catch (InterruptedException ignored) {
                }
            }
        }

        System.out.println("All chunks received. Starting file reconstruction...");
        recoverFile();

        threadPool.shutdown();

        try {
            threadPool.awaitTermination(20, TimeUnit.SECONDS);
            System.out.println("File successfully restored.");
        } catch (InterruptedException e) {
            System.out.println("File recovery failed.");
            return false;
        }

        return true;
    }

    private boolean receivedLastChunk() {
        byte[] chunk = receivedChunks.get(receivedChunks.size() - 1); // Get last chunk
        return chunk != null && chunk.length < CHUNK_SIZE; // If the chunk does not exist or is less than CHUNK_SIZE
    }

    private void requestChunk(int chunkNo) {
        threadPool.submit(
                new Thread(() -> {
                    byte[] message = MessageBuilder.createMessage(
                            Server.RESTORE_INIT,
                            Double.toString(getProtocolVersion()),
                            Integer.toString(getServerId()),
                            fileId,
                            Integer.toString(chunkNo));

                    do {
                        controller.sendToRecoveryChannel(message);

                        try {
                            Thread.sleep(RESTORE_TIMEOUT);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } while (receivedChunks.get(chunkNo) == null && !receivedLastChunk());
                }));
    }

    public void putChunk(int chunkNo, byte[] chunk) {
        receivedChunks.put(chunkNo, chunk);
    }

    private void recoverFile() {
        threadPool.submit(() -> {
            FileOutputStream fileOutputStream;
            try {
                fileOutputStream = new FileOutputStream(getFile(RESTORED_DIR + filename));
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            for (int chunkNo = 0; chunkNo < receivedChunks.size(); chunkNo++) {
                try {
                    fileOutputStream.write(receivedChunks.get(chunkNo), 0, receivedChunks.get(chunkNo).length);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public String getFileId() {
        return fileId;
    }
}
