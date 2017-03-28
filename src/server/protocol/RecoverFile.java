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

    private ExecutorService threadPool;

    public RecoverFile(String filename, String fileId) {
        this.filename = filename;
        this.fileId = fileId;
        receivedChunks = new ConcurrentHashMap<>();
    }

    /**
     * Starts the file recovery process.
     *
     * @param controller Controller that handles message sending.
     * @return Returns true if the file has been successfully restored, returning false otherwise.
     */
    public boolean start(Controller controller) {
        this.controller = controller;

        while (!receivedAllChunks()) {
            threadPool = Executors.newFixedThreadPool(CHUNKS_PER_REQUEST);
            System.out.println("Requesting chunks " + currentChunk + " to " + (currentChunk + CHUNKS_PER_REQUEST - 1) + " for fileId " + fileId + "...");

            /* Requests CHUNKS_PER_REQUEST chunks at a time where i is the chunk number. */
            for (int i = currentChunk; i < currentChunk + CHUNKS_PER_REQUEST; i++)
                requestChunk(i);

            currentChunk += CHUNKS_PER_REQUEST;

            threadPool.shutdown();
            try {
                if (!threadPool.awaitTermination(CHUNKS_PER_REQUEST / 2, TimeUnit.SECONDS)) {
                    System.out.println("Timeout waiting for chunks " + (currentChunk - CHUNKS_PER_REQUEST) + " to " + (currentChunk - 1) + ".");
                    return false;
                }
            } catch (InterruptedException e) {
                System.out.println("Error waiting for chunks to be received.");
                return false;
            }
        }

        System.out.println("All chunks received. Starting file reconstruction...");
        return recoverFile();
    }

    /**
     * Checks if all chunks have been received.
     *
     * @return True if all chunks have been received. False otherwise.
     */
    private boolean receivedAllChunks() {
        byte[] chunk = receivedChunks.get(receivedChunks.size() - 1); // Get last chunk
        return chunk != null && chunk.length < CHUNK_SIZE; // If the chunk exists and is less than CHUNK_SIZE
    }

    /**
     * Requests a chunk and waits either for its retrieval or for all chunks to be received.
     * Sends a GETCHUNK message periodically.
     *
     * @param chunkNo Chunk number to ask for.
     */
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
                    } while (receivedChunks.get(chunkNo) == null && !receivedAllChunks());
                }));
    }

    /**
     * Stores the chunk in memory.
     *
     * @param chunkNo Chunk number
     * @param chunk   Chunk content
     */
    public void putChunk(int chunkNo, byte[] chunk) {
        receivedChunks.putIfAbsent(chunkNo, chunk);
    }

    /**
     * Joins all chunks and writes them to a file.
     *
     * @return Returns true if the file has been successfully written to.
     */
    private boolean recoverFile() {
        FileOutputStream fileOutputStream;

        try {
            fileOutputStream = new FileOutputStream(getFile(RESTORED_DIR + filename));
        } catch (IOException e) {
            System.err.println("Error opening file for recovery.");
            return false;
        }

        for (int chunkNo = 0; chunkNo < receivedChunks.size(); chunkNo++) {
            try {
                fileOutputStream.write(receivedChunks.get(chunkNo), 0, receivedChunks.get(chunkNo).length);
            } catch (IOException e) {
                System.err.println("Error writing chunk " + chunkNo + " to file.");
                return false;
            }
        }

        return true;
    }

    /**
     * Gets file id.
     *
     * @return FileId
     */
    public String getFileId() {
        return fileId;
    }
}
