package server.protocol;

import server.Controller;
import server.Server;
import server.messaging.MessageBuilder;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static server.Server.*;
import static server.Utils.getFile;

public class RecoverFile {
    private static final int CHUNKS_PER_REQUEST = 10;
    private final String filename;
    private final String fileId;
    private Controller controller;
    private final ConcurrentHashMap<Integer, byte[]> receivedChunks;
    private int currentChunk = 0;
    private ExecutorService threadPool;
    private DatagramSocket recoverySocket;

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

        /*If protocol version is greater than 1.0, starts listening to the udp socket from messages*/
        if(getProtocolVersion()>1.0)
            startReadingFromSocket();

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
                    do {
                        threadPool.shutdownNow();
                    } while (!threadPool.isTerminated());
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
                            RESTORE_INIT,
                            Double.toString(getProtocolVersion()),
                            Integer.toString(getServerId()),
                            fileId,
                            Integer.toString(chunkNo));

                    do {
                        controller.sendToRecoveryChannel(message);

                        try {
                            Thread.sleep(RESTORE_TIMEOUT);
                        } catch (InterruptedException e) {
                            System.out.println("Request for chunk " + chunkNo + " timed out.");
                            return;
                        }
                    } while (!(receivedChunks.containsKey(chunkNo) || receivedAllChunks()));
                }));
    }

    /**
     * Stores the chunk in memory.
     *
     * @param chunkNo Chunk number
     * @param chunk   Chunk content
     */
    public void putChunk(int chunkNo, byte[] chunk) {
        receivedChunks.put(chunkNo, chunk);
        System.out.println("Successfully stored chunk number " + chunkNo + ".");
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

    /**
     * Checks if the chunk is already stored.
     *
     * @param chunkNo Chunk number to check.
     * @return True if the chunk is already stored.
     */
    public boolean hasChunk(int chunkNo) {
        return receivedChunks.containsKey(chunkNo);
    }

    /**
     * Starts reading from socket if protocol version is greater than 1.0
     */
    public void startReadingFromSocket(){
        try {
            recoverySocket = new DatagramSocket(2000 + getServerId());
            listenToSocket();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }
    /**
     * Listens to the socket for messages.
     */
    public void listenToSocket() {
        new Thread(() -> {
            while (true) {
                byte[] buffer = new byte[Server.MAX_HEADER_SIZE + Server.CHUNK_SIZE];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                try {
                    recoverySocket.receive(packet);
                    controller.processMessage(packet.getData(), packet.getLength(), packet.getAddress(), packet.getPort());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
