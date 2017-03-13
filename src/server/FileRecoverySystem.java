package server;

import server.channel.ChannelManager;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import static server.Server.*;

public class FileRecoverySystem {
    private final String filename;
    private final String fileId;
    private ChannelManager channelManager;
    private int numChunks;
    private byte[][] receivedChunks;
    private int numReceivedChunks = 0;

    FileRecoverySystem(String filename, String fileId) {
        this.filename = filename;
        this.fileId = fileId;
    }


    public void start(ChannelManager channelManager) {
        this.channelManager = channelManager;
        numChunks = channelManager.getNumChunks(fileId);
        receivedChunks = new byte[numChunks][CHUNK_SIZE];

        for (int chunkNo = 0; chunkNo < numChunks; chunkNo++) { //TODO: Afterwards, do not ask for all chunks at once.
            getChunk(chunkNo);
        }
    }

    private void getChunk(int chunkNo) {
        new Thread(() -> {
            byte[] message = MessageBuilder.createMessage(Server.RESTORE_INIT, getProtocolVersion(), Integer.toString(getServerId()), fileId, Integer.toString(chunkNo));
            channelManager.sendToRecoveryChannel(message);
        }).start();
    }

    public synchronized void putChunk(int chunkNo, byte[] chunk) {
        receivedChunks[chunkNo] = chunk;
        numReceivedChunks++;

        if (numReceivedChunks == numChunks)
            recoverFile();
    }

    private void recoverFile() {
        new Thread(() -> {
            FileOutputStream fileOutputStream;
            try {
                fileOutputStream = new FileOutputStream(filename);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return;
            }

            for (int chunkNo = 0; chunkNo < receivedChunks.length; chunkNo++)
                try {
                    fileOutputStream.write(receivedChunks[chunkNo], chunkNo * CHUNK_SIZE, receivedChunks[chunkNo].length);
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }).start();
    }

    public String getFileId() {
        return fileId;
    }
}
