package server.protocol;

import server.Controller;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;

import static server.Server.CHUNK_SIZE;
import static server.Server.getProtocolVersion;

public class PartialBackup extends Backup {
    private final Set<Integer> chunks;

    public PartialBackup(String filename, int desiredReplicationDegree, Set<Integer> chunks) {
        super(filename, desiredReplicationDegree);
        this.chunks = chunks;
    }

    /**
     * Starts the file backup process.
     *
     * @param controller              Controller that handles message delivering.
     * @param chunksReplicationDegree Chunks current replication degree.
     * @return Returns true if the process is successful, returning false otherwise.
     */
    @Override
    public boolean start(Controller controller, ConcurrentHashMap<Integer, Integer> chunksReplicationDegree) {
        this.controller = controller;
        this.chunksReplicationDegree = chunksReplicationDegree;

        RandomAccessFile inputStream;
        try {
            inputStream = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            System.out.println("Could not open file to backup.");
            return false;
        }

        if (getProtocolVersion() > 1) {
            controller.getIncompleteTasks().put(getFileId(), new ConcurrentSkipListSet<>());
            for (Integer chunkNo : chunks)
                controller.addChunkToIncompleteTask(fileId, chunkNo);
            controller.saveIncompleteTasks();
        }

        ArrayList<Future<Boolean>> backedUpChunks = new ArrayList<>();
        try {
            int bytesRead;
            byte[] chunk = new byte[CHUNK_SIZE];

            for (Integer chunkNo : chunks) {
                inputStream.seek(chunkNo * CHUNK_SIZE);
                bytesRead = inputStream.read(chunk);
                backedUpChunks.add(backupChunk(chunkNo, chunk, bytesRead));
                chunk = new byte[CHUNK_SIZE];
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        waitForChunks(backedUpChunks);

        controller.removeFileFromIncompleteTask(fileId);
        controller.saveIncompleteTasks();
        System.out.println("File backup successful.");
        threadPool.shutdown();
        return true;
    }


}
