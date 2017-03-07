package server.channel;

import server.Peer;
import server.Server;

import java.util.Arrays;

public class BackupChannel extends Channel {
    /**
     * BackupChannel constructor.
     * @param peer Peer class that holds the information.
     * @param address Address string.
     * @param port Port number in String format.
     */
    public BackupChannel(Peer peer, String address, String port) {
        super(peer, address, port);
    }

    /**
     * Sends the specified chunk with number {@chunkNo} of file {@fileId}.
     * Also specifies a replication degree of {@replicationDegree}.
     * Should be started as a separated Thread.
     *
     * @param fileId            File Identifier
     * @param chunkNo           Chunk number in file
     * @param replicationDegree Minimum number of chunk replicas
     */
    public void sendChunk(String fileId, int chunkNo, int replicationDegree, byte[] chunk, int size) {
        if (size != Server.CHUNK_SIZE)
            chunk = Arrays.copyOf(chunk, size);

        do {
            byte[] message = createMessage(chunk,
                    Server.BACKUP_INIT,
                    peer.getProtocolVersion(),
                    Integer.toString(peer.getServerId()),
                    fileId,
                    Integer.toString(chunkNo),
                    Integer.toString(replicationDegree));

            sendMessage(message);

            try {
                Thread.sleep(Server.BACKUP_TIMEOUT);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (peer.getCurrentReplicationDegree(fileId) < peer.getDesiredReplicationDegree(fileId));
    }
}
