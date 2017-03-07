package server.channel;

import server.Peer;
import server.Server;

import java.util.Arrays;

public class BackupChannel extends Channel{
    public BackupChannel(Peer peer, String address, String port) {
        super(peer, address, port);
    }

    /**
     * Sends the specified chunk with number {@chunkNo} of file {@fileId}.
     * Also specifies a replication degree of {@replicationDegree}.
     *
     * @param fileId            File Identifier
     * @param chunkNo           Chunk number in file
     * @param replicationDegree Minimum number of chunk replicas
     */
    public void sendChunk(String fileId, int chunkNo, int replicationDegree, byte[] chunk) {
        String message = Server.BACKUP_INIT;

        message += " " + peer.getProtocolVersion()
                + " " + Integer.toString(peer.getServerId())
                + " " + fileId
                + " " + chunkNo
                + " " + Integer.toString(replicationDegree)
                + " " + Server.CRLF + Server.CRLF
                + chunk;

        System.out.println(message);
    }

    /**
     * Sends the specified chunk with number {@chunkNo} of file {@fileId}.
     * Also specifies a replication degree of {@replicationDegree}.
     *
     * @param fileId            File Identifier
     * @param chunkNo           Chunk number in file
     * @param replicationDegree Minimum number of chunk replicas
     */
    public void sendChunk(String fileId, int chunkNo, int replicationDegree, byte[] chunk, int size) {
        if (size != Server.CHUNK_SIZE)
            chunk = Arrays.copyOf(chunk, size);

        sendChunk(fileId, chunkNo, replicationDegree, chunk);
    }

}