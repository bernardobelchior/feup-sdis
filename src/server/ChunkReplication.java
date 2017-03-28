package server;

/**
 * Created by mariajoaomirapaulo on 28/03/17.
 */
public class ChunkReplication {
    private String fileId;
    private int chunkNo;
    private int replicationDifference;

    public ChunkReplication(String fileId, int chunkNo, int replicationDifference) {
        this.fileId = fileId;
        this.chunkNo = chunkNo;
        this.replicationDifference = replicationDifference;
    }

    public String getFileId() {
        return fileId;
    }

    public int getChunkNo() {
        return chunkNo;
    }

    public int getReplicationDifference() {
        return replicationDifference;
    }
}
