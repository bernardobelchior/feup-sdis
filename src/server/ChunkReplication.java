package server;

class ChunkReplication {
    private final String fileId;
    private final int chunkNo;
    private final int replicationDifference;

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
