package server;

import java.util.HashMap;

public class Peer {
    private final String protocolVersion;
    private final int serverId;
    private HashMap<String, FileEntry> fileEntryMap = new HashMap<>();

    public Peer(String protocolVersion, int serverId) {
        this.protocolVersion = protocolVersion;
        this.serverId = serverId;
    }

    public void addFile(String fileId, int chunkNo, int desiredReplicationDegree) {
        fileEntryMap.put(createKey(fileId, chunkNo), new FileEntry(desiredReplicationDegree));
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public int getServerId() {
        return serverId;
    }

    public int getCurrentReplicationDegree(String fileId, int chunkNo) {
        return fileEntryMap.get(createKey(fileId, chunkNo)).getCurrentReplicationDegree();
    }

    public int getDesiredReplicationDegree(String fileId, int chunkNo) {
        return fileEntryMap.get(createKey(fileId, chunkNo)).getDesiredReplicationDegree();
    }

    public void incrementCurrentReplicationDegree(String fileId, int chunkNo) {
        fileEntryMap.get(createKey(fileId, chunkNo)).incrementCurrentReplicationDegree();
    }

    public void decrementCurrentReplicationDegree(String fileId, int chunkNo) {
        fileEntryMap.get(createKey(fileId, chunkNo)).decrementCurrentReplicationDegree();
    }

    private String createKey(String fileId, int chunkNo) {
        return fileId + chunkNo;
    }
}

