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

    public void addFile(String fileId, int chunkNo, int desiredReplicationDegree) { //TODO: How to store files in the filesystem and hashmap?
        fileEntryMap.put(fileId, new FileEntry(desiredReplicationDegree));
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public int getServerId() {
        return serverId;
    }

    public int getCurrentReplicationDegree(String fileId) {
        return fileEntryMap.get(fileId).getCurrentReplicationDegree();
    }

    public int getDesiredReplicationDegree(String fileId) {
        return fileEntryMap.get(fileId).getDesiredReplicationDegree();
    }

    public void incrementCurrentReplicationDegree(String fileId) {
        fileEntryMap.get(fileId).incrementCurrentReplicationDegree();
    }

}

