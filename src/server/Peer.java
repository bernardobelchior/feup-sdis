package server;

import javafx.scene.shape.Path;

import java.io.*;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;

import static server.Server.*;

public class Peer {
    private final String protocolVersion;
    private final int serverId;
    private HashMap<String, FileEntry> fileEntryMap = new HashMap<>();

    public Peer(String protocolVersion, int serverId) {
        this.protocolVersion = protocolVersion;
        this.serverId = serverId;
    }

    public void addFile(String fileId, int desiredReplicationDegree) {
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

