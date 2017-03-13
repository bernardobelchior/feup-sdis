package server;

import server.messaging.Channel;
import server.messaging.MessageBuilder;
import server.protocol.BackupFile;
import server.protocol.RecoverFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ConcurrentHashMap;

import static server.messaging.MessageParser.parseHeader;
import static server.Server.*;

public class Controller {

    private final Channel controlChannel;
    private final Channel backupChannel;
    private final Channel recoveryChannel;
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> fileChunkMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> desiredReplicationDegreesMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RecoverFile> ongoingRecoveries = new ConcurrentHashMap<>();

    public Controller(Channel controlChannel, Channel backupChannel, Channel recoveryChannel) {
        this.controlChannel = controlChannel;
        this.backupChannel = backupChannel;
        this.recoveryChannel = recoveryChannel;

        this.controlChannel.setController(this);
        this.backupChannel.setController(this);
        this.recoveryChannel.setController(this);

        this.controlChannel.listen();
        this.backupChannel.listen();
        this.recoveryChannel.listen();
    }

    public void sendToBackupChannel(byte[] message) {
        backupChannel.sendMessage(message);
    }

    public void sendToRecoveryChannel(byte[] message) {
        recoveryChannel.sendMessage(message);
    }

    public void processMessage(byte[] message) {
        new Thread(() -> {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message);
            try {
                String[] headerFields = parseHeader(byteArrayInputStream).split(" ");

                if (headerFields.length < 3)
                    throw new InvalidHeaderException("A valid messaging header requires at least 3 fields.");

                switch (headerFields[0]) {
                    case BACKUP_INIT:
                        if (headerFields.length != 6)
                            throw new InvalidHeaderException("A chunk backup header must have exactly 6 fields. Received " + headerFields.length + ".");

                        processBackupMessage(byteArrayInputStream, headerFields[2], headerFields[3], headerFields[4], headerFields[5]);
                        break;
                    case BACKUP_SUCCESS:
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A chunk stored header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processStoredMessage(headerFields[3], headerFields[4]);
                        break;
                    case RESTORE_INIT:

                        break;
                    case RESTORE_SUCCESS:

                        break;
                    case DELETE_INIT:

                        break;
                    case RECLAIM_INIT: //TODO: Define implementation
                        break;
                    case RECLAIM_SUCESS:

                        break;
                    default:
                        throw new InvalidHeaderException("Unknown header messaging type " + headerFields[0]);
                }
            } catch (InvalidHeaderException | IOException e) {
                System.err.println(e.toString());
                e.printStackTrace();
            }
        }).start();
    }

    private void processBackupMessage(ByteArrayInputStream byteArrayInputStream, String senderId, String fileId, String chunkNoStr, String replicationDegreeStr) throws InvalidHeaderException, IOException {
        if (Integer.parseInt(senderId) == getServerId()) // Same sender
            return;

        Utils.checkFileIdValidity(fileId);
        int chunkNo = Utils.parseChunkNo(chunkNoStr);
        int desiredReplicationDegree = Utils.parseReplicationDegree(replicationDegreeStr);

        desiredReplicationDegreesMap.putIfAbsent(fileId, desiredReplicationDegree);

        if (fileChunkMap.getOrDefault(fileId, new ConcurrentHashMap<>()).getOrDefault(chunkNo, 0) > desiredReplicationDegree)
            return;

        Files.copy(byteArrayInputStream, getFilePath(fileId, chunkNo), StandardCopyOption.REPLACE_EXISTING);
        incrementReplicationDegree(fileId, chunkNo);

        controlChannel.sendMessage(
                MessageBuilder.createMessage(
                        Server.BACKUP_SUCCESS,
                        getProtocolVersion(),
                        Integer.toString(getServerId()),
                        fileId,
                        chunkNoStr));
    }

    private void processStoredMessage(String fileId, String chunkNoStr) throws InvalidHeaderException {
        Utils.checkFileIdValidity(fileId);
        int chunkNo = Utils.parseChunkNo(chunkNoStr);
        incrementReplicationDegree(fileId, chunkNo);
    }


    private void incrementReplicationDegree(String fileId, int chunkNo) {
        fileChunkMap.putIfAbsent(fileId, new ConcurrentHashMap<>());

        ConcurrentHashMap<Integer, Integer> chunks = fileChunkMap.get(fileId);
        chunks.put(chunkNo, chunks.getOrDefault(chunkNo, 0) + 1);
    }

    private Path getFilePath(String fileId, int chunkNo) {
        File file = new File(getServerId() + "/" + fileId + chunkNo);

        try {
            file.mkdirs();
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return file.toPath();
    }

    public void startFileBackup(BackupFile backupFile) {
        ConcurrentHashMap<Integer, Integer> chunksReplicationDegree = new ConcurrentHashMap<>();
        fileChunkMap.put(backupFile.getFileId(), chunksReplicationDegree);
        backupFile.start(this, chunksReplicationDegree);
    }

    public void startFileRecovery(RecoverFile recoverFile) {
        ongoingRecoveries.put(recoverFile.getFileId(), recoverFile);
        recoverFile.start(this);
    }

    public int getNumChunks(String fileId) {
        return fileChunkMap.getOrDefault(fileId, new ConcurrentHashMap<>()).size();
    }
}
