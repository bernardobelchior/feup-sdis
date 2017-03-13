package server.channel;

import server.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ConcurrentHashMap;

import static server.Server.*;

public class ChannelManager {

    private final Channel controlChannel;
    private final Channel backupChannel;
    private final Channel recoveryChannel;
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> fileChunkMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> desiredReplicationDegreesMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, FileRecoverySystem> ongoingRecoveries = new ConcurrentHashMap<>();

    public ChannelManager(Channel controlChannel, Channel backupChannel, Channel recoveryChannel) {
        this.controlChannel = controlChannel;
        this.backupChannel = backupChannel;
        this.recoveryChannel = recoveryChannel;

        this.controlChannel.setManager(this);
        this.backupChannel.setManager(this);
        this.recoveryChannel.setManager(this);

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

            String header = "";
            byte byteRead;

            /*
            * The header specification does not allow the byte '0xD' to be inside it.
            * As such, when we find a CR, we have either found the start of the two CRLFs of a well-formed header
            * Or the body of a malformed header.
            */
            while ((byteRead = (byte) byteArrayInputStream.read()) != CR) // if byte == CR
                header += Character.toString((char) byteRead);

            try {
                if ((byte) byteArrayInputStream.read() != LF // byte == LF
                        || (byte) byteArrayInputStream.read() != CR // byte == CR
                        || (byte) byteArrayInputStream.read() != LF) { // byte == LF

                    throw new InvalidHeaderException("Found improper header. Discarding message...");
                }

                String[] headerFields = header.split(" ");

                if (headerFields.length < 3)
                    throw new InvalidHeaderException("A valid message header requires at least 3 fields.");

                processHeader(headerFields, byteArrayInputStream);
            } catch (InvalidHeaderException | IOException e) {
                System.err.println(e.toString());
                e.printStackTrace();
                return;
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

    /**
     * Processes the header. Receives a {@link ByteArrayInputStream} for future parsing, if needed.
     * Header format:
     * Index: 0          1          2        3         4            5
     * <MessageType> <Version> <SenderId> <FileId> <ChunkNo> <ReplicationDeg> <CRLF>
     *
     * @param headerFields         Array of header fields.
     * @param byteArrayInputStream {@link ByteArrayInputStream} that contains the rest of the message.
     */
    private void processHeader(String[] headerFields, ByteArrayInputStream byteArrayInputStream) throws InvalidHeaderException, IOException {
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
                throw new InvalidHeaderException("Unknown header message type " + headerFields[0]);
        }
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

    public void startFileBackup(FileBackupSystem fileBackupSystem) {
        ConcurrentHashMap<Integer, Integer> chunksReplicationDegree = new ConcurrentHashMap<>();
        fileChunkMap.put(fileBackupSystem.getFileId(), chunksReplicationDegree);
        fileBackupSystem.start(this, chunksReplicationDegree);
    }

    public void startFileRecovery(FileRecoverySystem fileRecoverySystem) {
        ongoingRecoveries.put(fileRecoverySystem.getFileId(), fileRecoverySystem);
        fileRecoverySystem.start(this);
    }

    public int getNumChunks(String fileId) {
        return fileChunkMap.getOrDefault(fileId, new ConcurrentHashMap<>()).size();
    }
}
