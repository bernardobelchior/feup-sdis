package server;

import server.messaging.Channel;
import server.messaging.MessageBuilder;
import server.protocol.BackupFile;
import server.protocol.RecoverFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static server.Server.*;
import static server.messaging.MessageParser.parseHeader;

public class Controller {

    private final Channel controlChannel;
    private final Channel backupChannel;
    private final Channel recoveryChannel;
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> fileChunkMap;
    private ConcurrentHashMap<String, Integer> desiredReplicationDegreesMap;
    private final ConcurrentHashMap<String, RecoverFile> ongoingRecoveries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ScheduledExecutorService> chunksToSend = new ConcurrentHashMap<>();
    private Set<String> storedChunks;

    public Controller(Channel controlChannel, Channel backupChannel, Channel recoveryChannel) {
        this.controlChannel = controlChannel;
        this.backupChannel = backupChannel;
        this.recoveryChannel = recoveryChannel;

        loadServerMetadata();

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

    public void processMessage(byte[] message, int size) {
        new Thread(() -> {

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message, 0, size);

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
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A get chunk header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processGetChunkMessage(headerFields[2], headerFields[3], headerFields[4]);
                        break;
                    case RESTORE_SUCCESS:
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A chunk restored header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processRestoredMessage(byteArrayInputStream, headerFields[3], headerFields[4]);
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

        try {
            Files.copy(byteArrayInputStream, createFile(fileId, chunkNo), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.err.println("Could not create file to save chunk. Discarding...");
            e.printStackTrace();
            return;
        }

        storedChunks.add(getChunkId(fileId, chunkNo));
        incrementReplicationDegree(fileId, chunkNo);

        controlChannel.sendMessageWithRandomDelay(
                MessageBuilder.createMessage(
                        Server.BACKUP_SUCCESS,
                        getProtocolVersion(),
                        Integer.toString(getServerId()),
                        fileId,
                        chunkNoStr),
                BACKUP_REPLY_MIN_DELAY,
                BACKUP_REPLY_MAX_DELAY);
    }

    private void processStoredMessage(String fileId, String chunkNoStr) throws InvalidHeaderException {
        Utils.checkFileIdValidity(fileId);
        int chunkNo = Utils.parseChunkNo(chunkNoStr);
        incrementReplicationDegree(fileId, chunkNo);
    }

    private void processGetChunkMessage(String senderId, String fileId, String chunkNoStr) throws InvalidHeaderException, IOException {
        if (Integer.parseInt(senderId) == getServerId()) // Same sender
            return;

        Utils.checkFileIdValidity(fileId);
        int chunkNo = Utils.parseChunkNo(chunkNoStr);

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        chunksToSend.put(getChunkId(fileId, chunkNo), executorService);

        byte[] chunkBody = Files.readAllBytes(createFile(fileId, chunkNo));

        executorService.schedule(() -> controlChannel.sendMessage(
                MessageBuilder.createMessage(
                        chunkBody,
                        Server.RESTORE_SUCCESS,
                        getProtocolVersion(),
                        Integer.toString(getServerId()),
                        fileId,
                        chunkNoStr))
                , Utils.randomBetween(RESTORE_REPLY_MIN_DELAY, RESTORE_REPLY_MAX_DELAY), TimeUnit.MILLISECONDS);
    }

    private void processRestoredMessage(ByteArrayInputStream byteArrayInputStream, String fileId, String chunkNoStr) throws InvalidHeaderException, IOException {
        Utils.checkFileIdValidity(fileId);
        int chunkNo = Utils.parseChunkNo(chunkNoStr);
        RecoverFile recover = ongoingRecoveries.get(fileId);
        ScheduledExecutorService chunkToSend = chunksToSend.get(getChunkId(fileId, chunkNo));

        /* If there is a chunk waiting to be sent, then delete it */
        if (chunkToSend != null) {
            chunkToSend.shutdown();
            chunksToSend.remove(getChunkId(fileId, chunkNo));
        }

        /* If the server is not currently trying to restore the file */
        if (recover == null)
            return;

        byte[] chunkBody = new byte[byteArrayInputStream.available()];
        byteArrayInputStream.read(chunkBody, 0, chunkBody.length);
        recover.putChunk(chunkNo, chunkBody);
    }


    private void incrementReplicationDegree(String fileId, int chunkNo) {
        fileChunkMap.putIfAbsent(fileId, new ConcurrentHashMap<>());

        ConcurrentHashMap<Integer, Integer> chunks = fileChunkMap.get(fileId);
        chunks.put(chunkNo, chunks.getOrDefault(chunkNo, 0) + 1);
    }

    private Path createFile(String fileId, int chunkNo) throws IOException {
        File file = new File(getServerId() + "/" + getChunkId(fileId, chunkNo));
        file.mkdirs();
        file.createNewFile();

        return file.toPath();
    }

    public void startFileBackup(BackupFile backupFile) {
        ConcurrentHashMap<Integer, Integer> chunksReplicationDegree = new ConcurrentHashMap<>();
        fileChunkMap.put(backupFile.getFileId(), chunksReplicationDegree);
        backupFile.start(this, chunksReplicationDegree);
    }

    public void startFileRecovery(RecoverFile recoverFile) throws FileNotFoundException {
        ongoingRecoveries.put(recoverFile.getFileId(), recoverFile);
        recoverFile.start(this);
    }

    public int getNumChunks(String fileId) {
        ConcurrentHashMap<Integer, Integer> chunkMap = fileChunkMap.get(fileId);

        return chunkMap == null ? 0 : chunkMap.size();
    }

    private void loadServerMetadata() {
        ObjectInputStream objectInputStream;

        try {
            objectInputStream = new ObjectInputStream(new FileInputStream("." + getServerId()));
        } catch (IOException e) {
            System.err.println("Could not open configuration file.");
            e.printStackTrace();
            return;
        }

        try {
            storedChunks = (Set<String>) objectInputStream.readObject();
            desiredReplicationDegreesMap = (ConcurrentHashMap<String, Integer>) objectInputStream.readObject();
            fileChunkMap = (ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>>) objectInputStream.readObject();
        } catch (IOException e) {
            System.err.println("Could not read from configuration file.");
            e.printStackTrace();
            storedChunks = Collections.synchronizedSet(new HashSet<String>());
            desiredReplicationDegreesMap = new ConcurrentHashMap<>();
            fileChunkMap = new ConcurrentHashMap<>();
        } catch (ClassNotFoundException e) {
            System.err.println("Unknown content in configuration file.");
            e.printStackTrace();
            storedChunks = Collections.synchronizedSet(new HashSet<String>());
            desiredReplicationDegreesMap = new ConcurrentHashMap<>();
            fileChunkMap = new ConcurrentHashMap<>();
        }
    }

    private void saveServerMetadata() {
        ObjectOutputStream objectOutputStream;
        try {
            objectOutputStream = new ObjectOutputStream(new FileOutputStream("." + getServerId()));
        } catch (IOException e) {
            System.err.println("Could not open configuration file.");
            e.printStackTrace();
            return;
        }

        try {
            objectOutputStream.writeObject(storedChunks);
            objectOutputStream.writeObject(desiredReplicationDegreesMap);
            objectOutputStream.writeObject(fileChunkMap);
        } catch (IOException e) {
            System.err.println("Could not write to configuration file.");
            e.printStackTrace();
        }
    }

    private String getChunkId(String fileId, int chunkNo) {
        return fileId + chunkNo;
    }
}
