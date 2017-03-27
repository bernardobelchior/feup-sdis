package server;

import server.messaging.Channel;
import server.messaging.MessageBuilder;
import server.protocol.BackupFile;
import server.protocol.DeleteFile;
import server.protocol.RecoverFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static server.Server.*;
import static server.Utils.*;
import static server.messaging.MessageParser.parseHeader;

public class Controller {

    private final Channel controlChannel;
    private final Channel backupChannel;
    private final Channel recoveryChannel;

    /*Concurrent HashMap with fileId and Concurrent HashMap with file's chunkNo and respective replication degree*/
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> fileChunkMap;

    /*Concurrent HashMap with fileId and respective desired Replication Degree*/
    private ConcurrentHashMap<String, Integer> desiredReplicationDegreesMap;

    /*Concurrent HashMap with fileId and respective RecoverFile object*/
    private final ConcurrentHashMap<String, RecoverFile> ongoingRecoveries = new ConcurrentHashMap<>();

    /*Concurrent HashMap with String (fileId + chunkNo) and respective ExecutorService, responsible for schedule processRestoredMessage function*/
    private final ConcurrentHashMap<String, ScheduledExecutorService> chunksToSend = new ConcurrentHashMap<>();

    /*Server's stored chunks*/
    private ConcurrentHashMap<String, Set<Integer>> storedChunks;

    /* Max storage size allowed */
    private int storageMaxSize;


    public Controller(Channel controlChannel, Channel backupChannel, Channel recoveryChannel) {
        this.controlChannel = controlChannel;
        this.backupChannel = backupChannel;
        this.recoveryChannel = recoveryChannel;

        if (!loadServerMetadata()) {
            storedChunks = new ConcurrentHashMap<>();
            desiredReplicationDegreesMap = new ConcurrentHashMap<>();
            fileChunkMap = new ConcurrentHashMap<>();
        }

        this.controlChannel.setController(this);
        this.backupChannel.setController(this);
        this.recoveryChannel.setController(this);

        this.controlChannel.listen();
        this.backupChannel.listen();
        this.recoveryChannel.listen();
    }

    /**
     * Sends message to Backup Channel
     *
     * @param message Message to send.
     */
    public void sendToBackupChannel(byte[] message) {
        backupChannel.sendMessage(message);
    }

    /**
     * Sends message to Recovery Channel
     *
     * @param message Message to send
     */
    public void sendToRecoveryChannel(byte[] message) {
        recoveryChannel.sendMessage(message);
    }

    /**
     * Sends Delete message to Control Channel
     *
     * @param message
     */
    public void sendDeleteMessage(byte[] message) {
        controlChannel.sendMessage(message);
    }

    /**
     * Processes the message received asynchronously.
     *
     * @param message Message to process.
     * @param size    Message size
     */
    public void processMessage(byte[] message, int size) {
        new Thread(() -> {

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message, 0, size);

            try {
                String[] headerFields = parseHeader(byteArrayInputStream).split(" ");
                double serverVersion = parseServerVersion(headerFields[1]);
                int senderId = parseSenderId(headerFields[2]);
                checkFileIdValidity(headerFields[3]);

                int chunkNo = 0;
                int replicationDegree = 0;
                if (headerFields.length > 4) {
                    chunkNo = parseChunkNo(headerFields[4]);
                    if (headerFields.length > 5)
                        replicationDegree = parseReplicationDegree(headerFields[5]);
                }

                if (headerFields.length < 3)
                    throw new InvalidHeaderException("A valid messaging header requires at least 3 fields.");

                switch (headerFields[0]) {
                    case BACKUP_INIT:
                        if (headerFields.length != 6)
                            throw new InvalidHeaderException("A chunk backup header must have exactly 6 fields. Received " + headerFields.length + ".");

                        processBackupMessage(byteArrayInputStream, senderId, headerFields[3], chunkNo, replicationDegree);
                        break;
                    case BACKUP_SUCCESS:
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A chunk stored header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processStoredMessage(headerFields[3], chunkNo);
                        break;
                    case RESTORE_INIT:
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A get chunk header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processGetChunkMessage(senderId, headerFields[3], chunkNo);
                        break;
                    case RESTORE_SUCCESS:
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A chunk restored header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processRestoredMessage(byteArrayInputStream, senderId, headerFields[3], chunkNo);
                        break;
                    case DELETE_INIT:
                        if (headerFields.length != 4)
                            throw new InvalidHeaderException("A file delete header must have exactly 4 fields. Received " + headerFields.length + ".");

                        processDeleteMessage(senderId, headerFields[3]);
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

    /**
     * Processes a backup message
     *
     * @param byteArrayInputStream     InputStream containing everything after the end of the header
     * @param senderId                 Sender Id
     * @param fileId                   File Id
     * @param chunkNo                  Chunk number
     * @param desiredReplicationDegree Replication degree
     * @throws InvalidHeaderException In case of malformed header arguments.
     * @throws IOException            In case of error storing the received chunk.
     */
    private void processBackupMessage(ByteArrayInputStream byteArrayInputStream, int senderId, String fileId, int chunkNo, int desiredReplicationDegree) throws InvalidHeaderException, IOException {
        if (senderId == getServerId()) // Same sender
            return;

        checkFileIdValidity(fileId);

        desiredReplicationDegreesMap.putIfAbsent(fileId, desiredReplicationDegree);

        /* If the current replication degree is greater than or equal to the desired replication degree, then discard the message. */
        if (fileChunkMap.getOrDefault(fileId, new ConcurrentHashMap<>()).getOrDefault(chunkNo, 0) >= desiredReplicationDegree)
            return;

        try {
            Path chunkPath = getChunkPath(fileId, chunkNo);
            if (chunkPath.toFile().exists())
                chunkPath.toFile().delete();

            Files.copy(byteArrayInputStream, chunkPath, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("Stored chunk " + chunkNo + " of fileId " + fileId + ".");
        } catch (IOException e) {
            System.err.println("Could not create file to store chunk. Discarding...");
            e.printStackTrace();
            return;
        }

        Set<Integer> chunkNoSet;

        /* If server already has a set for that fileId, adds the new chunkNo, other way creates a new Set */
        if (storedChunks.containsKey(fileId))
            chunkNoSet = storedChunks.get(fileId);
        else chunkNoSet = new HashSet<>();

        chunkNoSet.add(chunkNo);
        storedChunks.put(fileId, chunkNoSet);

        incrementReplicationDegree(fileId, chunkNo);

        controlChannel.sendMessageWithRandomDelay(
                MessageBuilder.createMessage(
                        Server.BACKUP_SUCCESS,
                        Double.toString(getProtocolVersion()),
                        Integer.toString(getServerId()),
                        fileId,
                        Integer.toString(chunkNo)),
                BACKUP_REPLY_MIN_DELAY,
                BACKUP_REPLY_MAX_DELAY);
    }

    private void processStoredMessage(String fileId, int chunkNo) throws InvalidHeaderException {
        incrementReplicationDegree(fileId, chunkNo);
    }

    private void processGetChunkMessage(int senderId, String fileId, int chunkNo) throws InvalidHeaderException, IOException {
        if (senderId == getServerId()) // Same sender
            return;

        System.out.println("Requested chunk number " + chunkNo + " of fileId " + fileId + ".");

        /* If the requested chunk is not stored in our server, then do nothing. */
        if (!storedChunks.get(fileId).contains(chunkNo)) {
            System.out.println("But the chunk is not stored in this server.");
            return;
        }

        checkFileIdValidity(fileId);

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        chunksToSend.put(getChunkId(fileId, chunkNo), executorService);

        byte[] chunkBody = Files.readAllBytes(getChunkPath(fileId, chunkNo));

        executorService.schedule(() -> {
            System.out.println("Retrieving chunk " + chunkNo + " of fileId " + fileId + "...");

            controlChannel.sendMessage(
                    MessageBuilder.createMessage(
                            chunkBody,
                            Server.RESTORE_SUCCESS,
                            Double.toString(getProtocolVersion()),
                            Integer.toString(getServerId()),
                            fileId,
                            "" + chunkNo));
        }, randomBetween(RESTORE_REPLY_MIN_DELAY, RESTORE_REPLY_MAX_DELAY), TimeUnit.MILLISECONDS);
    }

    private void processRestoredMessage(ByteArrayInputStream byteArrayInputStream, int serverId, String fileId, int chunkNo) throws InvalidHeaderException, IOException {
        if (serverId == getServerId()) //Same sender
            return;

        System.out.println("Received " + RESTORE_SUCCESS + " from " + serverId + " for fileId " + fileId + " and chunk number " + chunkNo);

        RecoverFile recover = ongoingRecoveries.get(fileId);
        ScheduledExecutorService chunkToSend = chunksToSend.get(getChunkId(fileId, chunkNo));

        /* If there is a chunk waiting to be sent, then delete it */
        if (chunkToSend != null) {
            System.out.println("Received " + RESTORE_SUCCESS + " for fileId " + fileId + " chunk number " + chunkNo + ". Discarding...");
            chunkToSend.shutdownNow();
            chunksToSend.remove(getChunkId(fileId, chunkNo));
        }

        /* If the server is not currently trying to restore the file */
        if (recover == null)
            return;

        byte[] chunkBody = new byte[byteArrayInputStream.available()];
        byteArrayInputStream.read(chunkBody, 0, chunkBody.length);
        recover.putChunk(chunkNo, chunkBody);
    }

    private void processDeleteMessage(int serverId, String fileId) throws InvalidHeaderException, IOException {
        if (serverId == getServerId())  //Same sender
            return;

        if (!storedChunks.containsKey(fileId))
            return;

        System.out.println("Received " + DELETE_INIT + " from " + serverId + " for file " + fileId);

        for (Integer chunkNo : storedChunks.get(fileId))
            Files.deleteIfExists(getChunkPath(fileId, chunkNo));

        storedChunks.remove(fileId);
        desiredReplicationDegreesMap.remove(fileId);
        fileChunkMap.remove(fileId);

        System.out.println("Successfully deleted file " + fileId);
    }


    private void incrementReplicationDegree(String fileId, int chunkNo) {
        fileChunkMap.putIfAbsent(fileId, new ConcurrentHashMap<>());

        ConcurrentHashMap<Integer, Integer> chunks = fileChunkMap.get(fileId);
        chunks.put(chunkNo, chunks.getOrDefault(chunkNo, 0) + 1);
    }

    public void startFileBackup(BackupFile backupFile) {
        ConcurrentHashMap<Integer, Integer> chunksReplicationDegree = new ConcurrentHashMap<>();
        fileChunkMap.put(backupFile.getFileId(), chunksReplicationDegree);
        desiredReplicationDegreesMap.putIfAbsent(backupFile.getFileId(), backupFile.getDesiredReplicationDegree());

        backupFile.start(this, chunksReplicationDegree);
    }

    public void startFileRecovery(RecoverFile recoverFile) throws FileNotFoundException {
        /* If the fileId does not exist in the network */
        if (desiredReplicationDegreesMap.get(recoverFile.getFileId()) == null) {
            System.err.println("File not found in the network.");
            return;
        }

        ongoingRecoveries.put(recoverFile.getFileId(), recoverFile);
        recoverFile.start(this);
    }

    public void startFileDelete(DeleteFile deleteFile) {
        /*If the fileId does not exist in the network*/
        if (desiredReplicationDegreesMap.get(deleteFile.getFileId()) == null) {
            System.out.println("File not found in the network.");
            return;
        }

        deleteFile.start(this);
    }

    private boolean loadServerMetadata() {
        ObjectInputStream objectInputStream;

        try {
            objectInputStream = new ObjectInputStream(new FileInputStream("." + getServerId()));
        } catch (IOException e) {
            System.err.println("Could not open configuration file.");
            return false;
        }

        try {
            storedChunks = (ConcurrentHashMap<String, Set<Integer>>) objectInputStream.readObject();
            desiredReplicationDegreesMap = (ConcurrentHashMap<String, Integer>) objectInputStream.readObject();
            fileChunkMap = (ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>>) objectInputStream.readObject();
        } catch (IOException e) {
            System.err.println("Could not read from configuration file.");
            return false;

        } catch (ClassNotFoundException e) {
            System.err.println("Unknown content in configuration file.");
            return false;
        }

        return true;
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
