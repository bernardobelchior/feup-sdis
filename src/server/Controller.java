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
import static server.Utils.*;
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

        if (!loadServerMetadata()) {
            storedChunks = Collections.synchronizedSet(new HashSet<String>());
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

                        System.out.println("Received " + RESTORE_SUCCESS + " from " + headerFields[2] + " for fileId " + headerFields[3] + " and chunk number " + headerFields[4]);
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

    /**
     * Processes a backup message
     *
     * @param byteArrayInputStream InputStream containing everything after the end of the header
     * @param senderId             Sender Id
     * @param fileId               File Id
     * @param chunkNoStr           Chunk number as String
     * @param replicationDegreeStr Replication degree as String
     * @throws InvalidHeaderException In case of malformed header arguments.
     * @throws IOException            In case of error storing the received chunk.
     */
    private void processBackupMessage(ByteArrayInputStream byteArrayInputStream, String senderId, String fileId, String chunkNoStr, String replicationDegreeStr) throws InvalidHeaderException, IOException {
        if (Integer.parseInt(senderId) == getServerId()) // Same sender
            return;

        checkFileIdValidity(fileId);
        int chunkNo = parseChunkNo(chunkNoStr);
        int desiredReplicationDegree = parseReplicationDegree(replicationDegreeStr);

        desiredReplicationDegreesMap.putIfAbsent(fileId, desiredReplicationDegree);

        /* If the current replication degree is greater than or equal to the desired replication degree, then discard the message. */
        if (fileChunkMap.getOrDefault(fileId, new ConcurrentHashMap<>()).getOrDefault(chunkNo, 0) >= desiredReplicationDegree)
            return;

        try {
            Path chunkPath = getChunkPath(fileId, chunkNo);
            if (chunkPath.toFile().exists())
                chunkPath.toFile().delete();

            Files.copy(byteArrayInputStream, chunkPath, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("Stored chunk " + chunkNoStr + " of fileId " + fileId + ".");
        } catch (IOException e) {
            System.err.println("Could not create file to store chunk. Discarding...");
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
        checkFileIdValidity(fileId);
        int chunkNo = parseChunkNo(chunkNoStr);
        incrementReplicationDegree(fileId, chunkNo);
    }

    private void processGetChunkMessage(String senderId, String fileId, String chunkNoStr) throws InvalidHeaderException, IOException {
        if (Integer.parseInt(senderId) == getServerId()) // Same sender
            return;

        System.out.println("Requested chunk number " + chunkNoStr + " of fileId " + fileId + ".");

        /* If the requested chunk is not stored in our server, then do nothing. */
        if (!storedChunks.contains(fileId + chunkNoStr)) {
            System.out.println("But the chunk is not stored in this server.");
            return;
        }


        checkFileIdValidity(fileId);
        int chunkNo = parseChunkNo(chunkNoStr);

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        chunksToSend.put(getChunkId(fileId, chunkNo), executorService);

        byte[] chunkBody = Files.readAllBytes(getChunkPath(fileId, chunkNo));

        executorService.schedule(() -> {
            System.out.println("Retrieving chunk " + chunkNoStr + " of fileId " + fileId + "...");

            controlChannel.sendMessage(
                    MessageBuilder.createMessage(
                            chunkBody,
                            Server.RESTORE_SUCCESS,
                            getProtocolVersion(),
                            Integer.toString(getServerId()),
                            fileId,
                            chunkNoStr));
        }, randomBetween(RESTORE_REPLY_MIN_DELAY, RESTORE_REPLY_MAX_DELAY), TimeUnit.MILLISECONDS);
    }

    private void processRestoredMessage(ByteArrayInputStream byteArrayInputStream, String fileId, String chunkNoStr) throws InvalidHeaderException, IOException {
        checkFileIdValidity(fileId);
        int chunkNo = parseChunkNo(chunkNoStr);
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

    private boolean loadServerMetadata() {
        ObjectInputStream objectInputStream;

        try {
            objectInputStream = new ObjectInputStream(new FileInputStream("." + getServerId()));
        } catch (IOException e) {
            System.err.println("Could not open configuration file.");
            return false;
        }

        try {
            storedChunks = (Set<String>) objectInputStream.readObject();
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
