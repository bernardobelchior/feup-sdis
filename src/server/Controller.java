package server;


import javafx.util.Pair;
import server.messaging.Channel;
import server.messaging.MessageBuilder;
import server.protocol.BackupFile;
import server.protocol.DeleteFile;
import server.protocol.RecoverFile;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.*;
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
    /*Concurrent HashMap with fileId and respective RecoverFile object*/
    private final ConcurrentHashMap<String, RecoverFile> ongoingRecoveries = new ConcurrentHashMap<>();
    /*Concurrent HashMap with String (fileId + chunkNo) and respective ExecutorService, responsible for schedule processRestoredMessage function*/
    private final ConcurrentHashMap<String, ScheduledExecutorService> chunksToSend = new ConcurrentHashMap<>();
    /*Concurrent HashMap with fileId and Concurrent HashMap with file's chunkNo and respective replication degree*/
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> fileChunkMap;
    /*Concurrent HashMap with fileId and respective desired Replication Degree*/
    private ConcurrentHashMap<String, Integer> desiredReplicationDegreesMap;
    private final ConcurrentHashMap<String, ScheduledExecutorService> chunksToBackUp = new ConcurrentHashMap<>();
    /*Server's stored chunks*/
    private ConcurrentHashMap<String, Set<Integer>> storedChunks;

    /* Max storage size allowed, in bytes */
    private int maxStorageSize = (int) (Math.pow(1000, 2) * 8); // 8 Megabytes

    /* Maps FileId to Filename */
    private final ConcurrentHashMap<String, String> backedUpFiles = new ConcurrentHashMap<>();


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
    public void sendToControlChannel(byte[] message) {
        controlChannel.sendMessage(message);
    }

    /**
     * Processes the message received asynchronously.
     *
     * @param message Message to process.
     * @param size    Message size
     */
    public void processMessage(byte[] message, int size, InetAddress senderAddr, int senderPort) {
        new Thread(() -> {

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message, 0, size);

            try {
                String[] headerFields = parseHeader(byteArrayInputStream).split(" ");
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
                        System.out.println("Received message " + headerFields[0] + " from " + senderAddr + ":" + senderPort);
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A get chunk header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processGetChunkMessage(senderId, headerFields[3], chunkNo, senderAddr, senderPort);
                        break;
                    case RESTORE_SUCCESS:
                        System.out.println("Received message " + headerFields[0] + " from " + senderAddr + ":" + senderPort);

                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A chunk restored header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processRestoredMessage(byteArrayInputStream, senderId, headerFields[3], chunkNo);
                        break;
                    case DELETE_INIT:
                        if (headerFields.length != 4)
                            throw new InvalidHeaderException("A file delete header must have exactly 4 fields. Received " + headerFields.length + ".");

                        processDeleteMessage(senderId, headerFields[3]);
                        break;
                    case RECLAIM_SUCESS:
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A space reclaiming header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processReclaimMessage(senderId, headerFields[3], chunkNo);
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
    private void processBackupMessage(ByteArrayInputStream byteArrayInputStream, int senderId, String fileId, int chunkNo, int desiredReplicationDegree) throws InvalidHeaderException {
        if (senderId == getServerId()) // Same sender
            return;

        checkFileIdValidity(fileId);

        desiredReplicationDegreesMap.putIfAbsent(fileId, desiredReplicationDegree);

        ScheduledExecutorService chunkToSend = chunksToBackUp.get(getChunkId(fileId, chunkNo));

        System.out.println("1");
        /* If there is a PUTCHUNK message waiting to be sent,
         * then discard it because we have already an identical one. */
        if (chunkToSend != null) {
            chunkToSend.shutdownNow();
            chunksToBackUp.remove(getChunkId(fileId, chunkNo));
        }

        System.out.println("2");
        /* If we have already stored the chunk we just received, then just do nothing. */
        if (storedChunks.containsKey(fileId) && storedChunks.get(fileId).contains(chunkNo))
            return;

        System.out.println("3");
        /* If the current replication degree is greater than or equal to the desired replication degree, then discard the message. */
        if (fileChunkMap.getOrDefault(fileId, new ConcurrentHashMap<>()).getOrDefault(chunkNo, 0) >= desiredReplicationDegree)
            return;

        System.out.println("4");
        if (!hasAvailableSpace(fileId, BASE_DIR + CHUNK_DIR, byteArrayInputStream.available())) {
            System.out.println("Not enough space to backup chunk " + chunkNo + ".");
            return;
        }

        System.out.println("5");
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

        /* If server already has a set for that fileId, adds the new chunkNo, otherwise creates a new Set */
        Set<Integer> chunkNoSet = storedChunks.getOrDefault(fileId, Collections.synchronizedSet(new HashSet<>()));

        chunkNoSet.add(chunkNo);
        storedChunks.putIfAbsent(fileId, chunkNoSet);

        incrementReplicationDegree(fileId, chunkNo);

        controlChannel.sendMessageWithRandomDelay(
                MessageBuilder.createMessage(
                        BACKUP_SUCCESS,
                        Double.toString(getProtocolVersion()),
                        Integer.toString(getServerId()),
                        fileId,
                        Integer.toString(chunkNo)),
                BACKUP_REPLY_MIN_DELAY,
                BACKUP_REPLY_MAX_DELAY);
    }

    private void processStoredMessage(String fileId, int chunkNo) {
        incrementReplicationDegree(fileId, chunkNo);
    }

    private void processGetChunkMessage(int senderId, String fileId, int chunkNo, InetAddress senderAddr, int senderPort) throws InvalidHeaderException, IOException {
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

        byte[] message = MessageBuilder.createMessage(
                chunkBody,
                RESTORE_SUCCESS,
                Double.toString(getProtocolVersion()),
                Integer.toString(getServerId()),
                fileId,
                "" + chunkNo);

        if (getProtocolVersion() > 1) {
            System.out.println("Retrieving chunk " + chunkNo + " of fileId " + fileId + "...");
            controlChannel.sendMessageTo(message, senderAddr, senderPort);
        } else {
            executorService.schedule(() -> {
                System.out.println("Retrieving chunk " + chunkNo + " of fileId " + fileId + "...");
                controlChannel.sendMessage(message);
            }, randomBetween(RESTORE_REPLY_MIN_DELAY, RESTORE_REPLY_MAX_DELAY), TimeUnit.MILLISECONDS);
        }
    }

    private void processRestoredMessage(ByteArrayInputStream byteArrayInputStream, int serverId, String fileId, int chunkNo) {
        if (serverId == getServerId()) //Same sender
            return;

        System.out.println("Received " + RESTORE_SUCCESS + " from " + serverId + " for fileId " + fileId + " and chunk number " + chunkNo);

        ScheduledExecutorService chunkToSend = chunksToSend.get(getChunkId(fileId, chunkNo));

        /* If there is a chunk waiting to be sent, then delete it because someone sent it first. */
        if (chunkToSend != null) {
            System.out.println("Received " + RESTORE_SUCCESS + " for fileId " + fileId + " chunk number " + chunkNo + ". Discarding...");
            chunkToSend.shutdownNow();
            chunksToSend.remove(getChunkId(fileId, chunkNo));
        }

        RecoverFile recover = ongoingRecoveries.get(fileId);

        /* If this server is not currently trying to restore the file, then do nothing. */
        if (recover == null)
            return;

        if (!recover.hasChunk(chunkNo)) {
            byte[] chunkBody = new byte[byteArrayInputStream.available()];
            byteArrayInputStream.read(chunkBody, 0, chunkBody.length);
            recover.putChunk(chunkNo, chunkBody);
        } else {
            System.out.println("Chunk number " + chunkNo + " is already stored. Discarding...");
        }
    }

    private void processDeleteMessage(int serverId, String fileId) throws IOException {
        desiredReplicationDegreesMap.remove(fileId);
        fileChunkMap.remove(fileId);
        backedUpFiles.remove(fileId);

        if (!storedChunks.containsKey(fileId))
            return;

        System.out.println("Received " + DELETE_INIT + " from " + serverId + " for file " + fileId);

        for (Integer chunkNo : storedChunks.get(fileId))
            Files.deleteIfExists(getChunkPath(fileId, chunkNo));

        storedChunks.remove(fileId);

        System.out.println("Successfully deleted file " + fileId);
    }

    private void processReclaimMessage(int serverId, String fileId, int chunkNo) throws IOException {
        decrementReplicationDegree(fileId, chunkNo);

        if (serverId == getServerId())
            return;

        System.out.println("Received Reclaim Message... " + chunkNo + " from server " + serverId);

        if (!storedChunks.containsKey(fileId) || !storedChunks.get(fileId).contains(chunkNo))
            return;

        /* Chunk Replication Degree is greater than the desired Replication Degree for that fileId */
        if (fileChunkMap.get(fileId).get(chunkNo) > desiredReplicationDegreesMap.get(fileId))
            return;

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        chunksToBackUp.put(getChunkId(fileId, chunkNo), executorService);

        byte[] chunkBody = Files.readAllBytes(getChunkPath(fileId, chunkNo));

        System.out.println("Ready to start Backup ... ");

        executorService.schedule(() -> controlChannel.sendMessage(
                MessageBuilder.createMessage(
                        chunkBody,
                        Server.BACKUP_INIT,
                        Double.toString(getProtocolVersion()),
                        Integer.toString(getServerId()),
                        fileId,
                        Integer.toString(chunkNo),
                        Integer.toString(desiredReplicationDegreesMap.get(fileId)))),
                randomBetween(RECLAIM_REPLY_MIN_DELAY, RECLAIM_REPLY_MAX_DELAY),
                TimeUnit.MILLISECONDS);
    }


    private void incrementReplicationDegree(String fileId, int chunkNo) {
        fileChunkMap.putIfAbsent(fileId, new ConcurrentHashMap<>());

        ConcurrentHashMap<Integer, Integer> chunks = fileChunkMap.get(fileId);
        chunks.put(chunkNo, chunks.getOrDefault(chunkNo, 0) + 1);
    }

    private void decrementReplicationDegree(String fileId, int chunkNo) {
        ConcurrentHashMap<Integer, Integer> chunks = fileChunkMap.get(fileId);
        chunks.put(chunkNo, chunks.getOrDefault(chunkNo, 0) - 1);
    }

    public void deleteChunk(String fileId, Integer chunkNo) throws IOException {

        System.out.println("Deleting chunkNo " + chunkNo + " from file" + fileId);

         /* Decrement Replication Degree */
        ConcurrentHashMap<Integer, Integer> chunks = fileChunkMap.get(fileId);
        chunks.put(chunkNo, chunks.getOrDefault(chunkNo, 0) - 1);

        /* Delete Chunk */
        Files.deleteIfExists(getChunkPath(fileId, chunkNo));
        storedChunks.get(fileId).remove(chunkNo);

        System.out.println("Successfully deleted chunkNo " + chunkNo);
    }

    public boolean startFileBackup(BackupFile backupFile) {
        ConcurrentHashMap<Integer, Integer> chunksReplicationDegree = new ConcurrentHashMap<>();
        fileChunkMap.put(backupFile.getFileId(), chunksReplicationDegree);
        desiredReplicationDegreesMap.putIfAbsent(backupFile.getFileId(), backupFile.getDesiredReplicationDegree());
        backedUpFiles.put(backupFile.getFileId(), backupFile.getFilename());

        boolean ret = backupFile.start(this, chunksReplicationDegree);
        saveServerMetadata();
        return ret;
    }

    public boolean startFileRecovery(RecoverFile recoverFile) {
        /* If the fileId does not exist in the network */
        if (desiredReplicationDegreesMap.get(recoverFile.getFileId()) == null) {
            System.err.println("File not found in the network.");
            return false;
        }

        ongoingRecoveries.put(recoverFile.getFileId(), recoverFile);

        boolean ret = recoverFile.start(this);
        saveServerMetadata();
        return ret;
    }

    public void startFileDelete(DeleteFile deleteFile) {
        /*If the fileId does not exist in the network*/
        if (desiredReplicationDegreesMap.get(deleteFile.getFileId()) == null) {
            System.out.println("File not found in the network.");
            return;
        }

        deleteFile.start(this);
        saveServerMetadata();
    }


    public boolean startReclaim(int storageSize) throws IOException {
        if (storageSize > maxStorageSize) {
            maxStorageSize = storageSize;
            return true;
        } else {
            maxStorageSize = storageSize;
            return reclaimSpace();
        }
    }

    public boolean reclaimSpace() throws IOException {
        PriorityQueue<ChunkReplication> leastNecessaryChunks = new PriorityQueue<>(new ChunkReplicationComparator());

        int diff;

        Set<Map.Entry<String, Set<Integer>>> files = storedChunks.entrySet();

        for (Map.Entry<String, Set<Integer>> file : files) {
            String fileId = file.getKey();
            for (Integer chunk : file.getValue()) {
                diff = fileChunkMap.get(fileId).get(chunk) - desiredReplicationDegreesMap.get(fileId);
                leastNecessaryChunks.add(new ChunkReplication(fileId, chunk, diff));
            }
        }

        if(leastNecessaryChunks.isEmpty())
            return true;

        /* Chunk with Replication Degree greater than Desired Replication Degree */
        while (getDirectorySize(BASE_DIR + CHUNK_DIR) > maxStorageSize) {
            ChunkReplication leastNecessaryChunk = leastNecessaryChunks.poll();
            deleteChunk(leastNecessaryChunk.getFileId(), leastNecessaryChunk.getChunkNo());
            controlChannel.sendMessage(
                    MessageBuilder.createMessage(Server.RECLAIM_SUCESS,
                            Double.toString(getProtocolVersion()),
                            Integer.toString(getServerId()),
                            leastNecessaryChunk.getFileId(),
                            Integer.toString(leastNecessaryChunk.getChunkNo())));
        }

        //TODO: Wait for replication degree to be acceptable.
        return true;
    }

    /**
     * Loads server metadata.
     *
     * @return True if the metadata was correctly loaded.
     */
    @SuppressWarnings("unchecked")
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

    public String getState() {
        StringBuilder sb = new StringBuilder();

        sb.append("Backed up files");
        for (Map.Entry<String, String> file : backedUpFiles.entrySet()) {
            sb.append("\n\tPathname: ");
            sb.append(file.getValue());

            sb.append("\n\tFile Id: ");
            sb.append(file.getKey());

            sb.append("\n\tDesired Replication Degree: ");
            sb.append(desiredReplicationDegreesMap.get(file.getKey()));

            if (fileChunkMap.containsKey(file.getKey()))
                for (Map.Entry<Integer, Integer> chunk : fileChunkMap.get(file.getKey()).entrySet()) {
                    sb.append("\n\t\tChunk No: ");
                    sb.append(chunk.getKey());
                    sb.append("\n\t\tReplication Degree: ");
                    sb.append(chunk.getValue());
                }
        }

        sb.append("\n\n");
        sb.append("Stored chunks");

        for (Map.Entry<String, Set<Integer>> fileChunk : storedChunks.entrySet()) {
            if (fileChunk.getValue().isEmpty())
                continue;

            sb.append("\n\tFile Id: ");
            sb.append(fileChunk.getKey());

            for (Integer chunkNo : fileChunk.getValue()) {
                sb.append("\n\t\tChunk No: ");
                sb.append(chunkNo);

                sb.append("\n\t\t\tSize: ");
                try {
                    sb.append(Files.size(getChunkPath(fileChunk.getKey(), chunkNo)) / 1000);
                    sb.append(" KB");
                } catch (IOException e) {
                    sb.append("Could not open chunk.");
                    e.printStackTrace();
                }

                sb.append("\n\t\t\tReplication degree: ");
                sb.append(fileChunkMap.get(fileChunk.getKey()).getOrDefault(chunkNo, 0));
            }
        }

        sb.append("\n\n\nMaximum Storage Size: ");
        sb.append(maxStorageSize / 1000);
        sb.append(" KB");

        sb.append("\nCurrent Space Used: ");
        sb.append("TODO "); //TODO
        sb.append("KB");

        return sb.toString();
    }

    public boolean hasAvailableSpace(String fileId, String path, int chunkSize) {
        try {
            if (getDirectorySize(path) + (long) chunkSize < maxStorageSize)
                return true;
        } catch (IOException e) {
            System.out.println("Unable to calculate directory size ... ");
            return false;
        }

        System.out.println("Server needs more storage size to backup file with fileId " + fileId);
        return false;
    }


    public int getMaxStorageSize() {
        return maxStorageSize;
    }


}
