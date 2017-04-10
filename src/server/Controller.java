package server;

import server.messaging.Channel;
import server.protocol.Backup;
import server.protocol.PartialBackup;
import server.protocol.Recover;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.*;

import static server.Server.*;
import static server.Utils.getChunkId;
import static server.Utils.randomBetween;
import static server.messaging.MessageBuilder.createMessage;
import static server.messaging.MessageParser.*;
import static server.protocol.Backup.BACKUP_INIT;

public class Controller {

    /**
     * Control channel
     */
    private final Channel controlChannel;
    /**
     * Backup channel
     */
    private final Channel backupChannel;
    /**
     * Recovery channel
     */
    private final Channel recoveryChannel;

    /**
     * Concurrent HashMap with fileId and respective Recover object
     */
    private final ConcurrentHashMap<String, Recover> ongoingRecoveries = new ConcurrentHashMap<>();

    /**
     * Concurrent HashMap with String (fileId + chunkNo) and respective ExecutorService, responsible for schedule processRestoredMessage function
     */
    private final ConcurrentHashMap<String, ScheduledExecutorService> chunksToSend = new ConcurrentHashMap<>();

    /**
     * Saves every peer that stores the specified chunk.
     */
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConcurrentSkipListSet<Integer>>> peersStoringChunk;

    /**
     * Concurrent HashMap with fileId and respective desired Replication Degree
     */
    private ConcurrentHashMap<String, Integer> desiredReplicationDegrees;

    /**
     * Concurrent HashMap with fileId and respective {@link ExecutorService} for chunks prepared to be backed up
     */
    private final ConcurrentHashMap<String, ScheduledExecutorService> chunksToBackUp = new ConcurrentHashMap<>();

    /**
     * Server's stored chunks
     */
    private ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> storedChunks;

    /**
     * Maps FileId to Filename
     */
    private ConcurrentHashMap<String, String> backedUpFiles;

    /**
     * Lease Timer, used for delete enhancement.
     */
    private final LeaseManager leaseManager;

    /**
     * File manager.
     */
    FileManager fileManager;

    /**
     * Socket used for restore enhancement.
     */
    private Socket recoverySocket;

    /**
     * Concurrent HashMap with fileId and respective chunk No waiting to be stored
     */
    private ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> incompleteTasks;

    public Controller(Channel controlChannel, Channel backupChannel, Channel recoveryChannel, FileManager fileManager) {
        this.controlChannel = controlChannel;
        this.backupChannel = backupChannel;
        this.recoveryChannel = recoveryChannel;
        this.fileManager = fileManager;
        fileManager.setController(this);
        leaseManager = new LeaseManager(this, controlChannel);

        if (!fileManager.loadServerMetadata())
            newServerMetadata();

        this.controlChannel.setController(this);
        this.backupChannel.setController(this);
        this.recoveryChannel.setController(this);

        this.controlChannel.listen();
        this.backupChannel.listen();
        this.recoveryChannel.listen();

        if (getProtocolVersion() > 1.0) {
            if (fileManager.loadIncompleteTasks())
                finishIncompleteTasks();
            else
                incompleteTasks = new ConcurrentHashMap<>();
        }

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(fileManager::saveServerMetadata, 5, 5, TimeUnit.SECONDS);
    }

    private void finishIncompleteTasks() {
        incompleteTasks.forEachEntry(10, file -> {
            String fileId = file.getKey();
            String filename = backedUpFiles.get(fileId);
            int replicationDegree = desiredReplicationDegrees.get(fileId);
            startFileBackup(new PartialBackup(filename, replicationDegree, file.getValue()));
        });
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
     * @param message Message to send
     */
    private void sendToControlChannel(byte[] message) {
        controlChannel.sendMessage(message);
    }

    /**
     * Processes the message received asynchronously.
     *
     * @param message Message to process.
     * @param size    Message size
     */
    public void processMessage(byte[] message, int size, InetAddress senderAddr) {
        new Thread(() -> {

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message, 0, size);

            try {
                String[] headerFields = parseHeader(byteArrayInputStream).trim().split(" ");
                double protocolVersion = parseProtocolVersion(headerFields[1]);
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

                        if (getProtocolVersion() > 1.0 & protocolVersion > 1.0)
                            try {
                                Thread.sleep(randomBetween(Backup.BACKUP_REPLY_MIN_DELAY, Backup.BACKUP_REPLY_MAX_DELAY));
                            } catch (InterruptedException ignored) {
                            }

                        processBackupMessage(byteArrayInputStream, protocolVersion, senderId, headerFields[3], chunkNo, replicationDegree);
                        break;
                    case Backup.BACKUP_SUCCESS:
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A chunk stored header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processStoredMessage(senderId, headerFields[3], chunkNo);
                        break;
                    case Recover.RESTORE_INIT:
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A get chunk header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processGetChunkMessage(protocolVersion, senderId, headerFields[3], chunkNo, senderAddr);
                        break;
                    case Recover.RESTORE_SUCCESS:
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
                    case DELETE_GET_LEASE:
                        if (headerFields.length != 4)
                            throw new InvalidHeaderException("A get lease header must have exactly 4 fields. Received " + headerFields.length + ".");

                        processGetLease(protocolVersion, senderId, headerFields[3]);
                        break;
                    case DELETE_ACCEPT_LEASE:
                        if (headerFields.length != 4)
                            throw new InvalidHeaderException("A lease ok header must have exactly 4 fields. Received " + headerFields.length + ".");

                        processLeaseOk(protocolVersion, senderId, headerFields[3]);
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
     */
    private void processBackupMessage(ByteArrayInputStream byteArrayInputStream, double protocolVersion, int senderId, String fileId, int chunkNo, int desiredReplicationDegree) throws InvalidHeaderException {
        if (senderId == getServerId()) // Same sender
            return;

        checkFileIdValidity(fileId);

        /* If the server is backing up this file, it cannot store chunks from the same file */
        if (backedUpFiles.containsKey(fileId))
            return;

        desiredReplicationDegrees.putIfAbsent(fileId, desiredReplicationDegree);

        ScheduledExecutorService chunkToSend = chunksToBackUp.get(getChunkId(fileId, chunkNo));
        /* If there is a PUTCHUNK message waiting to be sent,
         * then discard it because we have already received an identical one. */
        if (chunkToSend != null) {
            chunkToSend.shutdownNow();
            chunksToBackUp.remove(getChunkId(fileId, chunkNo));
        }

        byte[] message = createMessage(
                Backup.BACKUP_SUCCESS,
                Double.toString(getProtocolVersion()),
                Integer.toString(getServerId()),
                fileId,
                Integer.toString(chunkNo));

        /* If we have already stored the chunk we just received, then just do nothing. */
        if (storedChunks.containsKey(fileId) && storedChunks.get(fileId).contains(chunkNo)) {
            addPeerStoringChunk(fileId, chunkNo, getServerId());
            controlChannel.sendMessage(message);
            return;
        }

        /* If the current replication degree is greater than or equal to the desired replication degree, then discard the message. */
        if (getCurrentReplicationDegree(fileId, chunkNo) >= desiredReplicationDegree)
            return;

        try {
            /* Update used space after backing up chunk */
            if (fileManager.storeChunk(fileId, chunkNo, byteArrayInputStream)) {
                System.out.println("Stored chunk " + chunkNo + " of fileId " + fileId + ".");
            } else {
                System.out.println("Not enough space to store chunk " + chunkNo + " from fileId " + fileId + ".");
                return;
            }
        } catch (IOException e) {
            System.err.println("Could not create file to store chunk. Discarding...");
            return;
        }

        /* If server already has a set for that fileId, adds the new chunkNo, otherwise creates a new Set */
        ConcurrentSkipListSet<Integer> fileStoredChunks = storedChunks.getOrDefault(fileId, new ConcurrentSkipListSet<>());

        fileStoredChunks.add(chunkNo);
        storedChunks.putIfAbsent(fileId, fileStoredChunks);

        if (getProtocolVersion() > 1)
            leaseManager.startLease(fileId);

        if (getProtocolVersion() > 1.0 && protocolVersion > 1.0)
            controlChannel.sendMessage(message);
        else
            controlChannel.sendMessageWithRandomDelay(message, Backup.BACKUP_REPLY_MIN_DELAY, Backup.BACKUP_REPLY_MAX_DELAY);
    }

    /**
     * Processes a stored message
     *
     * @param fileId  File Id
     * @param chunkNo Chunk number
     */
    private void processStoredMessage(int senderId, String fileId, int chunkNo) {
        /* Peer that initiated backup marks backup task as completed */
        if (getProtocolVersion() > 1.0) {
            if (incompleteTasks.containsKey(fileId)) {
                removeChunkFromIncompleteTask(fileId, chunkNo);
                saveIncompleteTasks();
            }
        }

        addPeerStoringChunk(fileId, chunkNo, senderId);
    }

    private synchronized void addPeerStoringChunk(String fileId, int chunkNo, int serverId) {
        ConcurrentHashMap<Integer, ConcurrentSkipListSet<Integer>> peersStoringFile = peersStoringChunk.getOrDefault(fileId, new ConcurrentHashMap<>());
        peersStoringChunk.putIfAbsent(fileId, peersStoringFile);

        ConcurrentSkipListSet<Integer> peersStoringChunk = peersStoringFile.getOrDefault(chunkNo, new ConcurrentSkipListSet<>());
        peersStoringFile.putIfAbsent(chunkNo, peersStoringChunk);
        peersStoringChunk.add(serverId);
    }

    /**
     * Processes a get chunk message
     *
     * @param senderProtocolVersion Sender Protocol Version
     * @param senderId              Sender Id
     * @param fileId                File Id
     * @param chunkNo               Chunk number
     * @param senderAddress         Sender IP Address
     * @throws InvalidHeaderException In case of malformed header arguments
     * @throws IOException            In case of error getting chunk path
     */

    private void processGetChunkMessage(double senderProtocolVersion, int senderId, String fileId, int chunkNo, InetAddress senderAddress) throws InvalidHeaderException, IOException {
        if (senderId == getServerId()) // Same sender
            return;

        System.out.println("Requested chunk number " + chunkNo + " of fileId " + fileId + ".");

        /* If the requested chunk is not stored in our server, then do nothing. */
        if (!storedChunks.containsKey(fileId) || !storedChunks.get(fileId).contains(chunkNo)) {
            System.out.println("But the chunk is not stored in this server.");
            return;
        }

        checkFileIdValidity(fileId);

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        chunksToSend.put(getChunkId(fileId, chunkNo), executorService);

        byte[] message = createMessage(
                fileManager.loadChunk(fileId, chunkNo),
                Recover.RESTORE_SUCCESS,
                Double.toString(getProtocolVersion()),
                Integer.toString(getServerId()),
                fileId,
                "" + chunkNo);

        if (senderProtocolVersion > 1.0 && getProtocolVersion() > 1.0) {
            System.out.println("Retrieving chunk " + chunkNo + " of fileId " + fileId + "...");
            sendMessageToSocket(message, senderAddress);
        } else {
            executorService.schedule(() -> {
                System.out.println("Retrieving chunk " + chunkNo + " of fileId " + fileId + "...");
                controlChannel.sendMessage(message);
            }, randomBetween(Recover.RESTORE_REPLY_MIN_DELAY, Recover.RESTORE_REPLY_MAX_DELAY), TimeUnit.MILLISECONDS);
        }


    }

    /**
     * Establishes TCP connection
     *
     * @param senderAddress sender InetAddress
     */
    private void createRecoverySocket(InetAddress senderAddress) {
        try {
            recoverySocket = new Socket(senderAddress, recoveryChannel.getPort());
        } catch (IOException e) {
            System.out.println("Error creating TCP socket.");
        }
    }

    /**
     * Sends a message to socket using TCP connection
     *
     * @param message       message to be sent
     * @param senderAddress Sender InetAddress
     */
    private void sendMessageToSocket(byte[] message, InetAddress senderAddress) {
        if (recoverySocket == null || recoverySocket.isClosed())
            createRecoverySocket(senderAddress);

        DataOutputStream recoverySocketOutputStream;
        try {
            recoverySocketOutputStream = new DataOutputStream(recoverySocket.getOutputStream());
        } catch (IOException e) {
            System.out.println("Error creating DataOutputStream for recovery socket.");
            try {
                recoverySocket.close();
            } catch (IOException ignored) {
            }
            return;
        }

        try {
            if (message.length > 0) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeInt(message.length);
                dos.write(message);

                recoverySocketOutputStream.write(baos.toByteArray());
            }
        } catch (IOException e) {
            System.out.println("Error writing message to recovery socket.");
            try {
                recoverySocket.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Processes restored message
     *
     * @param byteArrayInputStream InputStream containing everything after the end of the header
     * @param serverId             Server Id
     * @param fileId               File Id
     * @param chunkNo              Chunk number
     */
    private void processRestoredMessage(ByteArrayInputStream byteArrayInputStream, int serverId, String fileId, int chunkNo) {
        if (serverId == getServerId()) //Same sender
            return;

        System.out.println("Received " + Recover.RESTORE_SUCCESS + " from " + serverId + " for fileId " + fileId + " and chunk number " + chunkNo);

        ScheduledExecutorService chunkToSend = chunksToSend.get(getChunkId(fileId, chunkNo));

        /* If there is a chunk waiting to be sent, then delete it because someone sent it first. */
        if (chunkToSend != null) {
            System.out.println("Received " + Recover.RESTORE_SUCCESS + " for fileId " + fileId + " chunk number " + chunkNo + ". Discarding...");
            chunkToSend.shutdownNow();
            chunksToSend.remove(getChunkId(fileId, chunkNo));
        }

        Recover recover = ongoingRecoveries.get(fileId);

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

    /**
     * Processes delete message
     *
     * @param serverId Server Id
     * @param fileId   File Id
     */
    private void processDeleteMessage(int serverId, String fileId) {
        System.out.println("Received " + DELETE_INIT + " from " + serverId + " for file " + fileId);

        if (fileManager.deleteFile(fileId)) {
            System.out.println("Successfully deleted file " + fileId);
        } else
            System.out.println("Could not delete file " + fileId);
    }

    /**
     * Processes reclaim message
     *
     * @param serverId Server Id
     * @param fileId   File Id
     * @param chunkNo  Chunk number
     * @throws IOException In case of error getting chunk path
     */
    private void processReclaimMessage(int serverId, String fileId, int chunkNo) throws IOException {
        removePeerStoringChunk(fileId, chunkNo, serverId);

        if (serverId == getServerId())
            return;

        System.out.println("Received " + RECLAIM_SUCESS + " message for chunk number " + chunkNo + " of fileId " + fileId + " from server " + serverId);

        if (!storedChunks.containsKey(fileId) || !storedChunks.get(fileId).contains(chunkNo))
            return;

        /* Chunk Replication Degree is greater than the desired Replication Degree for that fileId */
        if (getCurrentReplicationDegree(fileId, chunkNo) >= desiredReplicationDegrees.get(fileId))
            return;

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        chunksToBackUp.put(getChunkId(fileId, chunkNo), executorService);

        byte[] chunkBody = fileManager.loadChunk(fileId, chunkNo);

        System.out.println("Ready to start backup...");

        /* Add chunk to Incomplete Tasks HashMap */
        if (getProtocolVersion() > 1.0) {
            addChunkToIncompleteTask(fileId, chunkNo);
            saveIncompleteTasks();
        }

        executorService.schedule(() -> controlChannel.sendMessage(
                createMessage(
                        chunkBody,
                        BACKUP_INIT,
                        Double.toString(getProtocolVersion()),
                        Integer.toString(getServerId()),
                        fileId,
                        Integer.toString(chunkNo),
                        Integer.toString(desiredReplicationDegrees.get(fileId)))),
                randomBetween(RECLAIM_REPLY_MIN_DELAY, RECLAIM_REPLY_MAX_DELAY),
                TimeUnit.MILLISECONDS);
    }

    private synchronized void removePeerStoringChunk(String fileId, int chunkNo, int serverId) {
        ConcurrentHashMap<Integer, ConcurrentSkipListSet<Integer>> peersStoringFile = peersStoringChunk.get(fileId);

        if (peersStoringFile != null) {
            ConcurrentSkipListSet<Integer> peersStoringChunk = peersStoringFile.get(chunkNo);

            if (peersStoringChunk != null)
                peersStoringChunk.remove(serverId);
        }
    }

    /**
     * Process the get lease message.
     *
     * @param serverVersion Server version
     * @param serverId      Server Id
     * @param fileId        File Id
     */
    private void processGetLease(double serverVersion, int serverId, String fileId) {
        if (serverVersion == 1 || serverId == getServerId())
            return;

        System.out.println("Asked to renew license for fileId " + fileId + ".");

        if (storedChunks.containsKey(fileId)) {
            sendToControlChannel(createMessage(
                    DELETE_ACCEPT_LEASE,
                    "" + getProtocolVersion(),
                    "" + getServerId(),
                    fileId
            ));
            leaseManager.leaseRenew(fileId);
        }
    }

    /**
     * Processes the lease ok message.
     *
     * @param serverVersion Server version
     * @param serverId      Server Id
     * @param fileId        File Id
     */
    private void processLeaseOk(double serverVersion, int serverId, String fileId) {
        if (serverVersion == 1 || serverId == getServerId())
            return;

        leaseManager.leaseRenew(fileId);
    }

    /**
     * Starts file backup
     *
     * @param backup File backup protocol.
     * @return Returns true if the backup was successful.
     */
    public boolean startFileBackup(Backup backup) {
        if (desiredReplicationDegrees.getOrDefault(backup.getFileId(), 0) == backup.getDesiredReplicationDegree()
                && !incompleteTasks.containsKey(backup.getFileId())) {
            System.out.println("File is already backed up in the network with the desired replication degree.");
            return true;
        }

        desiredReplicationDegrees.put(backup.getFileId(), backup.getDesiredReplicationDegree());
        backedUpFiles.putIfAbsent(backup.getFileId(), backup.getFilename());
        fileManager.saveServerMetadata();

        boolean ret = backup.start(this);
        fileManager.saveServerMetadata();
        return ret;
    }

    /**
     * Starts file recovery
     *
     * @param recover recover to be recovered
     * @return true If file was successfully restored
     */
    public boolean startFileRecovery(Recover recover) {
        /* If the fileId does not exist in the network */
        if (desiredReplicationDegrees.get(recover.getFileId()) == null) {
            System.err.println("File not found in the network.");
            return false;
        }

        ongoingRecoveries.put(recover.getFileId(), recover);

        boolean ret = recover.start(this, fileManager);
        fileManager.saveServerMetadata();
        return ret;
    }

    /**
     * Starts file delete
     *
     * @param fileId File Id
     */
    public boolean startFileDelete(String fileId) {
        /*If the fileId does not exist in the network*/
        if (!desiredReplicationDegrees.containsKey(fileId)) {
            System.out.println("File not found in the network.");
            return false;
        }

        sendToControlChannel(createMessage(
                DELETE_INIT,
                Double.toString(getProtocolVersion()),
                Integer.toString(getServerId()),
                fileId));

        fileManager.saveServerMetadata();
        return true;
    }

    /**
     * Starts the reclaim space process.
     *
     * @param storageSize Desired storage size in KBytes (1 KByte = 1000 Byte).
     * @return Returns true if the desired storage size can be set.
     */
    public void startReclaim(int storageSize) {
        fileManager.setMaxStorageSize(storageSize);

        if (!fileManager.hasSpaceAvailable()) {
            reclaimSpace();
            fileManager.saveServerMetadata();
        }
    }

    /**
     * Attempts to delete chunks in order to make space for the new storage size.
     *
     * @return Returns true if the reclaim space process was successful.
     */
    private void reclaimSpace() {
        PriorityQueue<ChunkReplication> leastNecessaryChunks = new PriorityQueue<>(new ChunkReplicationComparator());

        storedChunks.forEachEntry(10, file -> {
            String fileId = file.getKey();
            for (Integer chunk : file.getValue()) {
                int diff = getCurrentReplicationDegree(fileId, chunk) - desiredReplicationDegrees.get(fileId);
                leastNecessaryChunks.add(new ChunkReplication(fileId, chunk, diff));
            }
        });

        if (leastNecessaryChunks.isEmpty())
            return;

        /* Chunk with Replication Degree greater than Desired Replication Degree */
        while (!fileManager.hasSpaceAvailable()) {
            ChunkReplication leastNecessaryChunk = leastNecessaryChunks.poll();

            fileManager.deleteChunk(leastNecessaryChunk.getFileId(), leastNecessaryChunk.getChunkNo(), this);
            System.out.println("Successfully deleted chunk number " + leastNecessaryChunk.getChunkNo() + " belonging to fileId " + leastNecessaryChunk.getFileId() + ".");

            controlChannel.sendMessage(
                    createMessage(RECLAIM_SUCESS,
                            Double.toString(getProtocolVersion()),
                            Integer.toString(getServerId()),
                            leastNecessaryChunk.getFileId(),
                            Integer.toString(leastNecessaryChunk.getChunkNo())));
        }
    }

    /**
     * Starts the leasing of every file stored.
     */
    public void leaseStoredFiles() {
        storedChunks.forEachKey(10, leaseManager::startLease);
    }

    /**
     * Retrieves local service state information
     *
     * @return state information
     */
    public String getState() {
        StringBuilder sb = new StringBuilder();

        sb.append("Backed up files");
        backedUpFiles.forEachEntry(1, file -> {
            sb.append("\n\tPathname: ");
            sb.append(file.getValue());

            sb.append("\n\tFile Id: ");
            sb.append(file.getKey());

            sb.append("\n\tDesired Replication Degree: ");
            sb.append(desiredReplicationDegrees.get(file.getKey()));

            for (Map.Entry<Integer, ConcurrentSkipListSet<Integer>> chunk : peersStoringChunk.get(file.getKey()).entrySet()) {
                sb.append("\n\t\tChunk No: ");
                sb.append(chunk.getKey());
                sb.append("\n\t\tReplication Degree: ");
                sb.append(chunk.getValue().size());
            }
        });

        sb.append("\n\n");
        sb.append("Stored chunks");

        storedChunks.forEachEntry(1, fileChunk -> {
            if (fileChunk.getValue().isEmpty())
                return;

            sb.append("\n\tFile Id: ");
            sb.append(fileChunk.getKey());

            fileChunk.getValue().forEach(chunkNo -> {
                sb.append("\n\t\tChunk No: ");
                sb.append(chunkNo);

                sb.append("\n\t\t\tSize: ");
                try {
                    sb.append(Files.size(fileManager.getChunkPath(fileChunk.getKey(), chunkNo)) / 1000f);
                    sb.append(" KB");
                } catch (IOException e) {
                    sb.append("Could not open chunk.");
                    e.printStackTrace();
                }

                sb.append("\n\t\t\tReplication degree: ");
                sb.append(getCurrentReplicationDegree(fileChunk.getKey(), chunkNo));
            });
        });

        sb.append("\n\n\nMaximum Storage Size: ");
        sb.append(fileManager.getMaxStorageSize() / 1000.0f);
        sb.append(" KB");

        sb.append("\nCurrent Space Used: ");
        sb.append(fileManager.getUsedSpace() / 1000.0f);
        sb.append(" KB");

        return sb.toString();
    }

    /**
     * Gets recovery channel port
     *
     * @return Returns recovery channel port
     */
    public int getRecoveryChannelPort() {
        return recoveryChannel.getPort();
    }

    /**
     * Gets Incomplete Tasks HashMap
     *
     * @return Returns Incomplete Tasks HashMap
     */
    public ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> getIncompleteTasks() {
        return incompleteTasks;
    }

    public synchronized int getCurrentReplicationDegree(String fileId, int chunkNo) {
        ConcurrentHashMap<Integer, ConcurrentSkipListSet<Integer>> peersStoringFile = peersStoringChunk.get(fileId);

        if (peersStoringFile == null)
            return 0;

        ConcurrentSkipListSet<Integer> peersStoringChunk = peersStoringFile.get(chunkNo);
        if (peersStoringChunk != null)
            return peersStoringChunk.size();
        else
            return 0;
    }

    public void localChunkDeleted(String fileId, int chunkNo) {
        ConcurrentSkipListSet<Integer> chunks = storedChunks.get(fileId);
        removePeerStoringChunk(fileId, chunkNo, getServerId());

        if (chunks != null)
            chunks.remove(chunkNo);
    }

    public ConcurrentSkipListSet<Integer> getStoredChunksOfFile(String fileId) {
        return storedChunks.getOrDefault(fileId, new ConcurrentSkipListSet<>());
    }

    public void fileDeleted(String fileId) {
        peersStoringChunk.remove(fileId);
        backedUpFiles.remove(fileId);
        desiredReplicationDegrees.remove(fileId);

        if (getProtocolVersion() > 1)
            leaseManager.leaseEnd(fileId);
    }

    public ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> getStoredChunks() {
        return storedChunks;
    }

    public ConcurrentHashMap<String, Integer> getDesiredReplicationDegrees() {
        return desiredReplicationDegrees;
    }

    public ConcurrentHashMap<String, String> getBackedUpFiles() {
        return backedUpFiles;
    }

    public void setStoredChunks(ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> storedChunks) {
        this.storedChunks = storedChunks;
    }

    public void setDesiredReplicationDegrees(ConcurrentHashMap<String, Integer> desiredReplicationDegrees) {
        this.desiredReplicationDegrees = desiredReplicationDegrees;
    }

    public void setBackedUpFiles(ConcurrentHashMap<String, String> backedUpFiles) {
        this.backedUpFiles = backedUpFiles;
    }

    public void newServerMetadata() {
        storedChunks = new ConcurrentHashMap<>();
        desiredReplicationDegrees = new ConcurrentHashMap<>();
        backedUpFiles = new ConcurrentHashMap<>();
        peersStoringChunk = new ConcurrentHashMap<>();
    }

    public synchronized void setIncompleteTasks(ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> incompleteTasks) {
        this.incompleteTasks = incompleteTasks;
    }

    public synchronized void addChunkToIncompleteTask(String fileId, int chunkNo) {
        ConcurrentSkipListSet<Integer> incompleteChunks = incompleteTasks.computeIfAbsent(fileId, k -> new ConcurrentSkipListSet<>());

        incompleteChunks.add(chunkNo);
    }

    public synchronized void removeFileFromIncompleteTask(String fileId) {
        incompleteTasks.remove(fileId);
    }

    public synchronized void removeChunkFromIncompleteTask(String fileId, int chunkNo) {
        ConcurrentSkipListSet<Integer> incompleteChunks = incompleteTasks.get(fileId);

        if (incompleteChunks != null)
            incompleteChunks.remove(chunkNo);
    }

    public synchronized void saveIncompleteTasks() {
        fileManager.saveIncompleteTasks(incompleteTasks);
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConcurrentSkipListSet<Integer>>> getPeersStoringChunks() {
        return peersStoringChunk;
    }

    public void setPeersStoringChunks(ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConcurrentSkipListSet<Integer>>> peersStoringChunk) {
        this.peersStoringChunk = peersStoringChunk;
    }
}
