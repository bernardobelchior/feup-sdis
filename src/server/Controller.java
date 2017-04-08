package server;


import server.messaging.Channel;
import server.messaging.MessageBuilder;
import server.protocol.BackupFile;
import server.protocol.RecoverFile;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> chunkCurrentReplicationDegree;

    /*Concurrent HashMap with fileId and respective desired Replication Degree*/
    private ConcurrentHashMap<String, Integer> desiredReplicationDegrees;

    /*Concurrent HashMap with fileId and respective ExecuterService for chunks prepared to be backed up */
    private final ConcurrentHashMap<String, ScheduledExecutorService> chunksToBackUp = new ConcurrentHashMap<>();

    /*Server's stored chunks*/
    private ConcurrentHashMap<String, Set<Integer>> storedChunks;

    /* Max storage size allowed, in bytes */
    private int maxStorageSize = (int) (Math.pow(1000, 2) * 8); // 8 Megabytes

    /* Storage size used to store chunks. Value is updated on backup and delete protocol*/
    private long usedSpace;

    /* Maps FileId to Filename */
    private ConcurrentHashMap<String, String> backedUpFiles = new ConcurrentHashMap<>();

    /* Socket used for tcp connection */
    private Socket recoverySocket;

    private OutputStream outputStream;
    private DataOutputStream dataOutputStream;

    public Controller(Channel controlChannel, Channel backupChannel, Channel recoveryChannel) throws InstantiationException {
        this.controlChannel = controlChannel;
        this.backupChannel = backupChannel;
        this.recoveryChannel = recoveryChannel;

        try {
            initializeDirs();
        } catch (IOException e) {
            System.err.println("Could not initialize directories. Exiting...");
            throw new InstantiationException();
        }

        if (!loadServerMetadata()) {
            storedChunks = new ConcurrentHashMap<>();
            desiredReplicationDegrees = new ConcurrentHashMap<>();
            chunkCurrentReplicationDegree = new ConcurrentHashMap<>();
            usedSpace = 0;
        }

        this.controlChannel.setController(this);
        this.backupChannel.setController(this);
        this.recoveryChannel.setController(this);

        this.controlChannel.listen();
        this.backupChannel.listen();
        this.recoveryChannel.listen();

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(this::saveServerMetadata, 5, 5, TimeUnit.SECONDS);
    }

    private void initializeDirs() throws IOException {
        new File(BASE_DIR).mkdir();
        new File(BASE_DIR + CHUNK_DIR).mkdir();
        new File(BASE_DIR + RESTORED_DIR).mkdir();
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
    public void processMessage(byte[] message, int size, InetAddress senderAddr) {
        new Thread(() -> {

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message, 0, size);

            try {
                String[] headerFields = parseHeader(byteArrayInputStream).split(" ");
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
                                Thread.sleep(randomBetween(BACKUP_REPLY_MIN_DELAY, BACKUP_REPLY_MAX_DELAY));
                            } catch (InterruptedException ignored) {
                            }

                        processBackupMessage(byteArrayInputStream, protocolVersion, senderId, headerFields[3], chunkNo, replicationDegree);
                        break;
                    case BACKUP_SUCCESS:
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A chunk stored header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processStoredMessage(headerFields[3], chunkNo);
                        break;
                    case RESTORE_INIT:
                        System.out.println("Received message " + headerFields[0] + " from " + senderId);
                        if (headerFields.length != 5)
                            throw new InvalidHeaderException("A get chunk header must have exactly 5 fields. Received " + headerFields.length + ".");

                        processGetChunkMessage(protocolVersion, senderId, headerFields[3], chunkNo, senderAddr);
                        break;
                    case RESTORE_SUCCESS:
                        System.out.println("Received message " + headerFields[0] + " from " + senderId);

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

        /* If we have already stored the chunk we just received, then just do nothing. */
        if (storedChunks.containsKey(fileId) && storedChunks.get(fileId).contains(chunkNo))
            return;

        /* If the current replication degree is greater than or equal to the desired replication degree, then discard the message. */
        if (chunkCurrentReplicationDegree.getOrDefault(fileId, new ConcurrentHashMap<>()).getOrDefault(chunkNo, 0) >= desiredReplicationDegree)
            return;

        int chunkSize = byteArrayInputStream.available();
        if (!hasSpaceAvailable(chunkSize)) {
            System.out.println("Not enough space to backup chunk " + chunkNo + " from fileId " + fileId + ".");
            return;
        }

        try {
            /* Update used space after backing up chunk */
            usedSpace += storeChunk(fileId, chunkNo, byteArrayInputStream);
            System.out.println("Stored chunk " + chunkNo + " of fileId " + fileId + ".");
        } catch (IOException e) {
            System.err.println("Could not create file to store chunk. Discarding...");
            return;
        }

        /* If server already has a set for that fileId, adds the new chunkNo, otherwise creates a new Set */
        Set<Integer> fileStoredChunks = storedChunks.getOrDefault(fileId, Collections.synchronizedSet(new HashSet<>()));

        fileStoredChunks.add(chunkNo);
        storedChunks.putIfAbsent(fileId, fileStoredChunks);

        byte[] message = MessageBuilder.createMessage(
                BACKUP_SUCCESS,
                Double.toString(getProtocolVersion()),
                Integer.toString(getServerId()),
                fileId,
                Integer.toString(chunkNo));

        if (getProtocolVersion() > 1.0 && protocolVersion > 1.0)
            controlChannel.sendMessage(message);
        else
            controlChannel.sendMessageWithRandomDelay(message, BACKUP_REPLY_MIN_DELAY, BACKUP_REPLY_MAX_DELAY);
    }

    private long storeChunk(String fileId, int chunkNo, ByteArrayInputStream byteArrayInputStream) throws IOException {
        int chunkSize = byteArrayInputStream.available();

        Path chunkPath = getChunkPath(fileId, chunkNo);
        if (chunkPath.toFile().exists())
            chunkPath.toFile().delete();

        Files.copy(byteArrayInputStream, chunkPath, StandardCopyOption.REPLACE_EXISTING);
        return chunkSize;
    }

    private void processStoredMessage(String fileId, int chunkNo) throws IOException {
        incrementReplicationDegree(fileId, chunkNo);
    }

    /**
     * Processes a get chunk message
     * @param SenderProtocolVersion sender Protocol Version
     * @param senderId Sender Id
     * @param fileId File Id
     * @param chunkNo Chunk number
     * @throws InvalidHeaderException In case of malformed header arguments
     * @throws IOException In case of error getting chunk path
     */
    private void processGetChunkMessage(double SenderProtocolVersion, int senderId, String fileId, int chunkNo, InetAddress senderAddress) throws InvalidHeaderException, IOException {
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

        if(SenderProtocolVersion > 1.0 && getProtocolVersion() > 1.0){
            executorService.schedule(() -> {
                System.out.println("Retrieving chunk " + chunkNo + " of fileId " + fileId + "...");
                sendMessageToSocket(message,senderAddress);
            }, randomBetween(RESTORE_REPLY_MIN_DELAY, RESTORE_REPLY_MAX_DELAY), TimeUnit.MILLISECONDS);

        }
        else{
            executorService.schedule(() -> {
                System.out.println("Retrieving chunk " + chunkNo + " of fileId " + fileId + "...");
                controlChannel.sendMessage(message);
            }, randomBetween(RESTORE_REPLY_MIN_DELAY, RESTORE_REPLY_MAX_DELAY), TimeUnit.MILLISECONDS);
        }


    }

    /**
     * Establishes TCP connection
     * @param senderAddress sender InetAddress
     */
    public void createRecoverySocket(InetAddress senderAddress){
        try {
            recoverySocket = new Socket(senderAddress, recoveryChannel.getPort());
            outputStream = recoverySocket.getOutputStream();
            dataOutputStream = new DataOutputStream(outputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Sends a message to socket using TCP connection
     * @param message message to be sent
     * @param senderAddress Sender InetAddress
     */
    public void sendMessageToSocket(byte[] message, InetAddress senderAddress) {

        if(recoverySocket==null)
            createRecoverySocket(senderAddress);

        try {
            dataOutputStream.writeInt(message.length);
            if (message.length > 0) {
                dataOutputStream.write(message);
            }
        } catch (IOException e) {
            System.err.println("Error creating socket...");
        }

    }

    /**
     * Processes restored message
     * @param byteArrayInputStream InputStream containing everything after the end of the header
     * @param serverId Server Id
     * @param fileId File Id
     * @param chunkNo Chunk number
     */
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

    /**
     * Processes delete message
     * @param serverId Server Id
     * @param fileId File Id
     * @throws IOException  In case of error getting chunk path
     */
    private void processDeleteMessage(int serverId, String fileId) throws IOException {
        long chunksSize = 0;
        desiredReplicationDegrees.remove(fileId);
        chunkCurrentReplicationDegree.remove(fileId);
        backedUpFiles.remove(fileId);

        if (!storedChunks.containsKey(fileId))
            return;

        System.out.println("Received " + DELETE_INIT + " from " + serverId + " for file " + fileId);

        for (Integer chunkNo : storedChunks.get(fileId)) {
            chunksSize += Files.size(getChunkPath(fileId, chunkNo));
            Files.deleteIfExists(getChunkPath(fileId, chunkNo));
        }

        storedChunks.remove(fileId);

        /*Update storage size used to store chunks*/
        usedSpace -= chunksSize;

        System.out.println("Successfully deleted file " + fileId);
    }

    /**
     * Processes reclaim message
     * @param serverId Server Id
     * @param fileId File Id
     * @param chunkNo Chunk number
     * @throws IOException In case of error getting chunk path
     */
    private void processReclaimMessage(int serverId, String fileId, int chunkNo) throws IOException {
        decrementReplicationDegree(fileId, chunkNo);

        if (serverId == getServerId())
            return;

        System.out.println("Received " + RECLAIM_SUCESS + " message for chunk number " + chunkNo + " of fileId " + fileId + " from server " + serverId);

        if (!storedChunks.containsKey(fileId) || !storedChunks.get(fileId).contains(chunkNo))
            return;

        /* Chunk Replication Degree is greater than the desired Replication Degree for that fileId */
        if (chunkCurrentReplicationDegree.get(fileId).get(chunkNo) >= desiredReplicationDegrees.get(fileId))
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
                        Integer.toString(desiredReplicationDegrees.get(fileId)))),
                randomBetween(RECLAIM_REPLY_MIN_DELAY, RECLAIM_REPLY_MAX_DELAY),
                TimeUnit.MILLISECONDS);
    }


    /**
     * Increments replication degree
     * @param fileId File Id
     * @param chunkNo Chunk number
     */
    private void incrementReplicationDegree(String fileId, int chunkNo) {
        chunkCurrentReplicationDegree.putIfAbsent(fileId, new ConcurrentHashMap<>());

        ConcurrentHashMap<Integer, Integer> chunks = chunkCurrentReplicationDegree.get(fileId);
        chunks.put(chunkNo, chunks.getOrDefault(chunkNo, 0) + 1);
    }

    /***
     * Decrements replication degree
     * @param fileId File Id
     * @param chunkNo Chunk number
     */
    private void decrementReplicationDegree(String fileId, int chunkNo) {
        ConcurrentHashMap<Integer, Integer> chunks = chunkCurrentReplicationDegree.get(fileId);
        chunks.put(chunkNo, chunks.getOrDefault(chunkNo, 0) - 1);
    }

    /**
     * Delets a chunk
     * @param fileId File Id
     * @param chunkNo Chunk number
     * @throws IOException
     */
    public void deleteChunk(String fileId, Integer chunkNo) throws IOException {
        System.out.println("Deleting chunkNo " + chunkNo + " from file" + fileId);

         /* Decrement Replication Degree */
        ConcurrentHashMap<Integer, Integer> chunks = chunkCurrentReplicationDegree.get(fileId);
        chunks.put(chunkNo, chunks.getOrDefault(chunkNo, 0) - 1);

         /*Update storage size used to store chunks*/
        usedSpace -= Files.size(getChunkPath(fileId, chunkNo));

        /* Delete Chunk */
        Files.deleteIfExists(getChunkPath(fileId, chunkNo));
        storedChunks.get(fileId).remove(chunkNo);
    }


    public boolean startFileBackup(BackupFile backupFile) {
        ConcurrentHashMap<Integer, Integer> chunksReplicationDegree = new ConcurrentHashMap<>();
        chunkCurrentReplicationDegree.put(backupFile.getFileId(), chunksReplicationDegree);
        desiredReplicationDegrees.putIfAbsent(backupFile.getFileId(), backupFile.getDesiredReplicationDegree());
        backedUpFiles.put(backupFile.getFileId(), backupFile.getFilename());

        boolean ret = backupFile.start(this, chunksReplicationDegree);
        saveServerMetadata();
        return ret;
    }

    public boolean startFileRecovery(RecoverFile recoverFile) {
        /* If the fileId does not exist in the network */
        if (desiredReplicationDegrees.get(recoverFile.getFileId()) == null) {
            System.err.println("File not found in the network.");
            return false;
        }

        ongoingRecoveries.put(recoverFile.getFileId(), recoverFile);

        boolean ret = recoverFile.start(this);
        saveServerMetadata();
        return ret;
    }

    /**
     * Starts file delete
     * @param fileId File Id
     */
    public boolean startFileDelete(String fileId) {
        /*If the fileId does not exist in the network*/
        if (!desiredReplicationDegrees.containsKey(fileId)) {
            System.out.println("File not found in the network.");
            return false;
        }

        byte[] message = MessageBuilder.createMessage(Server.DELETE_INIT, Double.toString(getProtocolVersion()), Integer.toString(getServerId()), fileId);
        sendToControlChannel(message);

        saveServerMetadata();
        return true;
    }
    /**
     * Starts the reclaim space process.
     *
     * @param storageSize Desired storage size in KBytes (1 KByte = 1000 Byte).
     * @return Returns true if the desired storage size can be set.
     */
    public boolean startReclaim(int storageSize) {
        if (storageSize > maxStorageSize) {
            maxStorageSize = storageSize;
            return true;
        } else {
            maxStorageSize = storageSize;
            boolean ret = reclaimSpace();
            saveServerMetadata();
            return ret;
        }
    }

    /**
     * Attempts to delete chunks in order to make space for the new storage size.
     *
     * @return Returns true if the reclaim space process was successful.
     */
    public boolean reclaimSpace() {
        PriorityQueue<ChunkReplication> leastNecessaryChunks = new PriorityQueue<>(new ChunkReplicationComparator());

        int diff;

        Set<Map.Entry<String, Set<Integer>>> files = storedChunks.entrySet();

        for (Map.Entry<String, Set<Integer>> file : files) {
            String fileId = file.getKey();
            for (Integer chunk : file.getValue()) {
                diff = chunkCurrentReplicationDegree.get(fileId).get(chunk) - desiredReplicationDegrees.get(fileId);
                leastNecessaryChunks.add(new ChunkReplication(fileId, chunk, diff));
            }
        }

        if (leastNecessaryChunks.isEmpty())
            return true;

        /* Chunk with Replication Degree greater than Desired Replication Degree */
        while (usedSpace > maxStorageSize) {
            ChunkReplication leastNecessaryChunk = leastNecessaryChunks.poll();

            try {
                deleteChunk(leastNecessaryChunk.getFileId(), leastNecessaryChunk.getChunkNo());
                System.out.println("Successfully deleted chunk number " + leastNecessaryChunk.getChunkNo() + " belonging to fileId " + leastNecessaryChunk.getFileId() + ".");

                controlChannel.sendMessage(
                        MessageBuilder.createMessage(Server.RECLAIM_SUCESS,
                                Double.toString(getProtocolVersion()),
                                Integer.toString(getServerId()),
                                leastNecessaryChunk.getFileId(),
                                Integer.toString(leastNecessaryChunk.getChunkNo())));
            } catch (IOException e) {
                System.out.println("Could not delete chunk number " + leastNecessaryChunk.getChunkNo() + " belonging to fileId " + leastNecessaryChunk.getFileId() + ".");
            }
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
            objectInputStream = new ObjectInputStream(new FileInputStream(getFile("." + getServerId())));
        } catch (IOException e) {
            System.err.println("Could not open configuration file for reading.");
            return false;
        }

        try {
            backedUpFiles = (ConcurrentHashMap<String, String>) objectInputStream.readObject();
            storedChunks = (ConcurrentHashMap<String, Set<Integer>>) objectInputStream.readObject();
            desiredReplicationDegrees = (ConcurrentHashMap<String, Integer>) objectInputStream.readObject();
            chunkCurrentReplicationDegree = (ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>>) objectInputStream.readObject();
        } catch (IOException e) {
            System.err.println("Could not read from configuration file.");
            return false;
        } catch (ClassNotFoundException e) {
            System.err.println("Unknown content in configuration file.");
            return false;
        }

        try {
            usedSpace = getDirectorySize(BASE_DIR + CHUNK_DIR);
        } catch (IOException e) {
            System.err.println("Could not get server actual size.");
            return false;
        }

        System.out.println("Server metadata loaded successfully.");
        return true;
    }

    /**
     * Saves server metadata
     */
    private void saveServerMetadata() {
        ObjectOutputStream objectOutputStream;
        try {
            objectOutputStream = new ObjectOutputStream(new FileOutputStream(getFile("." + getServerId())));
        } catch (IOException e) {
            System.err.println("Could not open configuration file for writing.");
            return;
        }

        try {
            objectOutputStream.writeObject(backedUpFiles);
            objectOutputStream.writeObject(storedChunks);
            objectOutputStream.writeObject(desiredReplicationDegrees);
            objectOutputStream.writeObject(chunkCurrentReplicationDegree);
            System.out.println("Server metadata successfully saved.");
        } catch (IOException e) {
            System.err.println("Could not write to configuration file.");
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
            sb.append(desiredReplicationDegrees.get(file.getKey()));

            if (chunkCurrentReplicationDegree.containsKey(file.getKey()))
                for (Map.Entry<Integer, Integer> chunk : chunkCurrentReplicationDegree.get(file.getKey()).entrySet()) {
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
                sb.append(chunkCurrentReplicationDegree.get(fileChunk.getKey()).getOrDefault(chunkNo, 0));
            }
        }

        sb.append("\n\n\nMaximum Storage Size: ");
        sb.append(maxStorageSize / 1000.0f);
        sb.append(" KB");

        sb.append("\nCurrent Space Used: ");
        sb.append(usedSpace / 1000.0f);
        sb.append(" KB");

        return sb.toString();
    }

    /**
     * Checks if peer has available space to store a new chunk
     * @param chunkSize chunk size
     * @return Returns true if peers has available space to store chunk
     */
    public boolean hasSpaceAvailable(int chunkSize) {
        return usedSpace + (long) chunkSize < maxStorageSize;
    }

    public Channel getRecoveryChannel() {
        return recoveryChannel;
    }

}
