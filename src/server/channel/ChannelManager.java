package server.channel;

import server.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

import static server.Server.*;

public class ChannelManager {

    private Peer peer;
    private Channel controlChannel;
    private Channel backupChannel;
    private Channel recoveryChannel;

    public ChannelManager(Peer peer, Channel controlChannel, Channel backupChannel, Channel recoveryChannel) {
        this.peer = peer;
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

    /**
     * Sends the specified chunk with number {@chunkNo} of file {@fileId}.
     * Also specifies a replication degree of {@replicationDegree}.
     * Should be started as a separated Thread.
     *
     * @param fileId            File Identifier
     * @param chunkNo           Chunk number in file
     * @param replicationDegree Minimum number of chunk replicas
     */
    public void sendChunk(String fileId, int chunkNo, int replicationDegree, byte[] chunk, int size) {
        new Thread(() -> {
            peer.addFile(fileId, chunkNo, replicationDegree);
            byte[] effectiveChunk = chunk;

            if (size != Server.CHUNK_SIZE)
                effectiveChunk = Arrays.copyOf(chunk, size);

            do {
                byte[] message = MessageBuilder.createMessage(effectiveChunk,
                        Server.BACKUP_INIT,
                        peer.getProtocolVersion(),
                        Integer.toString(peer.getServerId()),
                        fileId,
                        Integer.toString(chunkNo),
                        Integer.toString(replicationDegree));

                backupChannel.sendMessage(message);

                try {
                    Thread.sleep(Server.BACKUP_TIMEOUT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            while (peer.getCurrentReplicationDegree(fileId, chunkNo) < peer.getDesiredReplicationDegree(fileId, chunkNo));
        }).start();
    }


    public void getChunk(String fileId, int chunkNo) {
        new Thread(() -> {
            byte[] message = MessageBuilder.createMessage(Server.RESTORE_INIT, peer.getProtocolVersion(), Integer.toString(peer.getServerId()), fileId, Integer.toString(chunkNo));
            recoveryChannel.sendMessage(message);

        }).start();
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

                if (headerFields[2].equals(Integer.toString(peer.getServerId())))
                    return;

                processHeader(headerFields, byteArrayInputStream);
            } catch (InvalidHeaderException | IOException e) {
                System.err.println(e.toString());
                e.printStackTrace();
                return;
            }
        }).start();
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

                Utils.checkFileIdValidity(headerFields[3]);
                int chunkNo = Utils.parseChunkNo(headerFields[4]);
                int replicationDegree = Utils.parseReplicationDegree(headerFields[5]);

                Files.copy(byteArrayInputStream, getFilePath(headerFields[3], chunkNo), StandardCopyOption.REPLACE_EXISTING);
                peer.addFile(headerFields[3], chunkNo, replicationDegree);

                controlChannel.sendMessage(
                        MessageBuilder.createMessage(
                                Server.BACKUP_SUCCESS,
                                peer.getProtocolVersion(),
                                Integer.toString(peer.getServerId()),
                                headerFields[3],
                                headerFields[4]));
                break;
            case BACKUP_SUCCESS:
                if (headerFields.length != 5)
                    throw new InvalidHeaderException("A chunk backup header must have exactly 5 fields. Received " + headerFields.length + ".");

                Utils.checkFileIdValidity(headerFields[3]);
                chunkNo = Utils.parseChunkNo(headerFields[4]);

                peer.incrementCurrentReplicationDegree(headerFields[3], chunkNo);
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

    private Path getFilePath(String fileId, int chunkNo) {
        File file = new File(peer.getServerId() + "/" + fileId + chunkNo);

        try {
            file.mkdirs();
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return file.toPath();
    }
}
