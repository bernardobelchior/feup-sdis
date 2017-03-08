package server.channel;

import server.*;
import server.MessageBuilder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
            } while (peer.getCurrentReplicationDegree(fileId) < peer.getDesiredReplicationDegree(fileId));
        }).start();
    }

    public void processMessage(byte[] message) {
        new Thread(() -> {

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message);

            String header = "";
            byte byteRead;

        /*
        * The header specification does not allow the byte '0xD' to be inside it.
        * As such, when we find a 0xD, we have either found the start of the two CRLFs of a well-formed header
        * Or the body of a malformed header.
         */
            while ((byteRead = (byte) byteArrayInputStream.read()) != 0xD) { // if byte == CR
                header += Byte.toString(byteRead);
            }

            if ((byte) byteArrayInputStream.read() != 0xA // byte == LF
                    || (byte) byteArrayInputStream.read() != 0xD // byte == CR
                    || (byte) byteArrayInputStream.read() != 0xD) { // byte == LF
                System.err.println("Found improper header. Discarding message...");
                return;
            }

            String[] headerFields = header.split(" ");

            try {
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
        for (String field : headerFields) //TODO: Delete. For debug only
            System.out.print(field + " ");
        System.out.println();

        if (headerFields.length < 3)
            throw new InvalidHeaderException("A valid message header requires at least 3 fields.");

        switch (headerFields[0]) {
            case BACKUP_INIT:
                if (headerFields.length != 5)
                    throw new InvalidHeaderException("A chunk backup header must have exactly 5 fields. Received " + headerFields.length + ".");

                Utils.checkFileIdValidity(headerFields[3]);
                int chunkNo = Utils.parseChunkNo(headerFields[4]);
                int replicationDegree = Utils.parseReplicationDegree(headerFields[5]);

                Files.copy(byteArrayInputStream, getFilePath(headerFields[3], headerFields[4]));
                peer.addFile(headerFields[3], chunkNo, replicationDegree);

                controlChannel.sendMessage(
                        MessageBuilder.createMessage(
                                Server.BACKUP_SUCCESS,
                                Integer.toString(peer.getServerId()),
                                headerFields[3],
                                headerFields[4],
                                Server.CRLF, Server.CRLF));
                break;
            case BACKUP_SUCCESS:

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

    private static Path getFilePath(String fileId, String chunkNo) {
        return new File(fileId).toPath(); //TODO: Add ChunkNo
        //return new File(fileId + chunkNo).toPath();
    }
}
