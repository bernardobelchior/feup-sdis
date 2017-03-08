package server;

import server.channel.Channel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static server.Server.*;

/**
 * Created by mariajoaomirapaulo on 08/03/17.
 */
public class ChannelManager {

    private Peer peer;
    private Channel controlChannel;
    private Channel backupChannel;
    private Channel recoveryChannel;

    public ChannelManager(Peer peer, Channel controlChannel, Channel backupChannel, Channel recoveryChannel) {
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
                byte[] message = createMessage(effectiveChunk,
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

    /**
     * Creates the message that only uses a header.
     * Header format:
     * <MessageType> <Version> <SenderId> <FileId> <ChunkNo> <ReplicationDeg> <CRLF>
     *
     * @param headerFields Every field of the header, in the correct sequence.
     * @return Message
     */
    protected static byte[] createMessage(String... headerFields) {
        return (String.join(" ", headerFields) + " " + Server.CRLF + Server.CRLF).getBytes();
    }

    /**
     * Creates a message with header and body
     *
     * @param body         Message body
     * @param headerFields Header fields in sequence.
     * @return Message
     */
    protected static byte[] createMessage(byte[] body, String... headerFields) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        byte[] header = createMessage(headerFields);
        byteArrayOutputStream.write(header, 0, header.length);
        byteArrayOutputStream.write(body, 0, body.length);
        return byteArrayOutputStream.toByteArray();
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

            processHeader(headerFields, byteArrayInputStream);

        /*
        * Now we are:
        * - At the end of message if it only has a header
        * - In the body if it has one
         */

        /*if (byteArrayInputStream.available() != 0) { //If there is something to read
            byte[] body = new byte[CHUNK_SIZE];
            byteArrayInputStream.read(body, 0, body.length);

        }*/

        }).start();
    }

    /**
     * Processes the header. Receives a {@link ByteArrayInputStream} for future parsing, if needed.
     * Header format:
     * <MessageType> <Version> <SenderId> <FileId> <ChunkNo> <ReplicationDeg> <CRLF>
     *
     * @param headerFields         Array of header fields.
     * @param byteArrayInputStream {@link ByteArrayInputStream} that contains the rest of the message.
     */
    private void processHeader(String[] headerFields, ByteArrayInputStream byteArrayInputStream) {
        for (String field : headerFields)
            System.out.print(field + " ");
        System.out.println();

        //TODO: Missing error checking

        switch (headerFields[0]) {
            case BACKUP_INIT:
                try {
                    Files.copy(byteArrayInputStream, new File(headerFields[3]).toPath());
                    //TODO: ChunkNo to File Id
                } catch (IOException e) {
                    e.printStackTrace();
                }

                //How to send reply??
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
        }
    }

}
