package server;

import javafx.scene.shape.Path;

import java.io.*;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;

import static server.Server.*;

public class Peer {
    private final String protocolVersion;
    private final int serverId;
    private HashMap<String, FileEntry> fileEntryMap = new HashMap<>();

    public Peer(String protocolVersion, int serverId) {
        this.protocolVersion = protocolVersion;
        this.serverId = serverId;
    }

    public void addFile(String fileId, int desiredReplicationDegree) {
        fileEntryMap.put(fileId, new FileEntry(desiredReplicationDegree));
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public int getServerId() {
        return serverId;
    }

    public int getCurrentReplicationDegree(String fileId) {
        return fileEntryMap.get(fileId).getCurrentReplicationDegree();
    }

    public int getDesiredReplicationDegree(String fileId) {
        return fileEntryMap.get(fileId).getDesiredReplicationDegree();
    }

    public void incrementCurrentReplicationDegree(String fileId) {
        fileEntryMap.get(fileId).incrementCurrentReplicationDegree();
    }

    public void processMessage(byte[] message) {
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

    private void backup
}

