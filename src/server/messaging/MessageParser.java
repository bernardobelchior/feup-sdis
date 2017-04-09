package server.messaging;

import server.InvalidHeaderException;

import java.io.ByteArrayInputStream;

import static server.messaging.MessageBuilder.CR;
import static server.messaging.MessageBuilder.LF;


public class MessageParser {
    /**
     * Parses the header and returns it as a String.
     *
     * @param byteArrayInputStream Stream that represents the message being parsed.
     * @return Header fields.
     * @throws InvalidHeaderException In case the header is not proper.
     */
    public static String parseHeader(ByteArrayInputStream byteArrayInputStream) throws InvalidHeaderException {
        StringBuilder sb = new StringBuilder();
        byte byteRead;

        /*The header specification does not allow the byte '0xD' to be inside it.
        * As such, when we find a CR, we have either found the start of the two CRLFs of a well-formed header
        * Or a malformed header. */
        while ((byteRead = (byte) byteArrayInputStream.read()) != CR) // if byte == CR
            sb.append((char) byteRead);

        if ((byte) byteArrayInputStream.read() != LF // byte == LF
                || (byte) byteArrayInputStream.read() != CR // byte == CR
                || (byte) byteArrayInputStream.read() != LF) // byte == LF
            throw new InvalidHeaderException("Found improper header. Discarding messaging...");

        return sb.toString();
    }

    /**
     * Parses replication degree received from a header field.
     *
     * @param replicationDegree Replication degree field.
     * @return Returns replicationDegree ranging from 0 to 9.
     * @throws InvalidHeaderException Thrown when the replicationDegree is not within range.
     */
    public static int parseReplicationDegree(String replicationDegree) throws InvalidHeaderException {
        if (replicationDegree.length() > 1)
            throw new InvalidHeaderException("Replication degree field cannot be larger than 1 character. Received: " + replicationDegree);

        try {
            return Integer.parseInt(replicationDegree);
        } catch (NumberFormatException e) {
            throw new InvalidHeaderException("Invalid replication degree " + replicationDegree);
        }
    }

    /**
     * Parses chunk number from a header field.
     *
     * @param chunkNo Chunk number field.
     * @return Returns chunk number ranging from 0 to 999 999.
     * @throws InvalidHeaderException Thrown when the replicationDegree is not within range.
     */
    public static int parseChunkNo(String chunkNo) throws InvalidHeaderException {
        if (chunkNo.length() > 6)
            throw new InvalidHeaderException("Chunk number field cannot be larger than 6 characters. Received: " + chunkNo);

        try {
            return Integer.parseUnsignedInt(chunkNo);
        } catch (NumberFormatException e) {
            throw new InvalidHeaderException("Invalid chunk number " + chunkNo);
        }
    }

    /**
     * Checks fileId validity.
     *
     * @param fileId fileId
     * @throws InvalidHeaderException Thrown if fileId is not a 64-byte String.
     */
    public static void checkFileIdValidity(String fileId) throws InvalidHeaderException {
        if (fileId.length() != 64)
            throw new InvalidHeaderException("FileId has invalid length.");
    }

    /**
     * Parses the senderId.
     *
     * @param senderId senderId
     * @return Sender Id.
     * @throws InvalidHeaderException Thrown if the senderId is not a valid number.
     */
    public static int parseSenderId(String senderId) throws InvalidHeaderException {
        try {
            return Integer.parseUnsignedInt(senderId);
        } catch (NumberFormatException e) {
            throw new InvalidHeaderException("Invalid Sender Id " + senderId);
        }
    }

    /**
     * Validates the server version.
     *
     * @param serverVersion Server version string
     * @throws InvalidHeaderException In case the header is malformed
     */
    public static double parseProtocolVersion(String serverVersion) throws InvalidHeaderException {
        try {
            return Double.parseDouble(serverVersion);
        } catch (NumberFormatException e) {
            throw new InvalidHeaderException("Invalid server version.");
        }
    }
}
