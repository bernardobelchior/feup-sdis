package server.messaging;

import java.io.ByteArrayOutputStream;

/**
 * Message builder class.
 */
public class MessageBuilder {
    /**
     * Carriage return.
     */
    public static final byte CR = 0xD;

    /**
     * Line feed
     */
    public static final byte LF = 0xA;

    /**
     * Carriage return and line feed joined.
     */
    private static final String CRLF = "" + (char) CR + (char) LF;

    /**
     * Creates the messaging that only uses a header.
     * Header format:
     * <MessageType> <Version> <SenderId> <FileId> <ChunkNo> <ReplicationDeg> <CRLF><CRLF>
     *
     * @param headerFields Every field of the header, in the correct sequence.
     * @return Message
     */
    public static byte[] createMessage(String... headerFields) {
        return (String.join(" ", headerFields) + " " + CRLF + CRLF).getBytes();
    }

    /**
     * Creates a messaging with header and body
     *
     * @param body         Message body
     * @param headerFields Header fields in sequence.
     * @return Message
     */
    public static byte[] createMessage(byte[] body, String... headerFields) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        byte[] header = createMessage(headerFields);
        byteArrayOutputStream.write(header, 0, header.length);
        byteArrayOutputStream.write(body, 0, body.length);
        return byteArrayOutputStream.toByteArray();
    }
}
