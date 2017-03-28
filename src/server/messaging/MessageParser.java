package server.messaging;

import server.InvalidHeaderException;

import java.io.ByteArrayInputStream;

import static server.Server.CR;
import static server.Server.LF;


public class MessageParser {
    /**
     * Parses the header and returns it as a String.
     *
     * @param byteArrayInputStream Stream that represents the message being parsed.
     * @return Header fields.
     * @throws InvalidHeaderException In case the header is not proper.
     */
    public static String parseHeader(ByteArrayInputStream byteArrayInputStream) throws InvalidHeaderException {
        String header = "";
        byte byteRead;

        /*The header specification does not allow the byte '0xD' to be inside it.
        * As such, when we find a CR, we have either found the start of the two CRLFs of a well-formed header
        * Or a malformed header. */
        while ((byteRead = (byte) byteArrayInputStream.read()) != CR) // if byte == CR
            header += Character.toString((char) byteRead);

        if ((byte) byteArrayInputStream.read() != LF // byte == LF
                || (byte) byteArrayInputStream.read() != CR // byte == CR
                || (byte) byteArrayInputStream.read() != LF) // byte == LF
            throw new InvalidHeaderException("Found improper header. Discarding messaging...");

        return header;
    }
}
