package server;

/**
 * Created by bernardo on 3/7/17.
 */
public class MessageBuilder {
    public static String createMessageWithoutTerminator(String messageType, String protocolVersion, int serverId, String fileId, int chunkNo, int replicationDegree) {
        return createMessageWithoutTerminator(messageType, protocolVersion, serverId, fileId, chunkNo) + " " + Integer.toString(replicationDegree);
    }

    public static String createMessageWithoutTerminator(String messageType, String protocolVersion, int serverId, String fileId, int chunkNo) {
        return createMessageWithoutTerminator(messageType, protocolVersion, serverId, fileId) + " " + Integer.toString(chunkNo);
    }

    public static String createMessageWithoutTerminator(String messageType, String protocolVersion, int serverId, String fileId) {
        return messageType + " " + protocolVersion + " " + Integer.toString(serverId) + " " + fileId;
    }
}
