package server.protocol;

import server.Controller;
import server.Server;
import server.messaging.MessageBuilder;

import static server.Server.getProtocolVersion;
import static server.Server.getServerId;

public class DeleteFile {
    private final String fileId;

    public DeleteFile(String fileId) {
        this.fileId = fileId;
    }

    public void start(Controller controller) {
        byte[] message = MessageBuilder.createMessage(Server.DELETE_INIT, Double.toString(getProtocolVersion()), Integer.toString(getServerId()), fileId);
        controller.sendToControlChannel(message);
    }

    public String getFileId() {
        return fileId;
    }

}
