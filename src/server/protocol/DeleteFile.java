package server.protocol;

import server.Controller;
import server.Server;
import server.messaging.MessageBuilder;

import static server.Server.getProtocolVersion;
import static server.Server.getServerId;

public class DeleteFile {
    private final String filename;
    private final String fileId;
    private Controller controller;

    public DeleteFile(String filename, String fileId) {
        this.filename = filename;
        this.fileId = fileId;
    }

    public void start(Controller controller) {
        this.controller = controller;
        byte[] message = MessageBuilder.createMessage(Server.DELETE_INIT, Double.toString(getProtocolVersion()), Integer.toString(getServerId()), fileId);
        this.controller.sendDeleteMessage(message);
    }

    public String getFileId() {
        return fileId;
    }

}
