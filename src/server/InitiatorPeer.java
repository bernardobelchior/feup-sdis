package server;

import common.IInitiatorPeer;
import server.channel.ChannelManager;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class InitiatorPeer extends UnicastRemoteObject implements IInitiatorPeer {

    private ChannelManager channelManager;

    public InitiatorPeer(ChannelManager channelManager) throws RemoteException {
        this.channelManager = channelManager;
    }

    @Override
    public void backup(String filename, int replicationDegree) throws RemoteException {
        FileInputStream inputStream;

        try {
            inputStream = new FileInputStream(filename);
        } catch (FileNotFoundException e) {
            System.err.println("File " + filename + " not found.");
            e.printStackTrace();
            return;
        }

        String fileId = generateFileId(filename);
        byte[] chunk = new byte[Server.CHUNK_SIZE];

        try {
            int chunkNo = 0;
            int bytesRead;
            while ((bytesRead = inputStream.read(chunk)) != -1) {
                channelManager.sendChunk(fileId, chunkNo, replicationDegree, chunk, bytesRead);
                chunkNo++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void restore(String filename) throws RemoteException {
        System.out.println("Restore");
    }

    @Override
    public void delete(String filename) throws RemoteException {
        System.out.println("Delete");
    }

    @Override
    public void reclaim(int spaceReserved) throws RemoteException {
        System.out.println("Reclaim");
    }

    @Override
    public String state() throws RemoteException {
        System.out.println("State");
        return null;
    }

    /**
     * Generates File ID from its filename, last modified and permissions.
     *
     * @param filename Filename
     * @return File ID
     */
    private String generateFileId(String filename) {
        File file = new File(filename);

        String bitString = filename + Long.toString(file.lastModified()) + Boolean.toString(file.canRead()) + Boolean.toString(file.canWrite()) + Boolean.toString(file.canExecute());

        return DatatypeConverter.printHexBinary(Utils.sha256(bitString));
    }
}
