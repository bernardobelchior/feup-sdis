package server;

import common.IInitiatorPeer;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.MulticastSocket;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;

public class Peer extends UnicastRemoteObject implements IInitiatorPeer {
    private String protocolVersion;
    private int serverId;
    private MulticastSocket controlChannel;
    private MulticastSocket backupChannel;
    private MulticastSocket recoveryChannel;

    public Peer(String protocolVersion, int serverId, MulticastSocket controlChannel, MulticastSocket backupChannel, MulticastSocket recoveryChannel) throws RemoteException {
        super();
        this.protocolVersion = protocolVersion;
        this.serverId = serverId;
        this.controlChannel = controlChannel;
        this.backupChannel = backupChannel;
        this.recoveryChannel = recoveryChannel;
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
                sendChunk(fileId, chunkNo, replicationDegree, chunk, bytesRead);
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

    /**
     * Sends the specified chunk with number {@chunkNo} of file {@fileId}.
     * Also specifies a replication degree of {@replicationDegree}.
     *
     * @param fileId            File Identifier
     * @param chunkNo           Chunk number in file
     * @param replicationDegree Minimum number of chunk replicas
     */
    private void sendChunk(String fileId, int chunkNo, int replicationDegree, byte[] chunk) {
        String message = Server.BACKUP_INIT;

        message += " " + protocolVersion
                + " " + Integer.toString(serverId)
                + " " + fileId
                + " " + chunkNo
                + " " + Integer.toString(replicationDegree)
                + " " + Server.CRLF + Server.CRLF
                + chunk;

        System.out.println(message);
    }

    /**
     * Sends the specified chunk with number {@chunkNo} of file {@fileId}.
     * Also specifies a replication degree of {@replicationDegree}.
     *
     * @param fileId            File Identifier
     * @param chunkNo           Chunk number in file
     * @param replicationDegree Minimum number of chunk replicas
     */
    private void sendChunk(String fileId, int chunkNo, int replicationDegree, byte[] chunk, int size) {
        if (size != Server.CHUNK_SIZE)
            chunk = Arrays.copyOf(chunk, size);

        sendChunk(fileId, chunkNo, replicationDegree, chunk);
    }
}

