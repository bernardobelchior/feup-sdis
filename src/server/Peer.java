package server;

import common.IInitiatorPeer;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.MulticastSocket;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

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
        BufferedReader reader = null;
        String content;

        try {
            reader = new BufferedReader(new FileReader("/Users/mariajoaomirapaulo/Desktop/teste2.txt"));


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        try {
            content = reader.readLine();
            System.out.println("Content :" + content);
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            inputStream = new FileInputStream(filename);
        } catch (FileNotFoundException e) {
            System.err.println("File " + filename + " not found.");
            e.printStackTrace();
            return;
        }


        String fileId = generateFileId(filename);
        byte[] chunk = new byte[64 * 1024];

        try {
            int chunkNo = 0;
            while (inputStream.read(chunk) != -1) {
                sendChunk(fileId, chunkNo, replicationDegree, chunk);
                System.out.print("chunk: " + chunk);
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
}

