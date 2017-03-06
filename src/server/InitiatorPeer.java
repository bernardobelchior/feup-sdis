package server;

import common.IBackupServer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MulticastSocket;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class InitiatorPeer extends UnicastRemoteObject implements IBackupServer {
    private final MulticastSocket controlChannel;
    private final MulticastSocket backupChannel;
    private final MulticastSocket recoveryChannel;

    public InitiatorPeer(MulticastSocket controlChannel, MulticastSocket backupChannel, MulticastSocket recoveryChannel) throws RemoteException {
        super();
        this.controlChannel = controlChannel;
        this.backupChannel = backupChannel;
        this.recoveryChannel = recoveryChannel;
    }

    @Override
    public void backup(String filename, int replicationDegree) throws RemoteException {
        System.out.println("Backup");

        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filename);
        } catch (FileNotFoundException e) {
            System.err.println("File " + filename + " not found.");
            e.printStackTrace();
            return;
        }

        byte[] chunk = new byte[64 * 1024];

        try {
            while (inputStream.read(chunk) != -1) {

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

    private sendMessage(String fileId, int chunkNo, int replicationDegree) {

    }
}

