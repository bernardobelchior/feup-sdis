package server;

import common.IBackupServer;

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
}

