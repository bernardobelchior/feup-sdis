package server;

import common.IBackupServer;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class InitiatorPeer extends UnicastRemoteObject implements IBackupServer {
    protected InitiatorPeer() throws RemoteException {
        super();
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

