package client;

import common.IBackupServer;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class TestApp {

    public  static IBackupServer initiatorPeer;

    public static void main(String[] args) {

        String peerAccessPoint = args[0];
        String operation = args[1];
        String pathName;

        try {
            Registry registry = LocateRegistry.getRegistry("localhost");
            initiatorPeer = (IBackupServer) registry.lookup(peerAccessPoint);
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        switch (operation){
            case "BACKUP":
                pathName = args[2];
                int replicationDegree = Integer.parseInt(args[3]);
                try {
                    initiatorPeer.backup(pathName,replicationDegree);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                break;
            case "RESTORE":
                pathName = args[2];
                try {
                    initiatorPeer.restore(pathName);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                break;
            case "DELETE":
                pathName = args[2];
                try {
                    initiatorPeer.delete(pathName);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                break;
            case "RECLAIM":
                int maximumDiskSpace = Integer.parseInt(args[2]);
                try {
                    initiatorPeer.reclaim(maximumDiskSpace);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                break;
            case "STATE":
                try {
                    initiatorPeer.state();
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                break;

        }


    }

}
