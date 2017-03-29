package client;

import common.IInitiatorPeer;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

class TestApp {
    public static void main(String[] args) {
        /* Needed for Mac OS X */
        System.setProperty("java.net.preferIPv4Stack", "true");

        String peerAccessPoint = args[0];
        String operation = args[1].toUpperCase();
        String pathName;

        IInitiatorPeer initiatorPeer;

        try {
            Registry registry = LocateRegistry.getRegistry("localhost"); //TODO: This is part of peerAcessPoint
            initiatorPeer = (IInitiatorPeer) registry.lookup(peerAccessPoint);
        } catch (NotBoundException | RemoteException e) {
            e.printStackTrace();
            return;
        }

        switch (operation) {
            case "BACKUP":
                pathName = args[2];
                int replicationDegree = Integer.parseInt(args[3]);
                try {
                    if (initiatorPeer.backup(pathName, replicationDegree))
                        System.out.println("File backup successful.");
                    else
                        System.out.println("File backup failed.");
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                break;
            case "RESTORE":
                pathName = args[2];
                try {
                    if (initiatorPeer.restore(pathName))
                        System.out.println("File successfully restored.");
                    else
                        System.out.println("File recovery failed.");
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
                    System.out.println(initiatorPeer.state());
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                break;
            default:
                System.out.println("Unrecognized option " + operation + ".");
                break;
        }


    }

}
