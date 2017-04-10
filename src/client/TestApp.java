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
            Registry registry = LocateRegistry.getRegistry("localhost");
            initiatorPeer = (IInitiatorPeer) registry.lookup(peerAccessPoint);
        } catch (NotBoundException | RemoteException e) {
            System.out.println("Could not find connect to peer with access point: " + peerAccessPoint);
            return;
        }

        switch (operation) {
            case "BACKUP":
            case "BACKUPENH":
                pathName = args[2];
                int replicationDegree = Integer.parseInt(args[3]);

                if (replicationDegree < 2) {
                    System.out.println("Desired replication degree is too low. Minimum value is 2.");
                    return;
                }

                try {
                    if (initiatorPeer.backup(pathName, replicationDegree))
                        System.out.println("File backup successful.");
                    else
                        System.out.println("File backup failed.");
                } catch (RemoteException ignored) {
                }
                break;
            case "RESTORE":
            case "RESTOREENH":
                pathName = args[2];
                try {
                    if (initiatorPeer.restore(pathName))
                        System.out.println("File successfully restored.");
                    else
                        System.out.println("File recovery failed.");
                } catch (RemoteException ignored) {
                }
                break;
            case "DELETE":
            case "DELETEENH":
                pathName = args[2];
                try {
                    if (initiatorPeer.delete(pathName))
                        System.out.println("File deletion successful.");
                    else
                        System.out.println("File deletion failed.");
                } catch (RemoteException ignored) {
                }
                break;
            case "RECLAIM":
            case "RECLAIMENH":
                int maximumDiskSpace = Integer.parseInt(args[2]);
                try {
                    initiatorPeer.reclaim(maximumDiskSpace);
                } catch (RemoteException ignored) {
                }
                break;
            case "STATE":
                try {
                    System.out.println(initiatorPeer.state());
                } catch (RemoteException ignored) {
                }
                break;
            default:
                System.out.println("Unrecognized option " + operation + ".");
                break;
        }


    }

}
