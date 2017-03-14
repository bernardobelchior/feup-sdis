package server;

import common.IInitiatorPeer;
import server.protocol.BackupFile;
import server.protocol.RecoverFile;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileNotFoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class InitiatorPeer extends UnicastRemoteObject implements IInitiatorPeer {

    private Controller controller;

    public InitiatorPeer(Controller controller) throws RemoteException {
        this.controller = controller;
    }

    @Override
    public void backup(String filename, int replicationDegree) throws RemoteException {
        controller.startFileBackup(new BackupFile(filename, replicationDegree));
    }

    @Override
    public void restore(String filename) throws RemoteException {
        String fileId = generateFileId(filename);

        try {
            controller.startFileRecovery(new RecoverFile(filename, fileId));
        } catch (FileNotFoundException e) {
            System.err.println(e.toString());
            e.printStackTrace();
        }
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
