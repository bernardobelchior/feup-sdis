package server;

import common.IInitiatorPeer;
import server.protocol.BackupFile;
import server.protocol.DeleteFile;
import server.protocol.RecoverFile;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class InitiatorPeer extends UnicastRemoteObject implements IInitiatorPeer {

    private Controller controller;

    public InitiatorPeer(Controller controller) throws RemoteException {
        this.controller = controller;
    }

    @Override
    public void backup(String filename, int replicationDegree) throws RemoteException {
        System.out.println("Starting backup of file with filename " + filename
                + " with replication degree of " + replicationDegree + "...");

        controller.startFileBackup(new BackupFile(filename, replicationDegree));
    }

    @Override
    public void restore(String filename) throws RemoteException {
        String fileId = generateFileId(filename);

        System.out.println("Starting restore of file with fileId " + fileId + "...");

        try {
            controller.startFileRecovery(new RecoverFile(filename, fileId));
        } catch (FileNotFoundException e) {
            System.err.println(e.toString());
            e.printStackTrace();
        }
    }

    @Override
    public void delete(String filename) throws RemoteException {
        String fileId = generateFileId(filename);
        System.out.println("Starting delete of file with fileId " + fileId + "...");

        controller.startFileDelete(new DeleteFile(filename, fileId));
    }

    @Override
    public boolean reclaim(int spaceReserved) throws IOException {
        System.out.println("Starting space reclaiming with space reserved of " + spaceReserved);
        return controller.startReclaim(spaceReserved);
    }

    @Override
    public String state() throws RemoteException {
        return controller.getState();
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
