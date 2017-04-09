package server;

import common.IInitiatorPeer;
import server.protocol.Backup;
import server.protocol.Recover;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class InitiatorPeer extends UnicastRemoteObject implements IInitiatorPeer {

    /**
     * Controller class.
     */
    private final Controller controller;

    public InitiatorPeer(Controller controller) throws RemoteException {
        this.controller = controller;
    }

    @Override
    public boolean backup(String filename, int replicationDegree) throws RemoteException {
        System.out.println("Starting backup of file with filename " + filename
                + " with replication degree of " + replicationDegree + "...");

        return controller.startFileBackup(new Backup(filename, replicationDegree));
    }

    @Override
    public boolean restore(String filename) throws RemoteException {
        String fileId = generateFileId(filename);

        System.out.println("Starting restore of file with fileId " + fileId + "...");

        return controller.startFileRecovery(new Recover(filename, fileId));
    }

    /**
     * Deletes the specified file.
     *
     * @param filename Name of file to delete.
     * @return Returns true if the file can be deleted.
     * @throws RemoteException In case there is a problem with RMI.
     */
    @Override
    public boolean delete(String filename) throws RemoteException {
        String fileId = generateFileId(filename);
        System.out.println("Starting deletion of file with fileId " + fileId + "...");

        return controller.startFileDelete(fileId);
    }

    @Override
    public void reclaim(int spaceReserved) throws RemoteException {
        System.out.println("Starting space reclaiming with space reserved of " + spaceReserved + " KBytes");
        controller.startReclaim(spaceReserved * 1000);
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
