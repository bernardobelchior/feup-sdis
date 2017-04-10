package server;

import common.IInitiatorPeer;
import server.protocol.Backup;
import server.protocol.Recover;

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

    /**
     * Starts the backup protocol.
     *
     * @param filename          Name of file to backup.
     * @param replicationDegree Number of copies of each chunk to keep around at any time.
     * @return True if the file is backed up correctly.
     * @throws RemoteException In case there is a problem with RMI.
     */
    @Override
    public boolean backup(String filename, int replicationDegree) throws RemoteException {
        System.out.println("Starting backup of file with filename " + filename
                + " with replication degree of " + replicationDegree + "...");

        return controller.startFileBackup(new Backup(filename, replicationDegree));
    }

    /**
     * Starts the restore protocol.
     *
     * @param filename Name of file to restore.
     * @return Returns true if the file is restored correctly.
     * @throws RemoteException In case there is a problem with RMI.
     */
    @Override
    public boolean restore(String filename) throws RemoteException {
        String fileId = controller.fileManager.generateFileId(filename);

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
        String fileId = controller.fileManager.generateFileId(filename);
        System.out.println("Starting deletion of file with fileId " + fileId + "...");

        return controller.startFileDelete(fileId);
    }

    /**
     * Starts the space reclaim protocol.
     *
     * @param spaceReserved Number of bytes to allocate to the backup service.
     * @throws RemoteException In case there is a problem with RMI.
     */
    @Override
    public void reclaim(int spaceReserved) throws RemoteException {
        System.out.println("Starting space reclaiming with space reserved of " + spaceReserved + " KBytes");
        controller.startReclaim(spaceReserved * 1000);
    }

    /**
     * Starts the state protocol.
     *
     * @return A string containing the desired information.
     * @throws RemoteException In case there is a problem with RMI.
     */
    @Override
    public String state() throws RemoteException {
        return controller.getState();
    }

}
