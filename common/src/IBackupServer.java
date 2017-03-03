/**
 * Interface to use with Remote Method Invocation
 */
public interface IBackupServer {
    /**
     * Backs up the file with {@filename} and have at least {@replicationDegree} copies of each chunk at any time.
     *
     * @param filename          Name of file to backup.
     * @param replicationDegree Number of copies of each chunk to keep around at any time.
     */
    void backup(String filename, int replicationDegree);

    /**
     * Restores backed up file with name {@filename}.
     *
     * @param filename Name of file to restore.
     */
    void restore(String filename);

    /**
     * Deletes file with name {@filename} from the server network.
     *
     * @param filename Name of file to delete.
     */
    void delete(String filename);

    /**
     * Changes space allocated to the backup service to {@spaceReserved} bytes.
     *
     * @param spaceReserved Number of bytes to allocate to the backup service.
     */
    void reclaim(int spaceReserved);

    /**
     * Gives information about the current state of the server.
     *
     * @return Information about the current state of the server.
     */
    String state();
}
