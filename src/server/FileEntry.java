package server;

public class FileEntry {
    private final int desiredReplicationDegree;
    private int currentReplicationDegree = 0;

    public FileEntry(int desiredReplicationDegree) {
        this.desiredReplicationDegree = desiredReplicationDegree;
    }

    public int getDesiredReplicationDegree() {
        return desiredReplicationDegree;
    }

    public synchronized int getCurrentReplicationDegree() {
        return currentReplicationDegree;
    }

    public synchronized void incrementCurrentReplicationDegree() {
        currentReplicationDegree++;
    }

    public synchronized void decrementCurrentReplicationDegree() {
        currentReplicationDegree--;
    }
}
