package server;

public class FileEntry {
    private int desiredReplicationDegree;
    private int currentReplicationDegree = 0;

    public FileEntry(int desiredReplicationDegree) {
        this.desiredReplicationDegree = desiredReplicationDegree;
    }

    public int getDesiredReplicationDegree() {
        return desiredReplicationDegree;
    }

    public int getCurrentReplicationDegree() {
        return currentReplicationDegree;
    }

    public void incrementCurrentReplicationDegree() {
        currentReplicationDegree++;
    }

    public void decrementCurrentReplicationDegree() {
        currentReplicationDegree--;
    }
}
