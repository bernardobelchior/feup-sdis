package server;

/**
 * Created by bernardo on 3/13/17.
 */
public class ChunkReplicationDegree {
    private int desired;
    public int current = 0;

    ChunkReplicationDegree(int desired) {
       this.desired = desired;
    }
}
