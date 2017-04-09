package server;

import java.util.Comparator;

class ChunkReplicationComparator implements Comparator<ChunkReplication>
{
    @Override
    public int compare(ChunkReplication o1, ChunkReplication o2) {
        return o2.getReplicationDifference() - o1.getReplicationDifference();
    }
}
