package org.neo4j.index.mapdb.provider;

import org.mapdb.BTreeMap;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.index.IndexReader;

public class MapDbIndexReader implements IndexReader {
    
    private static final long[] EMPTY_LONGS = new long[0];
    private BTreeMap<Object, long[]> snapshot;

    MapDbIndexReader(BTreeMap<Object, long[]> snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public PrimitiveLongIterator lookup(Object value) {
        final long[] result = snapshot.get(value);
        return PrimitiveLongCollections.iterator(result == null || result.length==0 ? EMPTY_LONGS : result);
    }
    
    @Override
    public int getIndexedCount(long nodeId, Object propertyValue) {
        final long[] result = snapshot.get(propertyValue);
        return result == null ? 0 : result.length;
    }        

    @Override
    public void close() {
        snapshot.close();
        snapshot=null;
    }
}
