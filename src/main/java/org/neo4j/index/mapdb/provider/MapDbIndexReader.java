package org.neo4j.index.mapdb.provider;

import java.util.Set;

import org.mapdb.BTreeMap;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.register.Register;

public class MapDbIndexReader implements IndexReader {
    
    private static final long[] EMPTY_LONGS = new long[0];
    private BTreeMap<Object, long[]> snapshot;

    MapDbIndexReader(final BTreeMap<Object, long[]> snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public PrimitiveLongIterator lookup(final Object value) {
        final long[] result = snapshot.get(value);
        return PrimitiveLongCollections.iterator(result == null || result.length==0 ? EMPTY_LONGS : result);
    }
    
    @Override
    public int getIndexedCount(final long nodeId, final Object propertyValue) {
        final long[] result = snapshot.get(propertyValue);
        return result == null ? 0 : result.length;
    }

    @Override public Set<Class> valueTypesInIndex() {

        // TODO

        return null;
    }

    @Override public long sampleIndex(final Register.DoubleLong.Out result) throws IndexNotFoundKernelException {

        // TODO

        return 0;
    }

    @Override
    public void close() {
        snapshot.close();
        snapshot=null;
    }
}
