package org.neo4j.index.mapdb.provider;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;

public class MapDbIndex extends IndexAccessor.Adapter implements IndexPopulator, IndexUpdater {

    private final BTreeMap<Object,long[]> indexData;

    private final DB db;

    private InternalIndexState state = InternalIndexState.POPULATING;

    @Override
    public IndexUpdater newUpdater(final IndexUpdateMode mode) {
        return this;
    }

    public MapDbIndex(final BTreeMap<Object,long[]> map, final DB db) {
        this.indexData = map;
        this.db = db;
    }

    public InternalIndexState getState() {
        return this.state;
    }

    @Override
    public void add(final long nodeId, final Object propertyValue) {
        long[] nodes = this.indexData.get(propertyValue);
        if (nodes==null || nodes.length==0) {
            this.indexData.put(propertyValue, new long[]{nodeId});
            return;
        }
        final int idx=this.indexOf(nodes,nodeId);
        if (idx!=-1) return;
        nodes = Arrays.copyOfRange(nodes, 0, nodes.length + 1);
        nodes[nodes.length-1]=nodeId;
        this.indexData.replace(propertyValue, nodes);
    }

    @Override
    public void process(final NodePropertyUpdate update) throws IOException, IndexEntryConflictException {
        switch (update.getUpdateMode()) {
        case ADDED:
            this.add(update.getNodeId(), update.getValueAfter());
            break;
        case CHANGED:
            this.removed(update.getNodeId(), update.getValueBefore());
            this.add(update.getNodeId(), update.getValueAfter());
            break;
        case REMOVED:
            this.removed(update.getNodeId(), update.getValueBefore());
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void remove(final Iterable<Long> nodeIds) throws IOException {

        final Iterator<Map.Entry<Object,long[]>> entries = this.indexData.entrySet().iterator();
        while (entries.hasNext()) {
            final Map.Entry<Object, long[]> entry = entries.next();
            long[] nodes = entry.getValue();
            int existingCount = nodes.length;
            for (final Long nodeId : nodeIds) {
                final int idx = this.indexOf(nodes, nodeId);
                if (idx != -1) {
                    if (existingCount == 1) {
                        entries.remove();
                        break;
                    } else {
                        System.arraycopy(nodes,idx,nodes,idx-1, existingCount-idx-1);
                        nodes = Arrays.copyOfRange(nodes, 0, existingCount - 1);
                        entry.setValue(nodes);
                        existingCount--;
                    }
                }
            }
        }
    }

    private void removed(final long nodeId, final Object propertyValue) {
        long[] nodes = this.indexData.get(propertyValue);
        if (nodes==null || nodes.length ==0) return;
        final int idx=this.indexOf(nodes,nodeId);
        if (idx==-1) return;
        final int existingCount = nodes.length;
        if (existingCount == 1) {
            this.indexData.remove(propertyValue);
            return;
        }
        System.arraycopy(nodes,idx,nodes,idx-1, existingCount-idx-1);
        nodes = Arrays.copyOfRange(nodes, 0, existingCount - 1);
        this.indexData.replace(propertyValue, nodes);
    }

    private int indexOf(final long[] nodes, final long nodeId) {
        for (int i = nodes.length - 1; i != 0; i--) {
            if (nodes[i]==nodeId) return i;
        }
        return -1;
    }

    @Override
    @Deprecated
    public void verifyDeferredConstraints(final PropertyAccessor accessor) throws Exception {
        // constraints are checked in add() so do nothing
    }

    @Override
    public IndexUpdater newPopulatingUpdater(final PropertyAccessor accessor) throws IOException {
        return this;
    }

    @Override
    public void markAsFailed(final String failure) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void force() {
        this.db.commit();
    }

    @Override
    public void create() {
        this.indexData.clear();
        this.db.commit();
    }

    @Override
    public void drop() {
        this.indexData.clear();
        this.db.commit();
    }

    @Override
    public void close(final boolean populationCompletedSuccessfully) {
        if (populationCompletedSuccessfully) {
            this.state = InternalIndexState.ONLINE;
        }
    }

    @Override
    public void close() {
        this.db.commit();
    }

    /**
     * @return a new {@link IndexReader} responsible for looking up results in the index.
     * The returned reader must honor repeatable reads.
     */
    @Override
    public IndexReader newReader() {
        return new MapDbIndexReader((BTreeMap<Object, long[]>) this.indexData.snapshot());
    }
}
