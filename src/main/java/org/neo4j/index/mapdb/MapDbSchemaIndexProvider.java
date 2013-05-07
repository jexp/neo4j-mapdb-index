package org.neo4j.index.mapdb;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.index.*;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @author mh
 * @since 03.05.13
 */
public class MapDbSchemaIndexProvider extends SchemaIndexProvider {
    static int PRIORITY;
    static {
        PRIORITY = 2;
    }
    // todo this is visibility isolation semantics for the in-memory index
    private final Map<Long, MapDbIndex> indexes = new CopyOnWriteHashMap<Long, MapDbIndex>();
    private final DB db;

    public MapDbSchemaIndexProvider() {
        super(MapDbIndexProviderFactory.PROVIDER_DESCRIPTOR, PRIORITY);
        db = DBMaker
                .newFileDB(new File("mapdb-index")) // todo
                .compressionEnable()
                .closeOnJvmShutdown()
                .make();

    }

    @Override
    public void shutdown() throws Throwable {
        super.shutdown();
        db.commit();
        db.close();
    }

    @Override
    public MapDbIndex getOnlineAccessor(long indexId) {
        MapDbIndex index = indexes.get(indexId);
        if (index == null || index.state != InternalIndexState.ONLINE)
            throw new IllegalStateException("Index " + indexId + " not online yet");
        return index;
    }

    @Override
    public InternalIndexState getInitialState(long indexId) {
        MapDbIndex index = indexes.get(indexId);
        return index != null ? index.state : InternalIndexState.POPULATING;
    }

    @Override
    public MapDbIndex getPopulator(long indexId) {
        BTreeMap<Object,long[]> map = db.getTreeMap(String.valueOf(indexId));
        MapDbIndex index = new MapDbIndex(map,db);
        indexes.put(indexId, index);
        return index;
    }

    public static class MapDbIndex extends IndexAccessor.Adapter implements IndexPopulator {
        private final BTreeMap<Object,long[]> indexData;
        private final DB db;
        private InternalIndexState state = InternalIndexState.POPULATING;

        public MapDbIndex(BTreeMap<Object,long[]> map, DB db) {
            this.indexData = map;
            this.db = db;
        }

        @Override
        public void add(long nodeId, Object propertyValue) {
            long[] nodes = indexData.get(propertyValue);
            if (nodes==null || nodes.length==0) {
                indexData.put(propertyValue, new long[]{nodeId});
                return;
            }
            int idx=indexOf(nodes,nodeId);
            if (idx!=-1) return;
            nodes = Arrays.copyOfRange(nodes, 0, nodes.length + 1);
            nodes[nodes.length-1]=nodeId;
            indexData.put(propertyValue, nodes);
        }

        private void removed(long nodeId, Object propertyValue) {
            long[] nodes = indexData.get(propertyValue);
            if (nodes==null || nodes.length==0) return;
            int idx=indexOf(nodes,nodeId);
            if (idx==-1) return;
            System.arraycopy(nodes,idx,nodes,idx-1,nodes.length-idx-1);
            nodes = Arrays.copyOfRange(nodes, 0, nodes.length - 1);
            indexData.put(propertyValue, nodes);
        }

        private int indexOf(long[] nodes, long nodeId) {
            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i]==nodeId) return i;
            }
            return -1;
        }


        @Override
        public void update(Iterable<NodePropertyUpdate> updates) {
            for (NodePropertyUpdate update : updates) {
                switch (update.getUpdateMode()) {
                    case ADDED:
                        add(update.getNodeId(), update.getValueAfter());
                        break;
                    case CHANGED:
                        removed(update.getNodeId(), update.getValueBefore());
                        add(update.getNodeId(), update.getValueAfter());
                        break;
                    case REMOVED:
                        removed(update.getNodeId(), update.getValueBefore());
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            db.commit();
        }

        @Override
        public void updateAndCommit(Iterable<NodePropertyUpdate> updates) {
            update(updates);
        }

        @Override
        public void recover(Iterable<NodePropertyUpdate> updates) throws IOException {
            update(updates);
        }

        @Override
        public void force() {
            db.commit();
        }

        @Override
        public void create() {
            indexData.clear();
            db.commit();
        }

        @Override
        public void drop() {
            indexData.clear();
            db.commit();
        }

        @Override
        public void close(boolean populationCompletedSuccessfully) {
            if (populationCompletedSuccessfully)
                state = InternalIndexState.ONLINE;
        }

        @Override
        public void close() {
            db.commit();
        }

        /**
         * @return a new {@link IndexReader} responsible for looking up results in the index.
         * TODO The returned reader must honor repeatable reads.
         */
        @Override
        public IndexReader newReader() {
            return new MapDbMemoryReader((BTreeMap<Object, long[]>) indexData.snapshot());
        }
    }

    private static class MapDbMemoryReader implements IndexReader {
        private BTreeMap<Object, long[]> snapshot;

        MapDbMemoryReader(BTreeMap<Object, long[]> snapshot) {
            // todo this is repeatable read semantics for the in-memory index
            // todo this.indexData = new HashMap<Object, List<Long>>(indexData);
            this.snapshot = snapshot;
        }

        @Override
        public Iterator<Long> lookup(Object value) {
            final long[] result = snapshot.get(value);
            return result == null || result.length==0 ? IteratorUtil.<Long>emptyIterator() : new Iterator<Long>() {
                int idx=0;
                private final int length = result.length;

                @Override
                public boolean hasNext() {
                    return idx < length;
                }

                @Override
                public Long next() {
                    return result[idx++];
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public void close() {
            snapshot.close();
            snapshot=null;
        }
    }
}
