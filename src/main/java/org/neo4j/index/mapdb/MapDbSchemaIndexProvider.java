package org.neo4j.index.mapdb;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.neo4j.kernel.api.index.*;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.neo4j.index.mapdb.MapDbIndexProviderFactory.PROVIDER_DESCRIPTOR;

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

    public MapDbSchemaIndexProvider(Config config) {
        super(PROVIDER_DESCRIPTOR, PRIORITY);
        db = DBMaker
                .newFileDB(getIndexFile(config))
                .compressionEnable()
                .asyncFlushDelay(1)
                .closeOnJvmShutdown()
                .make();

    }

    private File getIndexFile(Config config) {
        final File directory = getDirectory(config);
        return new File(directory,"mapdb-index-tree.db");
    }

    private File getDirectory(Config config) {
        final File rootDirectory = getRootDirectory(config, PROVIDER_DESCRIPTOR.getKey());
        final File indexDirectory = new File(rootDirectory, PROVIDER_DESCRIPTOR.getVersion());
        if ((indexDirectory.exists() && indexDirectory.isDirectory()) || indexDirectory.mkdirs()) return indexDirectory;
        throw new RuntimeException("Error creating directory "+indexDirectory+" for index "+PROVIDER_DESCRIPTOR);
    }

    @Override
    public void shutdown() throws Throwable {
        super.shutdown();
        db.commit();
        db.close();
    }

    @Override
    public MapDbIndex getOnlineAccessor(long indexId, IndexConfiguration indexConfiguration) {
        MapDbIndex index = indexes.get(indexId);
        if (index == null || index.state != InternalIndexState.ONLINE)
            throw new IllegalStateException("Index " + indexId + " not online yet");
        return index;
    }

    @Override
    public String getPopulationFailure(long l) throws IllegalStateException {
        return null;
    }

    @Override
    public InternalIndexState getInitialState(long indexId) {
        MapDbIndex index = indexes.get(indexId);
        return index != null ? index.state : InternalIndexState.POPULATING;
    }

    @Override
    public IndexPopulator getPopulator(long indexId, IndexConfiguration indexConfiguration) {
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
            indexData.replace(propertyValue, nodes);
        }

        private void removed(long nodeId, Object propertyValue) {
            long[] nodes = indexData.get(propertyValue);
            if (nodes==null || nodes.length ==0) return;
            int idx=indexOf(nodes,nodeId);
            if (idx==-1) return;
            final int existingCount = nodes.length;
            if (existingCount == 1) {
                indexData.remove(propertyValue);
                return;
            }
            System.arraycopy(nodes,idx,nodes,idx-1, existingCount-idx-1);
            nodes = Arrays.copyOfRange(nodes, 0, existingCount - 1);
            indexData.replace(propertyValue, nodes);
        }

        private int indexOf(long[] nodes, long nodeId) {
            for (int i = nodes.length - 1; i != 0; i--) {
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
        public void markAsFailed(String s) throws IOException {
        }

        @Override
        public void close() {
            db.commit();
        }

        /**
         * @return a new {@link IndexReader} responsible for looking up results in the index.
         * The returned reader must honor repeatable reads.
         */
        @Override
        public IndexReader newReader() {
            return new MapDbIndexReader((BTreeMap<Object, long[]>) indexData.snapshot());
        }
    }

    private static class MapDbIndexReader implements IndexReader {
        private BTreeMap<Object, long[]> snapshot;

        MapDbIndexReader(BTreeMap<Object, long[]> snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public PrimitiveLongIterator lookup(Object value) {
            final long[] result = snapshot.get(value);
            return new PrimitiveLongIterator() {
                int idx=0;
                private final int length = result.length;

                @Override
                public boolean hasNext() {
                    return idx < length;
                }

                @Override
                public long next() {
                    return result[idx++];
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
