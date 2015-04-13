package org.neo4j.index.mapdb;

import static org.neo4j.index.mapdb.MapDbIndexProviderFactory.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.store.StoreVersionMismatchHandler.ALLOW_OLD_VERSION;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.neo4j.index.mapdb.provider.MapDbIndex;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.SchemaIndexMigrator;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * @author mh
 * @author tschweer
 * @author zazi
 * @since 01.12.14
 */
public class MapDbSchemaIndexProvider extends SchemaIndexProvider {
    static int PRIORITY;
    static {
        PRIORITY = 2;
    }
    // todo this is visibility isolation semantics for the in-memory index
    private final Map<Long, MapDbIndex> indexes = new CopyOnWriteHashMap<>();
    private final DB db;

    public MapDbSchemaIndexProvider(final Config config) {
        super(PROVIDER_DESCRIPTOR, PRIORITY);
        this.db = DBMaker
                .newFileDB(this.getIndexFile(config))
                .compressionEnable()
                .snapshotEnable()
                .asyncWriteFlushDelay(1)
                .closeOnJvmShutdown()
                .make();

    }

    private File getIndexFile(final Config config) {
        final File directory = this.getDirectory(config);
        return new File(directory, "mapdb-index-tree.db");
    }

    private File getDirectory(final Config config) {
        final File rootDirectory = this.getRootDirectory(config, PROVIDER_DESCRIPTOR.getKey());
        final File indexDirectory = new File(rootDirectory, PROVIDER_DESCRIPTOR.getVersion());
        if ((indexDirectory.exists() && indexDirectory.isDirectory()) || indexDirectory.mkdirs())
            return indexDirectory;
        throw new RuntimeException("Error creating directory " + indexDirectory + " for index " + PROVIDER_DESCRIPTOR);
    }

    @Override
    public void shutdown() throws Throwable {
        super.shutdown();
        this.db.commit();
        this.db.close();
    }

    @Override
    public String getPopulationFailure(final long indexId) throws IllegalStateException {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public IndexAccessor getOnlineAccessor(final long indexId, final IndexConfiguration config, final IndexSamplingConfig samplingConfig) throws IOException {
        final MapDbIndex index = this.indexes.get(indexId);
        if (index == null || index.getState() != InternalIndexState.ONLINE)
            throw new IllegalStateException("Index " + indexId + " not online yet");
        return index;
    }

    @Override
    public InternalIndexState getInitialState(final long indexId) {
        final MapDbIndex index = this.indexes.get(indexId);
        return index != null ? index.getState() : InternalIndexState.POPULATING;
    }

    @Override public StoreMigrationParticipant storeMigrationParticipant(final FileSystemAbstraction fs, final UpgradableDatabase upgradableDatabase) {

        // taken from org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider#storeMigrationParticipant
        return new SchemaIndexMigrator( fs, upgradableDatabase, new SchemaIndexMigrator.SchemaStoreProvider()
        {
            @Override
            public SchemaStore provide( File dir, PageCache pageCache )
            {
                return new StoreFactory( fs, dir, pageCache, DEV_NULL, new Monitors(), ALLOW_OLD_VERSION ).newSchemaStore();
            }
        } );
    }

    @Override
    public IndexPopulator getPopulator(final long indexId, final IndexDescriptor descriptor, final IndexConfiguration config,
            final IndexSamplingConfig samplingConfig) {
        final BTreeMap<Object,long[]> map = this.db.getTreeMap(String.valueOf(indexId));
        final MapDbIndex index = new MapDbIndex(map,this.db);
        this.indexes.put(indexId, index);
        return index;
    }
}
