package org.neo4j.index.mapdb;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;

@Service.Implementation(KernelExtensionFactory.class)
public class MapDbIndexProviderFactory extends KernelExtensionFactory<MapDbIndexProviderFactory.Dependencies> {
    public static final String KEY = "mapdb-index";

    public static final SchemaIndexProvider.Descriptor PROVIDER_DESCRIPTOR =
            new SchemaIndexProvider.Descriptor(KEY, "1.0");

    private final MapDbSchemaIndexProvider singleProvider;

    public interface Dependencies {
        Config getConfig();
    }

    public MapDbIndexProviderFactory() {
        this(null);
    }

    public MapDbIndexProviderFactory(MapDbSchemaIndexProvider singleProvider) {
        super(KEY);
        this.singleProvider = singleProvider;
    }

    @Override
    public Lifecycle newKernelExtension(Dependencies dependencies) throws Throwable {
        return hasSingleProvider() ? singleProvider : new MapDbSchemaIndexProvider(dependencies.getConfig());
    }

    private boolean hasSingleProvider() {
        return singleProvider != null;
    }
}
