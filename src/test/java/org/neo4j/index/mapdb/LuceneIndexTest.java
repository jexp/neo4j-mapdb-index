package org.neo4j.index.mapdb;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;

import java.io.IOException;

/**
 * @author mh
 * @since 06.05.13
 */
public class LuceneIndexTest extends BasicIndexTest {
    @Override
    @Before
    public void setUp() throws IOException {
        MapDbSchemaIndexProvider.PRIORITY = 0;
        super.setUp();
    }
}
