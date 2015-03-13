package org.neo4j.index.mapdb;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Label;

/**
 * @author mh
 * @since 06.05.13
 */
public class NoIndexTest extends BasicIndexTest {
    @Override
    protected void createIndex(Label l) {
    }

    @Test
    @Ignore
    public void testCreateAddIndex() throws Exception {

    }
}
