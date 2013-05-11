package org.neo4j.index.mapdb;

import org.junit.Ignore;
import org.junit.Test;

/**
 * @author mh
 * @since 06.05.13
 */
public class NoIndexTest extends BasicIndexTest {
    @Override
    protected void createIndex() {
    }

    @Test
    @Ignore
    public void testCreateAddIndex() throws Exception {

    }
}
