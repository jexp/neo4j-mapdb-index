package org.neo4j.index.mapdb;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;

/**
 * @author mh
 * @since 06.05.13
 */
public class BasicIndexLuceneTest extends BasicIndexTest {
    @Override
    @Before
    public void setUp() {
        MapDbSchemaIndexProvider.PRIORITY = 0;
        super.setUp();
    }

    @Test
    public void testInsertPerformanceLucene() throws Exception {
        long time=System.currentTimeMillis();
        tx = db.beginTx();
        for (int i=0;i<COUNT;i++) {
            final Node node = db.createNode(LABELS);
            node.setProperty(PROPERTY, i);
        }
        tx.success();
        tx.finish();
        time = System.currentTimeMillis() - time;
        System.out.println("Creating "+COUNT+" nodes with lucene index took "+time+" ms.");
    }

}
