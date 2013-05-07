package org.neo4j.index.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 06.05.13
 */
public class BasicIndexTest {

    private static final Label LABEL = DynamicLabel.label("foo");
    protected static final Label[] LABELS = new Label[]{LABEL};
    protected static final String PROPERTY = "bar";
    protected static final int COUNT = 20000;
    protected ImpermanentGraphDatabase db;
    protected Transaction tx;
    private IndexDefinition indexDefinition;

    @Test
    public void testCreateAddIndex() throws Exception {
        final ResourceIterable<IndexDefinition> indexes = db.schema().getIndexes(LABEL);
        final IndexDefinition index = IteratorUtil.single(indexes);
        assertEquals(LABEL.name(), index.getLabel().name());
        tx = db.beginTx();
        final Node node = db.createNode(LABELS);
        node.setProperty(PROPERTY, 42);
        tx.success();
        tx.finish();
        final ResourceIterable<Node> nodes = db.findNodesByLabelAndProperty(LABEL, PROPERTY, 42);
        assertEquals(node, IteratorUtil.single(nodes));
    }

    @Test
    public void testInsertPerformance() throws Exception {
        long time=System.currentTimeMillis();
        tx = db.beginTx();
        for (int i=0;i<COUNT;i++) {
            final Node node = db.createNode(LABELS);
            // todo concurrentmodification exception node.setProperty(PROPERTY, 42);
            node.setProperty(PROPERTY, i);
        }
        tx.success();
        tx.finish();
        time = System.currentTimeMillis() - time;
        System.out.println("Creating "+COUNT+" nodes with index took "+time+" ms.");
    }

    @Test
    public void testInsertPerformanceWithoutIndex() throws Exception {
        long time=System.currentTimeMillis();
        tx = db.beginTx();
        for (int i=0;i<COUNT;i++) {
            final Node node = db.createNode();
            node.setProperty(PROPERTY, i);
        }
        tx.success();
        tx.finish();
        time = System.currentTimeMillis() - time;
        System.out.println("Creating "+COUNT+" nodes with index took "+time+" ms.");
    }

    @Before
    public void setUp() {
        db = new ImpermanentGraphDatabase();
        tx = db.beginTx();
        final IndexCreator indexCreator = db.schema().indexCreator(LABEL).on(PROPERTY);
        indexDefinition = indexCreator.create();
        tx.success();
        tx.finish();
        db.schema().awaitIndexOnline(indexDefinition, 5, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() throws Exception {
        tx = db.beginTx();
        indexDefinition.drop();
        tx.success();
        tx.finish();
    }
}
