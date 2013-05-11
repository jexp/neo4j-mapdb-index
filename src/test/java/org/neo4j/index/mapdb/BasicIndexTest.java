package org.neo4j.index.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 06.05.13
 */
@Ignore
public abstract class BasicIndexTest {

    private static final Label LABEL = DynamicLabel.label("foo");
    protected static final Label[] LABELS = new Label[]{LABEL};
    protected String PROPERTY = "bar";
    protected static final int COUNT = 10000;
    protected static final int RUNS = 10;
    protected ImpermanentGraphDatabase db;
    protected Transaction tx;
    protected IndexDefinition indexDefinition;

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

    interface PropertyValue {
        Object from(int value);
    }
    @Test
    public void testInsertPerformanceWithIntValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() {
            public Object from(int value) {
                return value;
            }
        });
    }
    @Test
    public void testInsertPerformanceWithLongValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() {
            public Object from(int value) {
                return (long) value;
            }
        });
    }
    @Test
    public void testInsertPerformanceWithStringValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() {
            public Object from(int value) {
                return String.valueOf(value);
            }
        });
    }

    public void insertManyNodesWithIndex(PropertyValue propertyValue) throws Exception {
        long time=System.currentTimeMillis();
        for (int run=0;run<RUNS;run++) {
        tx = db.beginTx();
        for (int i=0;i<COUNT;i++) {
            final Node node = db.createNode(LABELS);
            // todo concurrentmodification exception node.setProperty(PROPERTY, 42);
            node.setProperty(PROPERTY, propertyValue.from(i));
        }
        tx.success();
        tx.finish();
        }
        time = System.currentTimeMillis() - time;
        System.out.println("Creating "+COUNT+" nodes with "+getClass().getSimpleName()+" took "+time+" ms.");
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
        System.out.println("Creating "+COUNT*RUNS+" nodes without index took "+time+" ms.");
    }

    @Before
    public void setUp() throws IOException {
        FileUtils.deleteRecursively(new File("test-data"));
        db = new ImpermanentGraphDatabase();
        createIndex();
    }

    protected void createIndex() {
        tx = db.beginTx();
        final IndexCreator indexCreator = db.schema().indexCreator(LABEL).on(PROPERTY);
        indexDefinition = indexCreator.create();
        tx.success();
        tx.finish();
        db.schema().awaitIndexOnline(indexDefinition, 5, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() throws Exception {
        if (indexDefinition==null) return;
        tx = db.beginTx();
        indexDefinition.drop();
        tx.success();
        tx.finish();
    }
}
