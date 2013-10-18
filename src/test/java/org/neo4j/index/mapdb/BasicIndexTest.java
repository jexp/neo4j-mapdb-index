package org.neo4j.index.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.*;
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
        tx = db.beginTx();
        final Iterable<IndexDefinition> indexes = db.schema().getIndexes(LABEL);
        final IndexDefinition index = IteratorUtil.single(indexes);
        assertEquals(LABEL.name(), index.getLabel().name());
        final Node node = db.createNode(LABELS);
        node.setProperty(PROPERTY, 42);
        tx.success();
        tx.finish();

        tx = db.beginTx();
        final ResourceIterable<Node> nodes = db.findNodesByLabelAndProperty(LABEL, PROPERTY, 42);
        assertEquals(node, IteratorUtil.single(nodes));
        tx.success();
        tx.finish();
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
        final String type = propertyValue.from(0).getClass().getSimpleName();
        System.out.println("Creating "+COUNT*RUNS+" nodes with "+getClass().getSimpleName()+" for "+type+" properties took "+time+" ms.");
    }

    @Before
    public void setUp() throws IOException {
        FileUtils.deleteRecursively(new File("test-data"));
        db = new ImpermanentGraphDatabase();
        createIndex();
    }

    protected void createIndex() {
        tx = db.beginTx();
        final IndexDefinition indexDef = db.schema().indexFor(LABEL).on(PROPERTY).create();
        tx.success();
        tx.finish();
        tx = db.beginTx();
        db.schema().awaitIndexOnline( indexDef, 10, TimeUnit.SECONDS );
        tx.success();
        tx.finish();
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
