package org.neo4j.index.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestGraphDatabaseFactory;

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

    private static final Label LABEL = DynamicLabel.label("fooint");
    protected static final Label[] LABELS = new Label[]{LABEL};
    protected String PROPERTY = "bar";
    protected static final int COUNT = 100000;
    protected static final int RUNS = 1;
    protected ImpermanentGraphDatabase db;
    protected IndexDefinition indexDefinition;

    @Test
    public void testCreateAddIndex() throws Exception {
        try (Transaction tx = db.beginTx()) {
            final Iterable<IndexDefinition> indexes = db.schema().getIndexes(LABEL);
            final IndexDefinition index = IteratorUtil.single(indexes);
            assertEquals(LABEL.name(), index.getLabel().name());
            tx.success();
        }
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = db.createNode(LABELS);
            node.setProperty(PROPERTY, 42);
            final ResourceIterable<Node> nodes = db.findNodesByLabelAndProperty(LABEL, PROPERTY, 42);
            assertEquals(node, IteratorUtil.single(nodes));
            tx.success();
        }
    }

    @Test
    public void testInsertPerformanceWithIntValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() {
            public Object from(int value) {
                return value;
            }
        }, "int");
    }
    @Test
    public void testInsertPerformanceWithLongValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() {
            public Object from(int value) {
                return (long) value;
            }
        },"long");
    }
    @Test
    public void testInsertPerformanceWithStringValues() throws Exception {
        insertManyNodesWithIndex(new PropertyValue() {
            public Object from(int value) {
                return String.valueOf(value);
            }
        }, "string");
    }

    public void insertManyNodesWithIndex(PropertyValue propertyValue, String ext) throws Exception {
        final Label label = DynamicLabel.label("foo"+ ext);
        final Label[] labels = new Label[]{label};
        long time=System.currentTimeMillis();
        for (int run=0;run<RUNS;run++) {
            try (Transaction tx = db.beginTx()) {
                for (int i = 0; i < COUNT; i++) {
                    final Node node = db.createNode(labels);
                    // todo concurrentmodification exception node.setProperty(PROPERTY, 42);
                    node.setProperty(PROPERTY, propertyValue.from(i));
                }
                tx.success();
            }
        }
        time = System.currentTimeMillis() - time;
        final String type = propertyValue.from(0).getClass().getSimpleName();
        System.out.println("Creating "+COUNT*RUNS+" nodes with "+getClass().getSimpleName()+" for "+type+" properties took "+time+" ms.");
    }

    @Before
    public void setUp() throws IOException {
        FileUtils.deleteRecursively(new File("test-data"));
        db = (ImpermanentGraphDatabase) new TestGraphDatabaseFactory().newImpermanentDatabase();
        createIndex(DynamicLabel.label("fooint"));
        createIndex(DynamicLabel.label("foolong"));
        createIndex(DynamicLabel.label("foostring"));
    }

    protected void createIndex(Label label) {
        try (Transaction tx = db.beginTx()) {
            final IndexCreator indexCreator = db.schema().indexFor(label).on(PROPERTY);
            indexDefinition = indexCreator.create();
            tx.success();
        }
        try (Transaction tx = db.beginTx()) {
            db.schema().awaitIndexOnline(indexDefinition, 5, TimeUnit.SECONDS);
            tx.success();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (indexDefinition==null) return;
        try (Transaction tx = db.beginTx()) {
            indexDefinition.drop();
            tx.success();
        }
    }
}
