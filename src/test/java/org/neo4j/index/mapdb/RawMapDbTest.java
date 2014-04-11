package org.neo4j.index.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.neo4j.kernel.impl.util.FileUtils;

import java.io.File;
import java.util.Comparator;

public class RawMapDbTest {

    protected static final int COUNT = 10000;
    protected static final int RUNS = 100;
    private BTreeMap<Object, long[]> map;
    private DB db;

    @Before
    public void setUp() throws Exception {
        File directory = new File("target/map.db");
        directory.mkdirs();
        FileUtils.deleteRecursively(directory);
        db = DBMaker
                .newFileDB(directory)
                .compressionEnable().asyncWriteFlushDelay(0)

                .closeOnJvmShutdown()
                .make();

//        map = db.getTreeMap("test");
        final Comparator<Object> comparator = null;
        final BTreeKeySerializer<Object> keySerializer = null;
        final Serializer<long[]> valueSerializer = null;
        map = db.createTreeMap("test").comparator(comparator).keySerializer(keySerializer).valueSerializer(valueSerializer)
                .counterEnable().nodeSize(64).make();
    }

    @After
    public void tearDown() throws Exception {
        map.clear();
        db.close();
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
        long time = System.currentTimeMillis();
        for (int run = 0; run < RUNS; run++) {
            for (int i = 0; i < COUNT; i++) {
                map.put(propertyValue.from(i), new long[]{i});
            }
            db.commit();
        }
        time = System.currentTimeMillis() - time;
        final String type = propertyValue.from(0).getClass().getSimpleName();
        System.out.println("Creating " + COUNT * RUNS + " nodes with " + getClass().getSimpleName() + " for " + type + " properties took " + time + " ms.");
    }

}
