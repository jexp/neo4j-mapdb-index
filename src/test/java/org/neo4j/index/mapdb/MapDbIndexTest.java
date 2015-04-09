package org.neo4j.index.mapdb;

import org.junit.Before;

import java.io.IOException;

/**
 * @author mh
 * @since 06.05.13
 */
public class MapDbIndexTest extends BasicIndexTest {
    @Override
    @Before
    public void setUp() throws IOException {
//        System.out.println("setting prioty");
//        MapDbSchemaIndexProvider.PRIORITY = 12;
        super.setUp();
    }
}
