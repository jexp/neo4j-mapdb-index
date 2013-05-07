package org.neo4j.index.mapdb;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.Iterator;

/**
 * @author mh
 * @since 06.05.13
 */
public class NodeRelationships {
    private static final RelationshipBlock[] EMPTY_RELATIONSHIP_BLOCKS = new RelationshipBlock[0];
    long nodeId;
    RelationshipBlock[] blocks = EMPTY_RELATIONSHIP_BLOCKS;
    static class RelationshipBlock {
        boolean outgoing;
        int relType;
        // 80% occupancy in the beginning
        // gc-like structure
        LongBlock otherNodeBlock;
        LongBlock propertyRecordBlock;
    }
    static class PropertyRelationshipBlock extends RelationshipBlock {
        int lookupPropertyId; // optional via 2 NodeRelationship - structures
        LongBlock lookupPropertyBlock;
        LongBlock toOtherNodePageIds;
    }
    interface LongBlock {}

    // push down methods via builder, shortcut as early as possible, e.g. when there is no RelBlock for the dir, type combo
    RelationshipLookup relationships() {
        return null;
    }

    interface RelationshipLookup {
         RelationshipLookup incoming();
         RelationshipLookup outgoing();
         RelationshipLookup withType(RelationshipType type);
         RelationshipLookup withType(String type);
         RelationshipLookup on(String property);
         RelationshipLookup on(String property,Object value);
         RelationshipLookup on(String property,Object from, Object to);
         RelationshipLookup single();

        boolean exist();
        Iterator<Relationship> get();
    }
}
