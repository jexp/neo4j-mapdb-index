# Neo4j 2.0 MapDB Index Provider

Implements a Schema Index Provider for Neo4j 2.0, label based indexes using [MapDB](http://www.mapdb.org/), which is a high performance, persistent map implementation using compression and custom serialization.

It also supports snapshots which are required by a Schema Index Provider for repeatable reads.

`mvn clean install`

That will create a zip-file: `target/mapdb-index-1.0-provider.zip` whose content you have to put in Neo4j's classpath.
