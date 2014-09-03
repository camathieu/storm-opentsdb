Storm connector for OpenTSDB
============================

This connector for Apache Storm use OpenTSDB java library to 
persist raw data and Trident states directly to HBase using 
AsyncHBase client.
 
As you should have only one AsyncHBase client per application
storm-opentsdb uses the storm-asynchbase client factory to get
a unique instance per cluster.

I suggest you to read the javadoc for all more detailed information.
http://javadoc.root.gg/storm-opentsdb

Usage
-----

Usage example can be found in the storm.opentsdb.example package

 * Client configuration  
You have to register a configuration Map in the topology Config for
each hbase client and for each opentsdb instance you want to use. 

```
    Map<String, String> hBaseConfig = new HashMap<>();
    hBaseConfig.put("zkQuorum", "node1,node2,node3");
    conf.put("hbase-cluster", hBaseConfig);
    Map<String, String> openTsdbConfig = new HashMap<>();
    openTsdbConfig.put("tsd.core.auto_create_metrics", "true");
    openTsdbConfig.put("tsd.storage.hbase.data_table", "test_tsdb");
    openTsdbConfig.put("tsd.storage.hbase.uid_table", "test_tsdb-uid");
    conf.put("test-tsdb", openTsdbConfig);
```

 * Mapper  
To map Storm tuple to OpenTSDB put requests you'll have to provide
some mappers to the bolts, function, states.
You can use method chaining syntax to configure them.
You can map a put parameter to a tuple field or to a
fixed constant value and you can also provide serializers to format 
input values.

For now you have two basic field mapper types, TupleMapper which maps
tuple fields metric, timestamp, value and tags to an OpenTSDB put
request and EventMapper which maps an OpenTsdbEvent to an OpenTSDB put
request. As you can execute more than one put request for a given storm
tuple you have to wrap FieldMappers into Mappers.

```
    OpenTsdb mapper = OpenTsdbMapper mapper = new OpenTsdbMapper()
        .addFieldMapper(
            new OpenTsdbTupleFieldMapper("metric","timestamp","value,"tags)
        .addFieldMapper(
            new OpenTsdbEventFieldMapper("event")
        );
```

 * Bolts
OpenTsdbBolt is used to execute put requests for each incoming tuple, it use
on or more FieldMapper to build the requests from the tuple's fields. All 
requests executed from a tuple are executed in parallel. By default this 
bolt is asynchronous, be sure to read the doc to fully understand what does it
means.

```
    builder
        .setBolt("opentsdb",
            new OpenTsdbBolt("hbase-cluster", "test-tsdb", mapper, 1)
        .shuffleGrouping("events");
```

 * Trident State
This is a TridentState implementation to persist a partition to OpenTSDB.
It should be used with the partition persist method.
You should only use this state if your update is idempotent regarding batch replay. Use the
AsyncHBaseStateUpdater / AsyncHBaseStateQuery and AsyncHBaseStateFactory to interact with it.
You have to provide a mapper.

```
    TridentState streamRate = stream
                .aggregate(new Fields(), new SomeAggregator(2), new Fields("value"))
                .partitionPersist(
                                 new OpenTsdbStateFactory("hbase-cluster", "test-tsdb",mapper),
                                 new Fields("value"),
                                 new OpenTsdbStateUpdater()
                             )
```

TODO
----

 * Handle OpenTSDB query to get data from OpenTSDB to Storm