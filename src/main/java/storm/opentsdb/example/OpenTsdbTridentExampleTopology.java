package net.ovh.storm.opentsdb.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import net.ovh.storm.kafka.model.BaseEvent;
import net.ovh.storm.kafka.scheme.GenericEventGsonScheme;
import net.ovh.storm.opentsdb.model.OpenTsdbEvent;
import net.ovh.storm.opentsdb.trident.OpenTsdbValueFactory;
import net.ovh.storm.opentsdb.trident.OpenTsdbValueUpdater;
import net.ovh.storm.opentsdb.trident.mapper.OpenTsdbTridentEventField;
import net.ovh.storm.opentsdb.trident.mapper.SimpleOpenTsdbTridentMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Debug;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class OpenTsdbTridentExampleTopology {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbTridentExampleTopology.class);

    public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {

        /**
         * Extract fields from BaseEvent
         */

        class ExtractFields extends BaseFunction {
            @Override
            public void execute(TridentTuple tuple, TridentCollector collector) {
                BaseEvent event = (BaseEvent) tuple.getValue(0);
                collector.emit(new Values(event.key, event.timestamp, event.value, event.tags));
            }
        }

        /**
         * Kafka Spout
         **/

        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(new ZkHosts("node1:2181"), "test", "storm_" + System.currentTimeMillis());
        kafkaConfig.scheme = new SchemeAsMultiScheme(new GenericEventGsonScheme(OpenTsdbEvent.class));
        TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);

        /**
         * Topology
         */

        TridentTopology topology = new TridentTopology();

        ArrayList<String> validTags = new ArrayList<String>();
        validTags.add("foo");

        Stream stream = topology.newStream("kafka", kafkaSpout)
            .each(new Fields("event"), new Debug())
            .each(new Fields("event"), new ExtractFields(), new Fields("metric", "timestamp", "value", "tags"));

        /**
         * Using FieldSet mapper ( default )
         */
        stream.partitionPersist(
            new OpenTsdbValueFactory("hbase-cluster", "test-tsdb"),
            new Fields("metric", "timestamp", "value", "tags"),
            new OpenTsdbValueUpdater());

        /**
         * Using EventField mapper
         */
        stream.partitionPersist(
            new OpenTsdbValueFactory("hbase-cluster", "test-tsdb",
                new SimpleOpenTsdbTridentMapper()
                    .addFieldMapper(
                        new OpenTsdbTridentEventField("event")
                            .setValidTags(validTags))),
            new Fields("event"),
            new OpenTsdbValueUpdater());


        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);

        Map<String, String> hBaseConfig = new HashMap<>();
        hBaseConfig.put("zkQuorum", "node-00113.hadoop.ovh.net,node-00114.hadoop.ovh.net,node-00116.hadoop.ovh.net");
        conf.put("hbase-cluster", hBaseConfig);

        Map<String, String> openTsdbConfig = new HashMap<>();
        openTsdbConfig.put("tsd.core.auto_create_metrics", "true");
        openTsdbConfig.put("tsd.storage.hbase.data_table", "test_tsdb");
        openTsdbConfig.put("tsd.storage.hbase.uid_table", "test_tsdb-uid");
        conf.put("test-tsdb", openTsdbConfig);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        } else {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("OpenTsdbTridentExampleTopology", conf, buildTopology(drpc));
        }
    }
}
