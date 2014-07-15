package net.ovh.storm.opentsdb.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.ovh.storm.kafka.scheme.GenericEventGsonScheme;
import net.ovh.storm.opentsdb.bolt.OpenTsdbBolt;
import net.ovh.storm.opentsdb.bolt.mapper.OpenTsdbEventField;
import net.ovh.storm.opentsdb.bolt.mapper.SimpleOpenTsdbMapper;
import net.ovh.storm.opentsdb.model.OpenTsdbEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class OpenTsdbBoltExampleTopology {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbBoltExampleTopology.class);

    public static StormTopology buildTopology() {
        class ExtractFields extends BaseBasicBolt {
            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("metric", "timestamp", "value", "tags"));
            }

            @Override
            public void execute(Tuple tuple, BasicOutputCollector collector) {
                OpenTsdbEvent event = (OpenTsdbEvent) tuple.getValue(0);
                collector.emit(new Values(event.key, event.timestamp, event.value, event.tags));
            }
        }

        class PrinterBolt extends BaseBasicBolt {
            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }

            @Override
            public void execute(Tuple tuple, BasicOutputCollector collector) {
                log.info(tuple.toString());
            }
        }

        /**
         * Kafka Spout
         **/

        SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts("node1:2181"), "test", "", "storm_" + System.currentTimeMillis());
        kafkaConfig.scheme = new SchemeAsMultiScheme(new GenericEventGsonScheme(OpenTsdbEvent.class));
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

        /**
         * Topology
         */

        TopologyBuilder builder = new TopologyBuilder();

        ArrayList<String> validTags = new ArrayList<String>();
        validTags.add("foo");

        builder.setSpout("spout", kafkaSpout, 1);
        builder
            .setBolt("print", new PrinterBolt(), 1)
            .shuffleGrouping("spout");

        builder
            .setBolt("extract", new ExtractFields(), 1).shuffleGrouping("spout")
            .shuffleGrouping("spout");

        builder
            .setBolt("opentsdb-fieldset", new OpenTsdbBolt("hbase-cluster", "test-tsdb"), 1)
            .shuffleGrouping("extract");

        builder
            .setBolt("opentsdb-eventfield",
                new OpenTsdbBolt("hbase-cluster", "test-tsdb",
                    new SimpleOpenTsdbMapper()
                        .addFieldMapper(
                            new OpenTsdbEventField("event")
                                .setValidTags(validTags)
                        )), 1)
            .shuffleGrouping("spout");


        return builder.createTopology();
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
            StormSubmitter.submitTopology(args[0], conf, buildTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("OpenTsdbBoltExampleTopology", conf, buildTopology());
        }
    }
}
