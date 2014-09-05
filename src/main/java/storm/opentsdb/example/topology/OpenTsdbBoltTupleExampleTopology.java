/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.example.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.example.spout.RandomKeyValueSpout;
import storm.opentsdb.bolt.OpenTsdbBolt;
import storm.opentsdb.example.bolt.OpenTsdbTupleAdadptatorBolt;

import java.util.HashMap;
import java.util.Map;

/**
 * Generate random values every 100ms to the metric OpenTsdbBoltTupleExampleTopology
 * This uses a tuple mapper ( the event fields contains an OpenTsdbEvent object )
 */
public class OpenTsdbBoltTupleExampleTopology {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbBoltTupleExampleTopology.class);

    public static StormTopology buildTopology() {

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
         * Random KeyValue generator
         **/

        RandomKeyValueSpout randomKeyValueSpout = new RandomKeyValueSpout().setSleep(100);

        /**
         * Topology
         */

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", randomKeyValueSpout, 1);
        builder
            .setBolt("events", new OpenTsdbTupleAdadptatorBolt("OpenTsdbBoltTupleExampleTopology", "value", new HashMap<String, String>())
                .SetMillisec(true), 1)
            .shuffleGrouping("spout");

        builder
            .setBolt("print", new PrinterBolt(), 1)
            .shuffleGrouping("events");

        // Automaticaly create default mapper to fields metric, timestamp, value and tags
        builder
            .setBolt("opentsdb", new OpenTsdbBolt("hbase-cluster", "test-tsdb"), 1)
            .shuffleGrouping("events");

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
