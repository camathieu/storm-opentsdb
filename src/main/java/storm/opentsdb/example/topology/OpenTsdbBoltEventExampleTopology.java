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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.example.spout.RandomKeyValueSpout;
import storm.opentsdb.bolt.OpenTsdbBolt;
import storm.opentsdb.bolt.mapper.OpenTsdbEventFieldMapper;
import storm.opentsdb.bolt.mapper.OpenTsdbMapper;
import storm.opentsdb.example.bolt.OpenTsdbTupleAdadptatorBolt;
import storm.opentsdb.model.OpenTsdbEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Generate random values every 100ms to the metric OpenTsdbBoltTupleExampleTopology
 * This one uses an event mapper ( the event fields contains an OpenTsdbEvent object )
 */
public class OpenTsdbBoltEventExampleTopology {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbBoltEventExampleTopology.class);

    public static StormTopology buildTopology() {

        class OpenTsdbEventGeneratorBolt extends OpenTsdbTupleAdadptatorBolt {

            OpenTsdbEventGeneratorBolt(String metric, String valueField, Map<String, String> tags) {
                super(metric, valueField, tags);
            }

            @Override
            public void execute(Tuple tuple, BasicOutputCollector collector) {
                long ts = System.currentTimeMillis();
                if (!this.millisec) {
                    ts /= 1000;
                }
                collector.emit(new Values(new OpenTsdbEvent(this.metric, ts, (double) tuple.getValueByField(this.valueField), this.tags)));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("event"));
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

        RandomKeyValueSpout randomKeyValueSpout = new RandomKeyValueSpout().setSleep(100);

        TopologyBuilder builder = new TopologyBuilder();

        Map<String, String> tags = new HashMap<>();
        tags.put("foo", "bar");
        tags.put("plip", "plop");

        ArrayList<String> validTags = new ArrayList<String>();
        validTags.add("foo");

        builder.setSpout("spout", randomKeyValueSpout, 1);
        builder
            .setBolt("events", new OpenTsdbEventGeneratorBolt("OpenTsdbBoltTupleExampleTopology", "value", tags)
                .SetMillisec(true), 1)
            .shuffleGrouping("spout");

        builder
            .setBolt("print", new PrinterBolt(), 1)
            .shuffleGrouping("events");

        builder
            .setBolt("opentsdb",
                new OpenTsdbBolt("hbase-cluster", "test-tsdb",
                    new OpenTsdbMapper()
                        .addFieldMapper(
                            new OpenTsdbEventFieldMapper("event")
                                .setValidTags(validTags)
                        )), 1)
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
