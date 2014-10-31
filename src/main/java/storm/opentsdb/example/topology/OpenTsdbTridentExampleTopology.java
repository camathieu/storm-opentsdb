/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.example.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.example.spout.RandomKeyValueBatchSpout;
import storm.asynchbase.example.trident.operation.StreamRateAggregator;
import storm.opentsdb.example.trident.operation.OpenTsdbTupleAdaptatorFunction;
import storm.opentsdb.trident.OpenTsdbStateFactory;
import storm.opentsdb.trident.OpenTsdbStateUpdater;
import storm.opentsdb.trident.mapper.OpenTsdbTridentMapper;
import storm.opentsdb.trident.mapper.OpenTsdbTridentTupleFieldMapper;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Generate a random key,value stream.
 * Use an aggregator to compute the stream rate and save it to OpenTSDB under metric "steam_rate"
 */
public class OpenTsdbTridentExampleTopology {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbTridentExampleTopology.class);

    public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {

        TridentTopology topology = new TridentTopology();

        Stream stream = topology.newStream("stream", new RandomKeyValueBatchSpout(1000).setSleep(100)).parallelismHint(5);

        TridentState streamRate = stream
            .aggregate(new Fields(), new StreamRateAggregator(2), new Fields("rate"))
            .each(
                new Fields("rate"),
                new OpenTsdbTupleAdaptatorFunction("stream_rate", "rate", new HashMap<String, String>()),
                new Fields("metric", "timestamp", "value", "tags")
            ).partitionPersist(
                new OpenTsdbStateFactory("hbase-cluster", "test-tsdb"),
                new Fields("rate"),
                new OpenTsdbStateUpdater(new OpenTsdbTridentMapper().addFieldMapper(new OpenTsdbTridentTupleFieldMapper()))
            ).parallelismHint(5);

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
