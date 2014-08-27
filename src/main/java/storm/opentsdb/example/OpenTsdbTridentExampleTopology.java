package storm.opentsdb.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.asynchbase.example.spout.RandomKeyValueBatchSpout;
import storm.asynchbase.example.spout.RandomKeyValueSpout;
import storm.opentsdb.trident.OpenTsdbStateUpdater;
import storm.opentsdb.trident.mapper.OpenTsdbTridentEventFieldMapper;
import storm.opentsdb.trident.mapper.OpenTsdbTridentMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class OpenTsdbTridentExampleTopology {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbTridentExampleTopology.class);

    public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {

        TridentTopology topology = new TridentTopology();

        Stream stream = topology.newStream("stream", new RandomKeyValueBatchSpout(10).setSleep(1000)).parallelismHint(5);

        stream.each(new Fields("event"), new Debug());

        /**
         * Using Tuple mapper
         */

        stream.partitionPersist(
            new storm.opentsdb.trident.OpenTsdbStateFactory("hbase-cluster", "test-tsdb"),
            new Fields("metric", "timestamp", "value", "tags"),
            new OpenTsdbStateUpdater());

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
