package net.ovh.storm.opentsdb.trident;

import backtype.storm.task.IMetricsContext;
import net.ovh.storm.opentsdb.trident.mapper.OpenTsdbTridentFieldSet;
import net.ovh.storm.opentsdb.trident.mapper.OpenTsdbTridentMapper;
import net.ovh.storm.opentsdb.trident.mapper.SimpleOpenTsdbTridentMapper;
import net.ovh.storm.opentsdb.utils.OpenTsdbClientFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class OpenTsdbValueFactory implements StateFactory {
    private final String cluster;
    private final String name;
    private final OpenTsdbTridentMapper mapper;

    public OpenTsdbValueFactory(String cluster, String name, OpenTsdbTridentMapper mapper) {
        this.cluster = cluster;
        this.name = name;
        this.mapper = mapper;
    }

    public OpenTsdbValueFactory(String cluster, String name) {
        this(cluster, name, new SimpleOpenTsdbTridentMapper()
            .addFieldMapper(new OpenTsdbTridentFieldSet("metric", "timestamp", "value", "tags")));
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new OpenTsdbValueState(OpenTsdbClientFactory
            .getTsdbClient(conf, this.cluster, this.name), this.mapper);
    }
}
