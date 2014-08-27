package storm.opentsdb.trident;

import backtype.storm.task.IMetricsContext;
import storm.opentsdb.trident.mapper.OpenTsdbTridentTupleFieldMapper;
import storm.opentsdb.trident.mapper.OpenTsdbTridentMapper;
import storm.opentsdb.utils.OpenTsdbClientFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class OpenTsdbStateFactory implements StateFactory {
    private final String cluster;
    private final String name;
    private final OpenTsdbTridentMapper mapper;

    public OpenTsdbStateFactory(String cluster, String name, OpenTsdbTridentMapper mapper) {
        this.cluster = cluster;
        this.name = name;
        this.mapper = mapper;
    }

    public OpenTsdbStateFactory(String cluster, String name) {
        this(cluster, name, new OpenTsdbTridentMapper()
            .addFieldMapper(new OpenTsdbTridentTupleFieldMapper("metric", "timestamp", "value", "tags")));
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new OpenTsdbState(OpenTsdbClientFactory
            .getTsdbClient(conf, this.cluster, this.name), this.mapper);
    }
}
