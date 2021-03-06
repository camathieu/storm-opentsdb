/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident.light;

import backtype.storm.task.IMetricsContext;
import storm.opentsdb.trident.mapper.IOpenTsdbTridentFieldMapper;
import storm.opentsdb.trident.mapper.OpenTsdbTridentTupleFieldMapper;
import storm.opentsdb.utils.OpenTsdbClientFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Factory to handle creation of OpenTsdbState objects
 */
public class OpenTsdbLightStateFactory implements StateFactory {
    private final String cluster;
    private final String name;
    private final IOpenTsdbTridentFieldMapper mapper;

    /**
     * @param cluster The HBase cluster to use.
     * @param name    The OpenTSDB instance to use.
     * @param mapper  A mapper to map trident tuple to OpenTSDB put request.
     */
    public OpenTsdbLightStateFactory(String cluster, String name, IOpenTsdbTridentFieldMapper mapper) {
        this.cluster = cluster;
        this.name = name;
        this.mapper = mapper;
    }

    /**
     * @param cluster The HBase cluster to use.
     * @param name    The OpenTSDB instance to use.
     */
    public OpenTsdbLightStateFactory(String cluster, String name) {
        this(cluster, name, new OpenTsdbTridentTupleFieldMapper());
    }

    /**
     * <p>
     * Factory method to create a OpenTsdbState object
     * </p>
     *
     * @param conf           topology configuration.
     * @param metrics        metrics helper.
     * @param partitionIndex partition index handled by this state.
     * @param numPartitions  number of partition to handle.
     * @return An initialized OpenTsdbState.
     */
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new OpenTsdbLightState(OpenTsdbClientFactory
            .getTsdbClient(conf, this.cluster, this.name), this.mapper);
    }
}
