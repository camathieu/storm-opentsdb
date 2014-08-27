package storm.opentsdb.bolt.mapper;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Map;

public interface IOpenTsdbFieldMapper extends Serializable {
    public String getMetric(Tuple tuple);

    public long getTimestamp(Tuple tuple);

    public double getValue(Tuple tuple);

    public Map<String, String> getTags(Tuple tuple);

    /**
     * <p>
     * Initialize the mapper.
     * </p>
     *
     * @param conf Topology configuration.
     */
    void prepare(Map conf);
}