package storm.opentsdb.trident.mapper;

import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.Map;


public interface IOpenTsdbTridentFieldMapper extends Serializable {
    public String getMetric(TridentTuple tuple);

    public long getTimestamp(TridentTuple tuple);

    public double getValue(TridentTuple tuple);

    public Map<String, String> getTags(TridentTuple tuple);

    /**
     * <p>
     * Initialize the mapper.
     * </p>
     *
     * @param conf Topology configuration.
     */
    void prepare(Map conf);
}