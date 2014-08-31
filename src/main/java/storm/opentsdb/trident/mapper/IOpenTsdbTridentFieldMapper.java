/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident.mapper;

import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.Map;

/**
 * <p>
 * This interface describes a mapper that maps
 * a storm tuple to OpenTsdb put request components
 * </p>
 */
public interface IOpenTsdbTridentFieldMapper extends Serializable {
    /**
     * @param tuple The storm tuple to process.
     * @return The metric for the put request.
     */
    public String getMetric(TridentTuple tuple);

    /**
     * @param tuple The storm tuple to process
     * @return The timestamp for the put request.
     */
    public long getTimestamp(TridentTuple tuple);

    /**
     * @param tuple The storm tuple to process.
     * @return The value for the put request.
     */
    public double getValue(TridentTuple tuple);

    /**
     * @param tuple The storm tuple to process.
     * @return The tags for the put request.
     */
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