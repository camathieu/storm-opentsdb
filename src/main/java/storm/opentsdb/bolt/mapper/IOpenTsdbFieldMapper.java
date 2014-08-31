/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.bolt.mapper;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Map;

/**
 * <p>
 * This interface describes a mapper that maps
 * a storm tuple to OpenTsdb put request components
 * </p>
 */
public interface IOpenTsdbFieldMapper extends Serializable {
    /**
     * @param tuple The storm tuple to process.
     * @return The metric for the put request.
     */
    public String getMetric(Tuple tuple);

    /**
     * @param tuple The storm tuple to process
     * @return The timestamp for the put request.
     */
    public long getTimestamp(Tuple tuple);

    /**
     * @param tuple The storm tuple to process.
     * @return The value for the put request.
     */
    public double getValue(Tuple tuple);

    /**
     * @param tuple The storm tuple to process.
     * @return The tags for the put request.
     */
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