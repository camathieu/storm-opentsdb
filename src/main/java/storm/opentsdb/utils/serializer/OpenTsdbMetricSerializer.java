/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.utils.serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Serialize your metric object to a string.
 */
public interface OpenTsdbMetricSerializer extends Serializable {
    /**
     * @param object Object to serialize.
     * @return The metric.
     */
    String serialize(Object object);

    /**
     * <p>
     * Initialize the serializer
     * </p>
     *
     * @param conf Topology configuration
     */
    void prepare(Map conf);
}
