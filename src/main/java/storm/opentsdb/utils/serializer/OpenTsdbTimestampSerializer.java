/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.utils.serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Serialize your timestamp object to a long.
 */
public interface OpenTsdbTimestampSerializer extends Serializable {
    /**
     * @param object Object to serialize.
     * @return The timestamp.
     */
    long serialize(Object object);

    /**
     * <p>
     * Initialize the serializer
     * </p>
     *
     * @param conf Topology configuration
     */
    void prepare(Map conf);
}