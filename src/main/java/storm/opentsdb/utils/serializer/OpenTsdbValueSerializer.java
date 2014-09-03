/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.utils.serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Serialize your value object to a double.
 */
public interface OpenTsdbValueSerializer extends Serializable {
    /**
     * @param object Object to serialize.
     * @return Thev value.
     */
    double serialize(Object object);

    /**
     * <p>
     * Initialize the serializer
     * </p>
     *
     * @param conf Topology configuration
     */
    void prepare(Map conf);
}