/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.utils.serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * Serialize your tags object to a Map(String,String).
 */
public interface OpenTsdbTagsSerializer extends Serializable {
    /**
     * @param object Object to serialize.
     * @return The tags.
     */
    Map<String, String> serialize(Object object);

    /**
     * <p>
     * Initialize the serializer
     * </p>
     *
     * @param conf Topology configuration
     */
    void prepare(Map conf);
}