/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.model;

import java.util.Map;

/**
 * This represents a basic OpenTSDB put request.
 */
public interface IOpenTsdbEvent {
    /**
     * @return The metric.
     */
    String getMetric();

    /**
     * @return The timestamp.
     */
    long getTimestamp();

    /**
     * @return The value.
     */
    double getValue();

    /**
     * @return The tags.
     */
    Map<String, String> getTags();
}
