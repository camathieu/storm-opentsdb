/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.model;

import java.util.Map;

/**
 * This represents a basic OpenTSDB put request.
 */
public class OpenTsdbEvent implements IOpenTsdbEvent {
    public String metric;
    public long timestamp;
    public double value;
    public Map<String, String> tags;

    /**
     * @param metric    The metric.
     * @param timestamp The timestamp.
     * @param value     The value.
     * @param tags      The tags.
     */
    public OpenTsdbEvent(String metric, long timestamp, double value, Map<String, String> tags) {
        this.metric = metric;
        this.timestamp = timestamp;
        this.value = value;
        this.tags = tags;
    }

    /**
     * @return The metric.
     */
    public String getMetric() {
        return this.metric;
    }

    /**
     * @return The timestamp.
     */
    public long getTimestamp() {
        return this.timestamp;
    }

    /**
     * @return The value.
     */
    public double getValue() {
        return this.value;
    }

    /**
     * @return The tags.
     */
    public Map<String, String> getTags() {
        return this.tags;
    }

    @Override
    public String toString() {
        return this.timestamp + " " + this.metric + " " +
            this.value + " " + this.tags.toString();
    }
}