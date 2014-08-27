package storm.opentsdb.model;

import java.util.Map;

public class OpenTsdbEvent implements IOpenTsdbEvent {
    public String metric;
    public long timestamp;
    public double value;
    public Map<String, String> tags;

    public OpenTsdbEvent(String metric, long timestamp, double value, Map<String, String> tags) {
        this.metric = metric;
        this.timestamp = timestamp;
        this.value = value;
        this.tags = tags;
    }

    public String getMetric() {
        return this.metric;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public double getValue() {
        return this.value;
    }

    public Map<String, String> getTags() {
        return this.tags;
    }

    @Override
    public String toString() {
        return this.timestamp + " " + this.metric + " " +
            this.value + " " + this.tags.toString();
    }
}