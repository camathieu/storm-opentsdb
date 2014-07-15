package net.ovh.storm.opentsdb.model;

import java.util.Map;

public class OpenTsdbEvent implements IOpenTsdbEvent {
    public long timestamp;
    public String key;
    public double value;
    public Map<String, String> tags;

    public String getMetric() {
        return this.key;
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
        return this.timestamp + " " + this.key + " " +
            this.value + " " + this.tags.toString();
    }
}