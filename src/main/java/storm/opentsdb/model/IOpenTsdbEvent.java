package storm.opentsdb.model;

import java.util.Map;

public interface IOpenTsdbEvent {
    String getMetric();

    long getTimestamp();

    double getValue();

    Map<String, String> getTags();
}
