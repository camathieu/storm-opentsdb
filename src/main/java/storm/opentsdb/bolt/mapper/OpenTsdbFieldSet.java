package net.ovh.storm.opentsdb.bolt.mapper;

import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OpenTsdbFieldSet implements OpenTsdbFieldMapper {
    private final String metricField;
    private final String timestampField;
    private final String valueField;
    private final String tagsField;

    private List<String> validTags;

    public OpenTsdbFieldSet(String metricField, String timestampField, String valueField, String tagsField) {
        this.metricField = metricField;
        this.timestampField = timestampField;
        this.valueField = valueField;
        this.tagsField = tagsField;
    }

    public OpenTsdbFieldSet setValidTags(List<String> validTags) {
        this.validTags = validTags;
        return this;
    }

    public String getMetric(Tuple tuple) {
        return tuple.getStringByField(this.metricField);
    }

    public long getTimestamp(Tuple tuple) {
        return tuple.getLongByField(this.timestampField);
    }

    public double getValue(Tuple tuple) {
        return tuple.getDoubleByField(this.valueField);
    }

    public Map<String, String> getTags(Tuple tuple) {
        if (this.validTags == null) {
            return (Map<String, String>) tuple.getValueByField(this.tagsField);
        }

        Map<String, String> eventTags = (Map<String, String>) tuple.getValueByField(this.tagsField);
        Map<String, String> tags = new HashMap<>();
        for (String tagk : eventTags.keySet()) {
            if (validTags.contains(tagk)) {
                String tagv = eventTags.get(tagk);
                if (tagv != null && !tagv.isEmpty()) {
                    tags.put(tagk, eventTags.get(tagk));
                }
            }
        }

        if (tags.size() == 0) {
            tags.put("foo", "bar");
        }

        return tags;
    }
}
