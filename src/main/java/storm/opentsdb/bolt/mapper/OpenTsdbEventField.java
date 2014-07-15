package net.ovh.storm.opentsdb.bolt.mapper;

import backtype.storm.tuple.Tuple;
import net.ovh.storm.opentsdb.model.IOpenTsdbEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OpenTsdbEventField implements OpenTsdbFieldMapper {
    private String eventField;
    private List<String> validTags;

    public OpenTsdbEventField(String eventField) {
        this.eventField = eventField;
    }

    public OpenTsdbEventField setValidTags(List<String> validTags) {
        this.validTags = validTags;
        return this;
    }

    public String getMetric(Tuple tuple) {
        IOpenTsdbEvent event = (IOpenTsdbEvent) tuple.getValueByField(this.eventField);
        return event.getMetric();
    }

    public long getTimestamp(Tuple tuple) {
        IOpenTsdbEvent event = (IOpenTsdbEvent) tuple.getValueByField(this.eventField);
        return event.getTimestamp();
    }

    public double getValue(Tuple tuple) {
        IOpenTsdbEvent event = (IOpenTsdbEvent) tuple.getValueByField(this.eventField);
        return event.getValue();
    }

    public Map<String, String> getTags(Tuple tuple) {
        IOpenTsdbEvent event = (IOpenTsdbEvent) tuple.getValueByField(this.eventField);

        if (this.validTags == null) {
            return event.getTags();
        }

        Map<String, String> eventTags = event.getTags();
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