package net.ovh.storm.opentsdb.trident.mapper;

import net.ovh.storm.opentsdb.model.IOpenTsdbEvent;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OpenTsdbTridentEventField implements OpenTsdbTridentFieldMapper {
    private String eventField;
    private List<String> validTags;

    public OpenTsdbTridentEventField(String eventField) {
        this.eventField = eventField;
    }

    public OpenTsdbTridentEventField setValidTags(List<String> validTags) {
        this.validTags = validTags;
        return this;
    }

    public String getMetric(TridentTuple tuple) {
        IOpenTsdbEvent event = (IOpenTsdbEvent) tuple.getValueByField(this.eventField);
        return event.getMetric();
    }

    public long getTimestamp(TridentTuple tuple) {
        IOpenTsdbEvent event = (IOpenTsdbEvent) tuple.getValueByField(this.eventField);
        return event.getTimestamp();
    }

    public double getValue(TridentTuple tuple) {
        IOpenTsdbEvent event = (IOpenTsdbEvent) tuple.getValueByField(this.eventField);
        return event.getValue();
    }

    public Map<String, String> getTags(TridentTuple tuple) {
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