package storm.opentsdb.trident.mapper;

import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OpenTsdbTridentTupleFieldMapper implements IOpenTsdbTridentFieldMapper {
    private String metricField;
    private String timestampField;
    private String valueField;
    private String tagsField;

    private List<String> validTags;

    public OpenTsdbTridentTupleFieldMapper(String metricField, String timestampField, String valueField, String tagsField) {
        this.metricField = metricField;
        this.timestampField = timestampField;
        this.valueField = valueField;
        this.tagsField = tagsField;
    }

    public OpenTsdbTridentTupleFieldMapper setValidTags(List<String> validTags) {
        this.validTags = validTags;
        return this;
    }

    public String getMetric(TridentTuple tuple) {
        return tuple.getStringByField(this.metricField);
    }

    public long getTimestamp(TridentTuple tuple) {
        return tuple.getLongByField(this.timestampField);
    }

    public double getValue(TridentTuple tuple) {
        return tuple.getDoubleByField(this.valueField);
    }

    public Map<String, String> getTags(TridentTuple tuple) {
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

    @Override
    public void prepare(Map conf) {

    }
}
