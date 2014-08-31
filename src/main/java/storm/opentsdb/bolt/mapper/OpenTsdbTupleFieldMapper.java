/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.bolt.mapper;

import backtype.storm.tuple.Tuple;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This field mapper maps OpenTSDB fields to
 * the OpenTsdb put params ( metrics / ts / value / tags ).
 * </p>
 * <p>
 * It can clean the tags map to keep only those you need.<br/>
 * As of OpenTSDB v2.1 you still need to have at least one tag
 * so providing an empty map will result in this mapper to add
 * a foo=bar tag to your put.
 * </p>
 * TODO implements serializers
 */
public class OpenTsdbTupleFieldMapper implements IOpenTsdbFieldMapper {
    private final String metricField;
    private final String timestampField;
    private final String valueField;
    private final String tagsField;

    private List<String> validTags;

    /**
     * @param metricField    Metric field name.
     * @param timestampField Timestamp field name.
     * @param valueField     Value field name.
     * @param tagsField      Tags field name.
     */
    public OpenTsdbTupleFieldMapper(String metricField, String timestampField, String valueField, String tagsField) {
        this.metricField = metricField;
        this.timestampField = timestampField;
        this.valueField = valueField;
        this.tagsField = tagsField;
    }

    /**
     * <p>
     * Default constructor with standards fields name
     * metric, timestamp, value, tags.
     * </p>
     */
    public OpenTsdbTupleFieldMapper() {
        this("metric", "timestamp", "value", "tags");
    }

    /**
     * @param validTags The valid tags list.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTupleFieldMapper setValidTags(List<String> validTags) {
        this.validTags = validTags;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return The metric from the OpenTsdbEvent.
     */
    public String getMetric(Tuple tuple) {
        return tuple.getStringByField(this.metricField);
    }

    /**
     * @param tuple The storm tuple to process
     * @return The timestamp from the OpenTsdbEvent.
     */
    public long getTimestamp(Tuple tuple) {
        return tuple.getLongByField(this.timestampField);
    }

    /**
     * @param tuple The storm tuple to process.
     * @return The value from the OpenTsdbEvent.
     */
    public double getValue(Tuple tuple) {
        return tuple.getDoubleByField(this.valueField);
    }

    /**
     * @param tuple The storm tuple to process.
     * @return the tags from the OpenTsdbEvent.
     */
    public Map<String, String> getTags(Tuple tuple) {
        Map<String, String> tags = (Map<String, String>) tuple.getValueByField(this.tagsField);

        if (tags.size() == 0) {
            tags.put("foo", "bar");
        }

        if (this.validTags != null) {
            for (Iterator<Map.Entry<String, String>> it = tags.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, String> tag = it.next();
                if (validTags.contains(tag.getKey())) {
                    if (tag.getValue() == null || tag.getValue().isEmpty()) {
                        it.remove();
                    }
                }
            }
        }

        return tags;
    }

    /**
     * <p>
     * Initialize the mapper.
     * </p>
     *
     * @param conf Topology configuration.
     */
    @Override
    public void prepare(Map conf) {

    }
}
