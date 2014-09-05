/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.opentsdb.utils.serializer.OpenTsdbMetricSerializer;
import storm.opentsdb.utils.serializer.OpenTsdbTagsSerializer;
import storm.opentsdb.utils.serializer.OpenTsdbTimestampSerializer;
import storm.opentsdb.utils.serializer.OpenTsdbValueSerializer;
import storm.trident.tuple.TridentTuple;

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
 */
public class OpenTsdbTridentTupleFieldMapper implements IOpenTsdbTridentFieldMapper {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbTridentTupleFieldMapper.class);

    private String metric;
    private String metricField;
    private OpenTsdbMetricSerializer metricSerializer;

    private Long timestamp;
    private String timestampField;
    private OpenTsdbTimestampSerializer timestampSerializer;

    private Double value;
    private String valueField;
    private OpenTsdbValueSerializer valueSerializer;

    private Map<String, String> tags;
    private String tagsField;
    private OpenTsdbTagsSerializer tagsSerializer;

    private List<String> validTags;

    /**
     * @param metricField    Metric field name.
     * @param timestampField Timestamp field name.
     * @param valueField     Value field name.
     * @param tagsField      Tags field name.
     */
    public OpenTsdbTridentTupleFieldMapper(String metricField, String timestampField, String valueField, String tagsField) {
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
    public OpenTsdbTridentTupleFieldMapper() {
        this("metric", "timestamp", "value", "tags");
    }

    /**
     * @param validTags The valid tags list.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setValidTags(List<String> validTags) {
        this.validTags = validTags;
        return this;
    }

    /**
     * @param metric The metric to set.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setMetric(Object metric) {
        if (this.metricSerializer != null) {
            this.metric = this.metricSerializer.serialize(metric);
        } else {
            this.metric = (String) metric;
        }
        return this;
    }

    /**
     * @param metricField Name of the tuple field containing the metric to use.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setMetricField(String metricField) {
        this.metricField = metricField;
        return this;
    }

    /**
     * <p>
     * Note that if you use a constant value ( setMetric )
     * you have to provide the serializer before so that the serialization
     * is done only once.
     * </p>
     *
     * @param metricSerializer The metric serializer to use.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setMetricSerializer(OpenTsdbMetricSerializer metricSerializer) {
        this.metricSerializer = metricSerializer;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return The metric to use.
     */
    public String getMetric(TridentTuple tuple) {
        if (this.metric != null) {
            return this.metric;
        }
        if (this.metricSerializer != null) {
            return this.metricSerializer.serialize(tuple.getValueByField(this.metricField));
        }
        return tuple.getStringByField(this.metricField);
    }

    /**
     * @param timestamp The timestamp to set.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setTimestamp(Object timestamp) {
        if (this.timestampSerializer != null) {
            this.timestamp = this.timestampSerializer.serialize(timestamp);
        } else {
            this.timestamp = (Long) timestamp;
        }
        return this;
    }

    /**
     * @param timestampField Name of the tuple field containing the timestamp to use.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setTimestampField(String timestampField) {
        this.timestampField = timestampField;
        return this;
    }

    /**
     * <p>
     * Note that if you use a constant value ( setTimestamp )
     * you have to provide the serializer before so that the serialization
     * is done only once.
     * </p>
     *
     * @param timestampSerializer The timestamp serializer to use.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setTimestampSerializer(OpenTsdbTimestampSerializer timestampSerializer) {
        this.timestampSerializer = timestampSerializer;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return The timestamp to use.
     */
    public long getTimestamp(TridentTuple tuple) {
        if (this.timestamp != null) {
            return this.timestamp;
        }
        if (this.timestampSerializer != null) {
            return this.timestampSerializer.serialize(tuple.getValueByField(this.timestampField));
        }
        return tuple.getLongByField(this.timestampField);
    }

    /**
     * @param value The value to set.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setValue(Object value) {
        if (this.valueSerializer != null) {
            this.value = this.valueSerializer.serialize(value);
        } else {
            this.value = (Double) value;
        }
        if (value instanceof Short) {
            this.value = new Double((Short) value);
        } else if (value instanceof Integer) {
            this.value = new Double((Integer) value);
        } else if (value instanceof Long) {
            this.value = new Double((Long) value);
        } else if (value instanceof Float) {
            this.value = new Double((Float) value);
        } else if (value instanceof Double) {
            this.value = (Double) value;
        } else {
            throw new IllegalArgumentException("Invalid value type");
        }
        return this;
    }

    /**
     * @param valueField Name of the tuple field containing the value to use.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setValueField(String valueField) {
        this.valueField = valueField;
        return this;
    }

    /**
     * <p>
     * Note that if you use a constant value ( setValue )
     * you have to provide the serializer before so that the serialization
     * is done only once.
     * </p>
     *
     * @param valueSerializer The value serializer to use.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setValueSerializer(OpenTsdbValueSerializer valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return The value to use.
     */
    public double getValue(TridentTuple tuple) {
        if( this.value != null) {
            return this.value;
        }
        if (this.valueSerializer != null) {
            return this.valueSerializer.serialize(tuple.getValueByField(this.valueField));
        }
        return tuple.getDoubleByField(this.valueField);
    }

    /**
     * @param tags The tags to set.
     * @return This so you can do method chaining.
     */
    @SuppressWarnings("unchecked")
    public OpenTsdbTridentTupleFieldMapper setTags(Object tags) {
        if (this.tagsSerializer != null) {
            this.tags = this.tagsSerializer.serialize(tags);
        } else {
            this.tags = (Map<String, String>) tags;
        }
        return this;
    }

    /**
     * @param tagsField Name of the tuple field containing the tags to use.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setTagsField(String tagsField) {
        this.tagsField = tagsField;
        return this;
    }

    /**
     * <p>
     * Note that if you use a constant tags ( setTags )
     * you have to provide the serializer before so that the serialization
     * is done only once.
     * </p>
     *
     * @param tagsSerializer The tags serializer to use.
     * @return This so you can do method chaining.
     */
    public OpenTsdbTridentTupleFieldMapper setTagsSerializer(OpenTsdbTagsSerializer tagsSerializer) {
        this.tagsSerializer = tagsSerializer;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return The tags to use.
     */
    @SuppressWarnings("unchecked")
    public Map<String, String> getTags(TridentTuple tuple) {
        Map<String, String> tags;
        if (this.tags != null) {
            tags = this.tags;
        } else {
            if (this.tagsSerializer != null) {
                tags = this.tagsSerializer.serialize(tuple.getValueByField(this.tagsField));
            } else {
                tags = (Map<String, String>) tuple.getValueByField(this.tagsField);
            }
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

        if (tags.size() == 0) {
            tags.put("foo", "bar");
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
        if (this.metricSerializer != null) {
            this.metricSerializer.prepare(conf);
        }
        if (this.timestampSerializer != null) {
            this.timestampSerializer.prepare(conf);
        }
        if (this.valueSerializer != null) {
            this.valueSerializer.prepare(conf);
        }
        if (this.tagsSerializer != null) {
            this.tagsSerializer.prepare(conf);
        }
    }
}
