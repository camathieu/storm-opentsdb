/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.bolt.mapper;

import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.opentsdb.model.IOpenTsdbEvent;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * A mapper to map an OpenTSDB event in a storm tuple field
 * ( ie : an object implementing the IOpenTsdbEvent interface )
 * to the OpenTsdb put params ( metrics / ts / value / tags ).
 * </p>
 * <p>
 * It can clean the tags map to keep only those you need.<br/>
 * As of OpenTSDB v2.1 you still need to have at least one tag
 * so providing an empty map will result in this mapper to add
 * a foo=bar tag to your put.
 * </p>
 * TODO implements serializers
 */
public class OpenTsdbEventFieldMapper implements IOpenTsdbFieldMapper {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbEventFieldMapper.class);

    private String eventField;
    private List<String> validTags;

    /**
     * @param eventField The tuple field containing the event
     */
    public OpenTsdbEventFieldMapper(String eventField) {
        this.eventField = eventField;
    }

    /**
     * @param validTags The valid tag list.
     * @return This so you can do method chaining.
     */
    public OpenTsdbEventFieldMapper setValidTags(List<String> validTags) {
        this.validTags = validTags;
        return this;
    }

    /**
     * @param tuple The storm tuple to process.
     * @return The metric from the OpenTsdbEvent.
     */
    public String getMetric(Tuple tuple) {
        IOpenTsdbEvent event = (IOpenTsdbEvent) tuple.getValueByField(this.eventField);
        return event.getMetric();
    }

    /**
     * @param tuple The storm tuple to process
     * @return The timestamp from the OpenTsdbEvent.
     */
    public long getTimestamp(Tuple tuple) {
        IOpenTsdbEvent event = (IOpenTsdbEvent) tuple.getValueByField(this.eventField);
        return event.getTimestamp();
    }

    /**
     * @param tuple The storm tuple to process.
     * @return The value from the OpenTsdbEvent.
     */
    public double getValue(Tuple tuple) {
        IOpenTsdbEvent event = (IOpenTsdbEvent) tuple.getValueByField(this.eventField);
        return event.getValue();
    }

    /**
     * @param tuple The storm tuple to process.
     * @return the tags from the OpenTsdbEvent.
     */
    public Map<String, String> getTags(Tuple tuple) {
        IOpenTsdbEvent event = (IOpenTsdbEvent) tuple.getValueByField(this.eventField);
        Map<String, String> tags = event.getTags();

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