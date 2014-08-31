/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.example.trident.operation;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * Adaptator function to add to a tuple with only value a static metric and tags
 * list and the current timestamp.
 * Until storm-opentsdb field mappers will be as strong as storm-asynchbase one
 * you'll may have to write some of this quite often.
 * </p>
 */
public class OpenTsdbTupleAdaptatorFunction extends BaseFunction {
    protected final String metric;
    protected final String valueField;
    protected final Map<String, String> tags;
    protected boolean millisec = false;

    /**
     * @param metric     Metric to put to.
     * @param valueField Tuple field containing the value.
     * @param tags       Tags.
     */
    public OpenTsdbTupleAdaptatorFunction(String metric, String valueField, Map<String, String> tags) {
        this.metric = metric;
        this.valueField = valueField;
        this.tags = tags;
    }

    /**
     * <p>
     * Default Adaptator with test metric, value in the value field and empty tags ( foo=bar )
     * </p>
     */
    public OpenTsdbTupleAdaptatorFunction() {
        this("test", "value", new HashMap<String, String>());
    }

    /**
     * @param millisec Activate or not millisecond timestamp.
     * @return This so you can do method chaining
     */
    public OpenTsdbTupleAdaptatorFunction SetMillisec(boolean millisec) {
        this.millisec = millisec;
        return this;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        long ts = System.currentTimeMillis();
        if (!this.millisec) {
            ts /= 1000;
        }

        collector.emit(new Values(this.metric, ts, (double) tuple.getValueByField(this.valueField), this.tags));
    }
}
