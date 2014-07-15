package net.ovh.storm.opentsdb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.ovh.storm.opentsdb.bolt.mapper.OpenTsdbFieldMapper;
import net.ovh.storm.opentsdb.bolt.mapper.OpenTsdbFieldSet;
import net.ovh.storm.opentsdb.bolt.mapper.OpenTsdbMapper;
import net.ovh.storm.opentsdb.bolt.mapper.SimpleOpenTsdbMapper;
import net.ovh.storm.opentsdb.utils.OpenTsdbClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class OpenTsdbBolt implements IRichBolt {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbBolt.class);
    private final String cluster;
    private final String name;
    private final OpenTsdbMapper mapper;
    private OutputCollector collector;
    private boolean autoAck = true;
    private TSDB tsdb;

    public OpenTsdbBolt(String cluster, String name, OpenTsdbMapper mapper) {
        this.cluster = cluster;
        this.name = name;
        this.mapper = mapper;
    }

    public OpenTsdbBolt(String cluster, String name) {
        this(cluster, name,
            new SimpleOpenTsdbMapper()
                .addFieldMapper(new OpenTsdbFieldSet("metric", "timestamp", "value", "tags")));
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.tsdb = OpenTsdbClientFactory.getTsdbClient(conf, this.cluster, this.name);
    }

    @Override
    public void execute(final Tuple tuple) {
        Deferred<Object> result = null;
        for (OpenTsdbFieldMapper openTsdbFieldMapper : mapper.getOpenTsdbFieldMappers()) {
            double value = openTsdbFieldMapper.getValue(tuple);
            if (value == (long) value) {
                try {
                    result = tsdb.addPoint(
                        openTsdbFieldMapper.getMetric(tuple),
                        openTsdbFieldMapper.getTimestamp(tuple),
                        (long) value,
                        openTsdbFieldMapper.getTags(tuple)
                    );
                } catch (Exception ex) {
                    result = Deferred.fromError(ex);
                }
            } else {
                try {
                    result = tsdb.addPoint(
                        openTsdbFieldMapper.getMetric(tuple),
                        openTsdbFieldMapper.getTimestamp(tuple),
                        value,
                        openTsdbFieldMapper.getTags(tuple)
                    );
                } catch (Exception ex) {
                    result = Deferred.fromError(ex);
                }
            }
        }

        if (result != null) {
            result.addCallbacks(new Callback<Object, Object>() {
                @Override
                public Object call(Object o) throws Exception {
                    // SUCCESS
                    collector.ack(tuple);
                    return this;
                }
            }, new Callback<Object, Exception>() {
                @Override
                public Object call(Exception ex) throws Exception {
                    // ERROR
                    log.warn("OpenTSDB exception : " + ex.toString());
                    collector.fail(tuple);
                    return this;
                }
            });
        } else {
            this.collector.ack(tuple);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * @return the autoAck
     */
    public boolean isAutoAck() {
        return autoAck;
    }

    /**
     * @param autoAck the autoAck to set
     */
    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }
}
