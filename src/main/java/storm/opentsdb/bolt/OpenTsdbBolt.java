package storm.opentsdb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import org.hbase.async.PleaseThrottleException;
import storm.opentsdb.bolt.mapper.IOpenTsdbFieldMapper;
import storm.opentsdb.bolt.mapper.IOpenTsdbMapper;
import storm.opentsdb.bolt.mapper.OpenTsdbTupleFieldMapper;
import storm.opentsdb.bolt.mapper.OpenTsdbMapper;
import storm.opentsdb.utils.OpenTsdbClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OpenTsdbBolt implements IRichBolt {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbBolt.class);
    private final String cluster;
    private final String name;
    private final IOpenTsdbMapper mapper;
    Callback<ArrayList<Object>, ArrayList<Object>> successCallback;
    Callback<Object, Exception> errorCallback;
    private Errback errback;
    private OutputCollector collector;
    private TSDB tsdb;
    private boolean async = true;
    private long timeout = 0;
    private volatile boolean throttle = false;

    public OpenTsdbBolt(String cluster, String name, IOpenTsdbMapper mapper) {
        this.cluster = cluster;
        this.name = name;
        this.mapper = mapper;
    }

    public OpenTsdbBolt(String cluster, String name) {
        this(cluster, name,
            new OpenTsdbMapper()
                .addFieldMapper(new OpenTsdbTupleFieldMapper("metric", "timestamp", "value", "tags")));
    }

    /**
     * @param callback Add a success callback between RPC return and tuple ack/emit.
     * @return This so you can do method chaining.
     */
    public OpenTsdbBolt addCallback(Callback<ArrayList<Object>, ArrayList<Object>> callback) {
        this.successCallback = callback;
        return this;
    }

    /**
     * @param errback Add an error callback between RPC return and tuple failure.
     * @return This so you can do method chaining.
     */
    public OpenTsdbBolt addErrback(Callback<Object, Exception> errback) {
        this.errorCallback = errback;
        return this;
    }

    /**
     * @param async set synchronous/asynchronous mode
     * @return This so you can do method chaining.
     */
    public OpenTsdbBolt setAsync(boolean async) {
        this.async = async;
        return this;
    }

    /**
     * @param timeout how long to wait for results in synchronous mode
     *                (in millisecond).
     * @return This so you can do method chaining.
     */
    public OpenTsdbBolt setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.tsdb = OpenTsdbClientFactory.getTsdbClient(conf, this.cluster, this.name);
        errback = new Errback();
    }

    @Override
    public void execute(final Tuple tuple) {
        List<IOpenTsdbFieldMapper> mappers = this.mapper.getFieldMappers();
        ArrayList<Deferred<Object>> requests = new ArrayList<>(mappers.size());
        for (IOpenTsdbFieldMapper fieldMapper : mappers) {
            double value = fieldMapper.getValue(tuple);
            if (value == (long) value) {
                try {
                    requests.add(this.tsdb.addPoint(
                        fieldMapper.getMetric(tuple),
                        fieldMapper.getTimestamp(tuple),
                        (long) value,
                        fieldMapper.getTags(tuple)
                    ).addErrback(errback));
                } catch (Exception ex) {
                    requests.add(Deferred.fromError(ex));
                }
            } else {
                try {
                    requests.add(this.tsdb.addPoint(
                        fieldMapper.getMetric(tuple),
                        fieldMapper.getTimestamp(tuple),
                        value,
                        fieldMapper.getTags(tuple)
                    ));
                } catch (Exception ex) {
                    requests.add(Deferred.fromError(ex));
                }
            }
        }

        Deferred<ArrayList<Object>> results = Deferred.groupInOrder(requests);

        if (this.successCallback != null) {
            results = results.addCallback(this.successCallback);
        }

        if (this.errorCallback != null) {
            results = results.addErrback(this.errorCallback);
        }

        if (throttle) {
            log.warn("Throttling...");
            long throttle_time = System.nanoTime();
            try {
                results.joinUninterruptibly(this.timeout);
                this.collector.ack(tuple);
            } catch (Exception ex) {
                log.error("AsyncHBase exception : " + ex.toString());
                this.collector.fail(tuple);
            } finally {
                throttle_time = System.nanoTime() - throttle_time;
                if (throttle_time < 1000000000L) {
                    log.info("Got throttled for only " + throttle_time + "ns, sleeping a bit now");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        log.error("AsyncHBase exception : " + ex.toString());
                    }
                }
                log.info("Done throttling...");
                this.throttle = false;
            }
        } else if (!this.async) {
            try {
                this.collector.emit(results.joinUninterruptibly(this.timeout));
                this.collector.ack(tuple);
            } catch (Exception ex) {
                log.error("AsyncHBase exception : " + ex.toString());
                this.collector.fail(tuple);
            }
            this.collector.ack(tuple);
        } else {
            results.addCallbacks(new Callback<Object, ArrayList<Object>>() {
                @Override
                public Object call(ArrayList<Object> results) throws Exception {
                    synchronized (collector) {
                        collector.emit(results);
                        collector.ack(tuple);
                    }
                    return null;
                }
            }, new Callback<Object, Exception>() {
                @Override
                public Object call(Exception ex) throws Exception {
                    // ERROR
                    log.error("AsyncHBase exception : " + ex.toString());
                    synchronized (collector) {
                        collector.fail(tuple);
                    }
                    return this;
                }
            });
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
     * <p>
     * This error callback checks if there is a need to throttle
     * </p>
     */
    final class Errback implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception ex) {
            if (ex instanceof PleaseThrottleException) {
                throttle = true;
                return null;
            }
            log.warn("hbase exception " + ex.toString());
            return ex;
        }
    }
}