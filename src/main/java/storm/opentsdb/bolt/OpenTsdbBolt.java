/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.opentsdb.bolt.mapper.IOpenTsdbFieldMapper;
import storm.opentsdb.bolt.mapper.IOpenTsdbMapper;
import storm.opentsdb.bolt.mapper.OpenTsdbMapper;
import storm.opentsdb.bolt.mapper.OpenTsdbTupleFieldMapper;
import storm.opentsdb.utils.OpenTsdbClientFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This bolt executes one or more OpenTSDB put using the OpenTSDB client<br/>
 * </p>
 * <p>
 * You have to provide some OpenTSDB field mappers to map tuple fields to
 * OpenTSDB metric, timestamp, value and tags.
 * </p>
 * By default the bolt is asynchronous ie: the tuples are acked or failed in
 * the Callback when the result is available. So the thread won't block waiting
 * for the response. But callback may be executed by another thread and unfortunately
 * the OuputCollector object is no longer thread safe, so the calls to ack/fail/emit
 * are synchronized. It should be ok as long as you have one tasks per executor
 * thread ( this is the default behaviour of storm ).<br/>
 * You may make the bolt synchronous by calling setAsync(false) but of course it's
 * killing performance.<br/>
 * Note : Even in synchronous mode multiple RPCs for the same tuple will run in parallel.
 * </p>
 * <p>
 * Throttling :<br/>
 * If HBase can't keep with the stream speed you should get some
 * "There are now N RPCs pending due to NSRE on..." in the logs and the AsyncHBase
 * client will trigger a PleaseThrottleException when reaching 10k pending requests
 * on a specific region. This appends especially when HBase is splitting regions.<br/>
 * This bolt will try to throttle stream speed by turning some execute calls to
 * synchronous mode and sleeping a little.<br/>
 * Please verify that your spout ack tuples and use conf.setMaxSpoutPending
 * to control stream speed.
 * Tuple failed due to PleaseThrottleExecption have to be replayed by the spout if
 * needed.<br/>
 * Note: the throttle code comes from net.opentsdb.tools.TextImporter
 * </p>
 * <p>
 * Look at storm.opentsdb.example.topology.OpenTsdbBoltEventExampleTopology and
 * storm.opentsdb.example.topology.OpenTsdbBoltTupleExampleTopology for
 * some concrete use cases.
 * </p>
 */
public class OpenTsdbBolt implements IRichBolt {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbBolt.class);
    private final String cluster;
    private final String name;
    private final IOpenTsdbMapper mapper;
    Callback<ArrayList<Object>, ArrayList<Object>> successCallback;
    Callback<Object, Exception> errorCallback;
    private OutputCollector collector;
    private TSDB tsdb;
    private boolean async = true;
    private long timeout = 0;
    private volatile boolean throttle = false;

    /**
     * @param cluster HBaseCluster to use
     * @param name    OpenTSDB instance to use
     * @param mapper  Mapper containing mapping from storm tuple to
     *                one or more OpenTsdb put requests
     */
    public OpenTsdbBolt(String cluster, String name, IOpenTsdbMapper mapper) {
        this.cluster = cluster;
        this.name = name;
        this.mapper = mapper;
    }

    /**
     * @param cluster HBaseCluster to use
     * @param name    OpenTSDB instance to use
     *                <p/>
     *                Default constructor for a tuple containing 4 standards fields
     *                named metric, timestamp, value, tags
     */
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
        this.mapper.prepare(conf);
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
                    ));
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

        Deferred<ArrayList<Object>> results = Deferred.group(requests);

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
        } else {
            results.addCallbacks(new SuccessCallback(tuple), new ErrorCallback(tuple));
        }
    }

    @Override
    public void cleanup() {
        // TODO gracefully shutdown HBaseClient.
        // TODO gracefully shutdown OpenTSDB Client
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * Called on success in async mode to asynchronously ack the tuple.
     */
    class SuccessCallback implements Callback<Object, ArrayList<Object>> {
        final Tuple tuple;

        SuccessCallback(Tuple tuple) {
            this.tuple = tuple;
        }

        @Override
        public Object call(ArrayList<Object> results) throws Exception {
            synchronized (collector) {
                collector.ack(tuple);
            }
            return null;
        }
    }

    /**
     * Called on failure in async mode to asynchronously fail the tuple.
     */
    class ErrorCallback implements Callback<Object, Exception> {
        final Tuple tuple;

        ErrorCallback(Tuple tuple) {
            this.tuple = tuple;
        }

        @Override
        public Object call(Exception ex) throws Exception {
            log.error("AsyncHBase exception : " + ex.toString());
            if (ex instanceof PleaseThrottleException) {
                throttle = true;
                return ex;
            }
            synchronized (collector) {
                collector.fail(tuple);
            }
            return ex;
        }
    }
}
