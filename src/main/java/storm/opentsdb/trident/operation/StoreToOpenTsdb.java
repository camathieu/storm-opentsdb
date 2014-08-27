/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident.operation;

import backtype.storm.topology.FailedException;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import org.hbase.async.PleaseThrottleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.opentsdb.trident.mapper.IOpenTsdbTridentFieldMapper;
import storm.opentsdb.trident.mapper.IOpenTsdbTridentMapper;
import storm.opentsdb.trident.mapper.OpenTsdbTridentMapper;
import storm.opentsdb.trident.mapper.OpenTsdbTridentTupleFieldMapper;
import storm.opentsdb.utils.OpenTsdbClientFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StoreToOpenTsdb extends BaseFunction{
    public static final Logger log = LoggerFactory.getLogger(StoreToOpenTsdb.class);

    private final String cluster;
    private final String name;
    private final IOpenTsdbTridentMapper mapper;
    public FailStrategy failStrategy = FailStrategy.LOG;
    Callback<ArrayList<Object>, ArrayList<Object>> successCallback;
    Callback<Object, Exception> errorCallback;
    private TSDB tsdb;
    private boolean async = true;
    private long timeout = 0;
    private volatile boolean throttle = false;

    public StoreToOpenTsdb(String cluster, String name, IOpenTsdbTridentMapper mapper) {
        this.cluster = cluster;
        this.name = name;
        this.mapper = mapper;
    }

    public StoreToOpenTsdb(String cluster, String name) {
        this(cluster, name,
            new OpenTsdbTridentMapper()
                .addFieldMapper(new OpenTsdbTridentTupleFieldMapper("metric", "timestamp", "value", "tags")));
    }

    /**
     * @param callback Add a success callback between RPC return and tuple ack/emit.
     * @return This so you can do method chaining.
     */
    public StoreToOpenTsdb addCallback(Callback<ArrayList<Object>, ArrayList<Object>> callback) {
        this.successCallback = callback;
        return this;
    }

    /**
     * @param errback Add an error callback between RPC return and tuple failure.
     * @return This so you can do method chaining.
     */
    public StoreToOpenTsdb addErrback(Callback<Object, Exception> errback) {
        this.errorCallback = errback;
        return this;
    }

    /**
     * @param async set synchronous/asynchronous mode
     * @return This so you can do method chaining.
     */
    public StoreToOpenTsdb setAsync(boolean async) {
        this.async = async;
        return this;
    }

    /**
     * @param timeout how long to wait for results in synchronous mode
     *                (in millisecond).
     * @return This so you can do method chaining.
     */
    public StoreToOpenTsdb setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * <p>
     * LOG : Only log error and don't return any results.<br/>
     * RETRY : Ask the spout to replay the batch.<br/>
     * FAILFAST : Let the function crash.<br/>
     * null/NOOP : Do nothing.
     * </p>
     * <p>
     * http://svendvanderveken.wordpress.com/2014/02/05/error-handling-in-storm-trident-topologies/
     * </p>
     *
     * @param strategy Set the strategy to adopt in case of AsyncHBase execption
     * @return This so you can do method chaining.
     */
    public StoreToOpenTsdb setFailStrategy(FailStrategy strategy) {
        this.failStrategy = strategy;
        return this;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        this.tsdb = OpenTsdbClientFactory.getTsdbClient(conf, this.cluster, this.name);
    }

    @Override
    public void execute(final TridentTuple tuple, final TridentCollector collector) {
        List<IOpenTsdbTridentFieldMapper> mappers = this.mapper.getFieldMappers();
        ArrayList<Deferred<Object>> requests = new ArrayList<>(mappers.size());
        for (IOpenTsdbTridentFieldMapper fieldMapper : mappers) {
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
                collector.emit(results.joinUninterruptibly(this.timeout));
            } catch (Exception ex) {
                this.handleFailure(ex);
            } finally {
                throttle_time = System.nanoTime() - throttle_time;
                if (throttle_time < 1000000000L) {
                    log.info("Got throttled for only " + throttle_time + "ns, sleeping a bit now");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        this.handleFailure(ex);
                    }
                }
                log.info("Done throttling...");
                this.throttle = false;
            }
        } else if (!this.async) {
            try {
                collector.emit(results.joinUninterruptibly(this.timeout));
            } catch (Exception ex) {
                this.handleFailure(ex);
            }
        } else {
            results.addCallbacks(new Callback<Object, ArrayList<Object>>() {
                @Override
                public Object call(ArrayList<Object> results) throws Exception {
                    synchronized (collector) {
                        collector.emit(results);
                    }
                    return null;
                }
            }, new Callback<Object, Exception>() {
                @Override
                public Object call(Exception ex) throws Exception {
                    // TODO : This may trigger the wrong batch to be replayed ?
                    handleFailure(ex);
                    return ex;
                }
            });
        }
    }

    @Override
    public void cleanup() {
        this.tsdb.shutdown();
    }

    private void handleFailure(Exception ex) {
        if (ex instanceof PleaseThrottleException) {
            throttle = true;
        }
        switch (this.failStrategy) {
            case LOG:
                log.error("AsyncHBase error while executing HBase RPC" + ex.getMessage());
                break;
            case RETRY:
                log.error("AsyncHBase error while executing HBase RPC" + ex.getMessage());
                throw new FailedException("AsyncHBase error while executing HBase RPC " + ex.getMessage());
            case FAILFAST:
                log.error("AsyncHBase error while executing HBase RPC" + ex.getMessage());
                throw new RuntimeException("AsyncHBase error while executing HBase RPC " + ex.getMessage());
        }
    }

    public enum FailStrategy {NOOP, LOG, FAILFAST, RETRY}
}
