/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import org.hbase.async.PleaseThrottleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.opentsdb.trident.mapper.IOpenTsdbTridentFieldMapper;
import storm.opentsdb.trident.mapper.IOpenTsdbTridentMapper;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Update an OpenTsdbState<br/>
 * Use this function with Stream.partitionAggregate(State,Aggregator,StateUpdater,...)
 */
public class OpenTsdbStateUpdater extends BaseStateUpdater<OpenTsdbState> {
    public static final Logger log = LoggerFactory.getLogger(storm.opentsdb.trident.OpenTsdbStateUpdater.class);

    private final IOpenTsdbTridentMapper mapper;

    private boolean throttle = false;
    private boolean async = true;

    public OpenTsdbStateUpdater(IOpenTsdbTridentMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param async Whether or not to wait for
     * @return this so you can do method chaining
     */
    public OpenTsdbStateUpdater setAsync(boolean async) {
        this.async = async;
        return this;
    }

    @Override
    public void updateState(OpenTsdbState state, List<TridentTuple> tuples,
                            final TridentCollector collector) {
        log.info("OpenTsdbStateUpdater : " + "Saving " + tuples.size() + " tuples to OpenTSDB");
        long start_time = System.currentTimeMillis();

        Callback<Object, Exception> errback = new Callback<Object, Exception>() {
            @Override
            public Object call(Exception ex) throws Exception {
                log.warn("OpenTSDB failure : " + ex.getMessage());
                if (ex instanceof PleaseThrottleException) {
                    throttle = true;
                    return ex;
                }
                synchronized (collector) {
                    collector.reportError(ex);
                }
                return ex;
            }
        };

        TSDB tsdb = state.getOpenTsdbClient();

        List<Deferred<Object>> results = new ArrayList<>();
        for (final TridentTuple tuple : tuples) {
            for (IOpenTsdbTridentFieldMapper fieldMapper : mapper.getFieldMappers()) {
                if ( ! fieldMapper.isFiltered(tuple) ) {
                    double value = fieldMapper.getValue(tuple);
                    if (value == (long) value) {
                        try {
                            results.add(tsdb.addPoint(
                                fieldMapper.getMetric(tuple),
                                fieldMapper.getTimestamp(tuple),
                                (long) value,
                                fieldMapper.getTags(tuple)
                            ).addErrback(errback));
                        } catch (Exception ex) {
                            results.add(Deferred.fromError(ex).addErrback(errback));
                        }
                    } else {
                        try {
                            results.add(tsdb.addPoint(
                                fieldMapper.getMetric(tuple),
                                fieldMapper.getTimestamp(tuple),
                                (long) value,
                                fieldMapper.getTags(tuple)
                            ).addErrback(errback));
                        } catch (Exception ex) {
                            results.add(Deferred.fromError(ex).addErrback(errback));
                        }
                    }
                }
            }
        }

        if (throttle) {
            log.warn("Throttling...");
            long throttle_time = System.nanoTime();
            try {
                Deferred.group(results).joinUninterruptibly();
            } catch (Exception ex) {
                log.error("AsyncHBase exception : " + ex.getMessage());
                collector.reportError(ex);
            } finally {
                throttle_time = System.nanoTime() - throttle_time;
                if (throttle_time < 1000000000L) {
                    log.info("Got throttled for only " + throttle_time + "ns, sleeping a bit now");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        log.error("AsyncHBase exception : " + ex.getMessage());
                    }
                }
                log.info("Done throttling...");
                this.throttle = false;
            }
        } else if (!async) {
            try {
                Deferred.group(results).joinUninterruptibly();
            } catch (InterruptedException ex) {
                log.warn("OpenTSDB results join exception " + ex.getMessage());
            } catch (Exception ex) {
                log.warn("OpenTSDB exception : " + ex.getMessage());
            }
        }

        long elapsed = System.currentTimeMillis() - start_time;
        log.info("OpenTsdbStateUpdater : " + tuples.size() + " tuples saved to OpenTSDB in " + elapsed + "ms");
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        mapper.prepare(conf);
    }
}
