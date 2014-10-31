/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident.light;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.PleaseThrottleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Update an OpenTsdbState<br/>
 * Use this function with Stream.partitionAggregate or Stream.partitionPersist
 */
public class OpenTsdbLightStateUpdater extends BaseStateUpdater<OpenTsdbLightState> {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbLightStateUpdater.class);

    private boolean throttle = true;
    private boolean async = true;

    /**
     * @param async Whether or not to wait for
     * @return this so you can do method chaining
     */
    public OpenTsdbLightStateUpdater setAsync(boolean async) {
        this.async = async;
        return this;
    }

    @Override
    public void updateState(OpenTsdbLightState state, List<TridentTuple> tuples,
                            final TridentCollector collector) {
        log.info("OpenTsdbStateUpdater : " + "Saving " + tuples.size() + " tuples to OpenTSDB");
        long start_time = System.currentTimeMillis();

        Callback<Object, Exception> errback = new Callback<Object, Exception>() {
            @Override
            public Object call(Exception ex) throws Exception {
                log.warn("OpenTSDB failure ", ex);
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

        List<Deferred<Object>> results = new ArrayList<>();
        for (final TridentTuple tuple : tuples) {
            results.add(state.put(tuple).addErrback(errback));
        }

        if (throttle) {
            log.warn("Throttling...");
            long throttle_time = System.nanoTime();
            try {
                Deferred.group(results).join();
            } catch (Exception ex) {
                log.error("AsyncHBase exception : " + ex.toString());
                collector.reportError(ex);
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
        } else if (!async) {
            try {
                Deferred.group(results).join();
            } catch (InterruptedException ex) {
                log.warn("OpenTSDB results join exception ", ex);
            } catch (Exception ex) {
                log.warn("OpenTSDB exception ", ex);
            }
        }

        long elapsed = System.currentTimeMillis() - start_time;
        log.info("OpenTsdbStateUpdater : " + tuples.size() + " tuples saved to OpenTSDB in " + elapsed + "ms");
    }
}
