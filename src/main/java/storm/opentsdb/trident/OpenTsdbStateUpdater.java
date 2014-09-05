/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Update an OpenTsdbState<br/>
 * Use this function with Stream.partitionAggregate(State,Aggregator,StateUpdater,...)
 */
public class OpenTsdbStateUpdater extends BaseStateUpdater<OpenTsdbState> {
    public static final Logger log = LoggerFactory.getLogger(storm.opentsdb.trident.OpenTsdbStateUpdater.class);

    @Override
    public void updateState(OpenTsdbState state, List<TridentTuple> tuples,
                            final TridentCollector collector) {

        log.info("OpenTsdbStateUpdater : " + "Saving " + tuples.size() + " tuples to OpenTSDB");
        long start_time = System.currentTimeMillis();

        List<Deferred<ArrayList<Object>>> results = new ArrayList<>();
        for (final TridentTuple tuple : tuples) {
            results.add(state.put(tuple).addErrback(new Callback<Object, Exception>() {
                @Override
                public Object call(Exception ex) throws Exception {
                    // ERROR
                    log.warn("OpenTSDB failure ", ex);
                    synchronized (collector) {
                        collector.reportError(ex);
                    }
                    return null;
                }
            }));
        }

        try {
            Deferred.group(results).join();
        } catch (InterruptedException ex) {
            log.warn("OpenTSDB results join exception ", ex);
        } catch (Exception ex) {
            log.warn("OpenTSDB exception ", ex);
        }

        long elapsed = System.currentTimeMillis() - start_time;
        log.info("OpenTsdbStateUpdater : " + tuples.size() + " tuples saved to OpenTSDB in " + elapsed + "ms");
    }
}
