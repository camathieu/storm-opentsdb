/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident;

import com.stumbleupon.async.Callback;
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
        for (final TridentTuple tuple : tuples) {
            state.put(tuple).addCallbacks(new Callback<Object, ArrayList<Object>>() {
                @Override
                public Object call(ArrayList<Object> o) throws Exception {
                    // SUCCESS
                    collector.emit(tuple);
                    return null;
                }
            }, new Callback<Object, Exception>() {
                @Override
                public Object call(Exception ex) throws Exception {
                    // ERROR
                    log.warn("OpenTSDB failure " + ex.toString());
                    collector.reportError(ex);
                    return null;
                }
            });

            collector.emit(tuple);
        }
    }
}
