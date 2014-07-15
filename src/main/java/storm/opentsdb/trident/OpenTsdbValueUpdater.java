package net.ovh.storm.opentsdb.trident;

import com.stumbleupon.async.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class OpenTsdbValueUpdater extends BaseStateUpdater<OpenTsdbValueState> {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbValueUpdater.class);

    @Override
    public void updateState(OpenTsdbValueState state, List<TridentTuple> tuples,
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
