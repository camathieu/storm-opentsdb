/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident;

import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * TODO implements queries for trident
 * </p>
 */
public class OpenTsdbStateQuery extends BaseQueryFunction<OpenTsdbState, Deferred<ArrayList<KeyValue>>> {
    @Override
    public List<Deferred<ArrayList<KeyValue>>> batchRetrieve(OpenTsdbState state, List<TridentTuple> args) {
        //TODO
        return null;
    }

    @Override
    public void execute(TridentTuple tuple, Deferred<ArrayList<KeyValue>> result, TridentCollector collector) {
        //TODO
    }
}