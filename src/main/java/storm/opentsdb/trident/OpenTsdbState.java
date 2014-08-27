package storm.opentsdb.trident;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import storm.opentsdb.trident.mapper.IOpenTsdbTridentFieldMapper;
import storm.opentsdb.trident.mapper.OpenTsdbTridentMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;

public class OpenTsdbState implements State {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbState.class);

    private final TSDB tsdb;
    private final OpenTsdbTridentMapper mapper;

    public OpenTsdbState(TSDB tsdb, OpenTsdbTridentMapper mapper) {
        this.tsdb = tsdb;
        this.mapper = mapper;
    }

    public Deferred<ArrayList<Object>> put(final TridentTuple tuple) {
        ArrayList<Deferred<Object>> results = new ArrayList<>();

        for (IOpenTsdbTridentFieldMapper fieldMapper : mapper.getFieldMappers()) {
            double value = fieldMapper.getValue(tuple);
            if (value == (long) value) {
                try {
                    results.add(tsdb.addPoint(
                        fieldMapper.getMetric(tuple),
                        fieldMapper.getTimestamp(tuple),
                        (long) value,
                        fieldMapper.getTags(tuple)
                    ));
                } catch (Exception ex) {
                    results.add(Deferred.fromError(ex));
                }
            } else {
                try {
                    results.add(tsdb.addPoint(
                        fieldMapper.getMetric(tuple),
                        fieldMapper.getTimestamp(tuple),
                        (long) value,
                        fieldMapper.getTags(tuple)
                    ));
                } catch (Exception ex) {
                    results.add(Deferred.fromError(ex));
                }
            }
        }

        return Deferred.group(results);
    }

    @Override
    public void beginCommit(Long txid) {
        log.debug("Beginning commit for tx " + txid);
    }

    @Override
    public void commit(Long txid) {
        log.debug("Commit tx " + txid);
    }
}
