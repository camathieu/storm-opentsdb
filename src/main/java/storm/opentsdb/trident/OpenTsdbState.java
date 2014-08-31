/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.opentsdb.trident.mapper.IOpenTsdbTridentFieldMapper;
import storm.opentsdb.trident.mapper.OpenTsdbTridentMapper;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;

/**
 * <p>
 * This is a TridentState implementation to persist a partition to OpenTSDB.<br/>
 * You should only use this state if your update is idempotent regarding batch replay.
 * </p>
 * <p>
 * Use storm.opentsdb.trident.state.StateFactory to handle state creation<br/>
 * Use storm.opentsdb.trident.state.StateUpdater to update state<br/>
 * Use storm.opentsdb.trident.state.StateQuery to query state<br/>
 * </p>
 * <p>
 * Please look at storm.opentsdb.example.topology.OpenTsdbTridentExampleTopology
 * for a concrete use case.
 * </p>
 */
public class OpenTsdbState implements State {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbState.class);

    private final TSDB tsdb;
    private final OpenTsdbTridentMapper mapper;

    /**
     * @param tsdb   Initialized OpenTSDB client ( used by the factory ).
     * @param mapper A mapper containing mapping from tuple to puts.
     */
    public OpenTsdbState(TSDB tsdb, OpenTsdbTridentMapper mapper) {
        this.tsdb = tsdb;
        this.mapper = mapper;
    }

    /**
     * @param tuple Trident tuple to process.
     * @return A deferred list of void results.
     */
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
