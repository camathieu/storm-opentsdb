/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident.light;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.opentsdb.trident.mapper.IOpenTsdbTridentFieldMapper;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

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
public class OpenTsdbLightState implements State {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbLightState.class);

    private final TSDB tsdb;
    private final IOpenTsdbTridentFieldMapper mapper;

    /**
     * @param tsdb   Initialized OpenTSDB client ( used by the factory ).
     * @param mapper A mapper containing mapping from tuple to puts.
     */
    public OpenTsdbLightState(TSDB tsdb, IOpenTsdbTridentFieldMapper mapper) {
        this.tsdb = tsdb;
        this.mapper = mapper;
    }

    /**
     * @param tuple Trident tuple to process.
     * @return A deferred list of void results.
     */
    public Deferred<Object> put(final TridentTuple tuple) {
        double value = mapper.getValue(tuple);
        if (value == (long) value) {
            try {
                return tsdb.addPoint(
                    mapper.getMetric(tuple),
                    mapper.getTimestamp(tuple),
                    (long) value,
                    mapper.getTags(tuple)
                );
            } catch (Exception ex) {
                Deferred.fromError(ex);
            }
        } else {
            try {
                return tsdb.addPoint(
                    mapper.getMetric(tuple),
                    mapper.getTimestamp(tuple),
                    (long) value,
                    mapper.getTags(tuple)
                );
            } catch (Exception ex) {
                Deferred.fromError(ex);
            }
        }

        return Deferred.fromError(new RuntimeException("void"));
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
