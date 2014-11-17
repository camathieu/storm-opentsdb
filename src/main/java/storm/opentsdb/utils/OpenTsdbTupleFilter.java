/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.utils;

import storm.trident.tuple.TridentTuple;

import java.io.Serializable;

public interface OpenTsdbTupleFilter extends Serializable {
    public boolean filter(TridentTuple tuple);
}
