/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.example.trident.operation;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

public class OpenTsdbTupleGeneratorFunction extends BaseFunction {
    private final String metric;
    private final Map<String,String> tags;
    private boolean millisec = false;

    public OpenTsdbTupleGeneratorFunction(String metric, Map<String, String> tags){
        this.metric = metric;
        this.tags = tags;
    }

    public OpenTsdbTupleGeneratorFunction(){
        this("test", new HashMap<String,String>());
    }

    public OpenTsdbTupleGeneratorFunction SetMillisec(boolean millisec){
        this.millisec = millisec;
        return this;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        long ts = System.currentTimeMillis();
        if (!this.millisec){
            ts /= 1000;
        }

        collector.emit(new Values(this.metric,ts,(double)tuple.getValue(1),this.tags));
    }
}
