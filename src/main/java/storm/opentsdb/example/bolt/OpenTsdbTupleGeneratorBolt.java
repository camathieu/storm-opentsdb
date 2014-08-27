/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.example.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class OpenTsdbTupleGeneratorBolt extends BaseBasicBolt {
    private final String metric;
    private final Map<String,String> tags;
    private boolean millisec = false;

    public  OpenTsdbTupleGeneratorBolt(String metric, Map<String, String> tags){
        this.metric = metric;
        this.tags = tags;
    }

    public OpenTsdbTupleGeneratorBolt(){
        this("test", new HashMap<String,String>());
    }

    public OpenTsdbTupleGeneratorBolt SetMillisec(boolean millisec){
        this.millisec = millisec;
        return this;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long ts = System.currentTimeMillis();
        if (!this.millisec){
            ts /= 1000;
        }

        collector.emit(new Values(this.metric,ts,(double)tuple.getValue(1),this.tags));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("metric","timestamp","value","tags"));
    }
}