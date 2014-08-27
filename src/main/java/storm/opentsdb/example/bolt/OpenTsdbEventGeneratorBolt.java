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
import storm.opentsdb.model.OpenTsdbEvent;

import java.util.HashMap;
import java.util.Map;

public class OpenTsdbEventGeneratorBolt extends BaseBasicBolt {
    private final String metric;
    private final Map<String,String> tags;
    private boolean millisec = false;

    public OpenTsdbEventGeneratorBolt(String metric, Map<String, String> tags){
        this.metric = metric;
        this.tags = tags;
    }

    public OpenTsdbEventGeneratorBolt(){
        this("test", new HashMap<String,String>());
    }

    public OpenTsdbEventGeneratorBolt SetMillisec(boolean millisec){
        this.millisec = millisec;
        return this;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long ts = System.currentTimeMillis();
        if (!this.millisec){
            ts /= 1000;
        }

        collector.emit(new Values(new OpenTsdbEvent(this.metric,ts,(double)tuple.getValue(1),this.tags)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event"));
    }
}