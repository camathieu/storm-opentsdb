package net.ovh.storm.opentsdb.bolt.mapper;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Map;

public interface OpenTsdbFieldMapper extends Serializable {
    public String getMetric(Tuple tuple);

    public long getTimestamp(Tuple tuple);

    public double getValue(Tuple tuple);

    public Map<String, String> getTags(Tuple tuple);
}