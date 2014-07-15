package net.ovh.storm.opentsdb.bolt.mapper;

import java.io.Serializable;
import java.util.List;

public interface OpenTsdbMapper extends Serializable {
    List<OpenTsdbFieldMapper> getOpenTsdbFieldMappers();
}