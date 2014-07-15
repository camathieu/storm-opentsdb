package net.ovh.storm.opentsdb.trident.mapper;

import java.io.Serializable;
import java.util.List;

public interface OpenTsdbTridentMapper extends Serializable {
    List<OpenTsdbTridentFieldMapper> getFieldMappers();
}
