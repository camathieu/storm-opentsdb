package net.ovh.storm.opentsdb.bolt.mapper;

import java.util.ArrayList;
import java.util.List;

public class SimpleOpenTsdbMapper implements OpenTsdbMapper {
    private ArrayList<OpenTsdbFieldMapper> openTsdbFieldMappers;

    public SimpleOpenTsdbMapper addFieldMapper(OpenTsdbFieldMapper openTsdbFieldMapper) {
        if (this.openTsdbFieldMappers == null) {
            this.openTsdbFieldMappers = new ArrayList<>();
        }
        this.openTsdbFieldMappers.add(openTsdbFieldMapper);
        return this;
    }

    @Override
    public List<OpenTsdbFieldMapper> getOpenTsdbFieldMappers() {
        return openTsdbFieldMappers;
    }
}
