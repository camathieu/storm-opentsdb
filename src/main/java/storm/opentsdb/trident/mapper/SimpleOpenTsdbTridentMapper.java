package net.ovh.storm.opentsdb.trident.mapper;

import java.util.ArrayList;
import java.util.List;

public class SimpleOpenTsdbTridentMapper implements OpenTsdbTridentMapper {
    private ArrayList<OpenTsdbTridentFieldMapper> fieldSets;

    public SimpleOpenTsdbTridentMapper addFieldMapper(OpenTsdbTridentFieldMapper fieldMapper) {
        if (this.fieldSets == null) {
            this.fieldSets = new ArrayList<>();
        }
        this.fieldSets.add(fieldMapper);
        return this;
    }

    @Override
    public List<OpenTsdbTridentFieldMapper> getFieldMappers() {
        return fieldSets;
    }
}