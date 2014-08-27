package storm.opentsdb.trident.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OpenTsdbTridentMapper implements IOpenTsdbTridentMapper {
    private ArrayList<IOpenTsdbTridentFieldMapper> fieldMappers;

    public OpenTsdbTridentMapper addFieldMapper(IOpenTsdbTridentFieldMapper fieldMapper) {
        if (this.fieldMappers == null) {
            this.fieldMappers = new ArrayList<>();
        }
        this.fieldMappers.add(fieldMapper);
        return this;
    }

    @Override
    public List<IOpenTsdbTridentFieldMapper> getFieldMappers() {
        return fieldMappers;
    }

    /**
     * <p>
     * This method will initialize all mappers and serializers.<br/>
     * It will typically has to be called by the bolt prepare method.
     * </p>
     *
     * @param conf Topology configuration.
     */

    @Override
    public void prepare(Map conf) {
        for (IOpenTsdbTridentFieldMapper mapper : this.fieldMappers) {
            mapper.prepare(conf);
        }
    }
}