package storm.opentsdb.bolt.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OpenTsdbMapper implements IOpenTsdbMapper {
    private ArrayList<IOpenTsdbFieldMapper> fieldMappers;

    public OpenTsdbMapper addFieldMapper(IOpenTsdbFieldMapper openTsdbFieldMapper) {
        if (this.fieldMappers == null) {
            this.fieldMappers = new ArrayList<>();
        }
        this.fieldMappers.add(openTsdbFieldMapper);
        return this;
    }

    @Override
    public List<IOpenTsdbFieldMapper> getFieldMappers() {
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
        for (IOpenTsdbFieldMapper mapper : this.fieldMappers) {
            mapper.prepare(conf);
        }
    }
}
