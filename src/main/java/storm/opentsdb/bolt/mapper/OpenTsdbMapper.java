/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.bolt.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This mapper maps a storm tuple to
 * one or more fields mappers
 */
public class OpenTsdbMapper implements IOpenTsdbMapper {
    private ArrayList<IOpenTsdbFieldMapper> fieldMappers;

    /**
     * @param fieldMapper Add a FieldMapper to the mapper list.
     * @return This so you can do method chaining.
     */
    public OpenTsdbMapper addFieldMapper(IOpenTsdbFieldMapper fieldMapper) {
        if (this.fieldMappers == null) {
            this.fieldMappers = new ArrayList<>();
        }
        this.fieldMappers.add(fieldMapper);
        return this;
    }

    /**
     * @return The list of mappers to execute.
     */
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
