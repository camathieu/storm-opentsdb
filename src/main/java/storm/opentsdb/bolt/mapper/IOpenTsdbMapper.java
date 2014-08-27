package storm.opentsdb.bolt.mapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * This interface holds several fields mappers ( RPC configuration ) by name
 */
public interface IOpenTsdbMapper extends Serializable {

    /**
     * @return List of mappers to execute.
     */
    List<IOpenTsdbFieldMapper> getFieldMappers();

    /**
     * <p>
     * This method will initialize all mappers and serializers.<br/>
     * It will typically has to be called by the bolt prepare method.
     * </p>
     *
     * @param conf Topology configuration.
     */
    void prepare(Map conf);
}