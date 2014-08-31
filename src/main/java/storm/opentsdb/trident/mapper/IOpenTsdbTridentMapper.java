/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.trident.mapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This interface maps a storm tuple to
 * one or more fields mappers
 * </p>
 */
public interface IOpenTsdbTridentMapper extends Serializable {

    /**
     * @return List of mappers to execute.
     */
    List<IOpenTsdbTridentFieldMapper> getFieldMappers();

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