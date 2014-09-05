/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.utils;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.utils.AsyncHBaseClientFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * <p>
 * Instentiate an OpenTSDB client instance from the
 * topology configuration. As you should have only one
 * AsyncHBase client per application storm-opentsdb uses
 * the storm-asynchbase client factory.
 * </p>
 */
public class OpenTsdbClientFactory implements Serializable {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbClientFactory.class);

    /**
     * @param config       Topology config.
     * @param hBaseCluster The HBase cluster to use.
     * @param name         Topology config key.
     * @return
     */
    @SuppressWarnings("unchecked")
    public static TSDB getTsdbClient(Map config, String hBaseCluster, String name) {
        log.info("New OpenTSDB client : " + name);
        try {
            Map<String, String> conf = (Map<String, String>) config.get(name);
            if (conf == null) {
                throw new RuntimeException("Missing configuration for OpenTsdb client : " + name);
            }

            Config openTsdbConfig = new net.opentsdb.utils.Config(true);
            for (String key : conf.keySet()) {
                openTsdbConfig.overrideConfig(key, conf.get(key));
            }
            TSDB tsdb = new TSDB(
                AsyncHBaseClientFactory.getHBaseClient(config, hBaseCluster),
                openTsdbConfig);

            try {
                tsdb.checkNecessaryTablesExist().joinUninterruptibly();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return tsdb;
        } catch (IOException ex) {
            log.error("OpenTSDB config execption : should not be here !!!");
            return null;
        } catch (Exception ex) {
            log.error("OpenTSDB config execption : " + ex.toString());
            throw ex;
        }
    }
}