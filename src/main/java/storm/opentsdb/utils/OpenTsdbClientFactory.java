package storm.opentsdb.utils;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.asynchbase.utils.AsyncHBaseClientFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class OpenTsdbClientFactory implements Serializable {
    public static final Logger log = LoggerFactory.getLogger(OpenTsdbClientFactory.class);

    public static TSDB getTsdbClient(Map config, String hBaseCluster, String name) {
        log.info("New OpenTSDB client : " + name);
        try {
            Map<String, String> conf = (Map<String, String>) config.get(name);
            Config openTsdbConfig = new net.opentsdb.utils.Config(true);
            for (String key : conf.keySet()) {
                openTsdbConfig.overrideConfig(key, conf.get(key));
            }
            return new TSDB(
                AsyncHBaseClientFactory.getHBaseClient(config, hBaseCluster),
                openTsdbConfig);
        } catch (IOException ex) {
            log.error("OpenTSDB config execption : should not be here !!!");
            return null;
        } catch (Exception ex) {
            log.error("OpenTSDB config execption : " + ex.toString());
            throw ex;
        }
    }
}