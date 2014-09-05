/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.utils.serializer;

import java.util.Map;

public class IntegerToDoubleSerializer implements OpenTsdbValueSerializer {
    @Override
    public double serialize(Object object) {
        return ((Integer) object).doubleValue();
    }

    @Override
    public void prepare(Map conf) {

    }
}