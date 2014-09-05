/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.utils.serializer;

import java.util.Map;

public class FloatToDoubleSerializer implements OpenTsdbValueSerializer {
    @Override
    public double serialize(Object object) {
        return ((Float) object).doubleValue();
    }

    @Override
    public void prepare(Map conf) {

    }
}