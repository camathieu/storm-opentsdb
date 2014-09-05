/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.opentsdb.utils.serializer;

import java.util.Map;

public class ShortToDoubleSerializer implements OpenTsdbValueSerializer {
    @Override
    public double serialize(Object object) {
        return ((Short) object).doubleValue();
    }

    @Override
    public void prepare(Map conf) {

    }
}