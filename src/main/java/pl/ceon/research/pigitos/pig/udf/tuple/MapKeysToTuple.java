package pl.ceon.research.pigitos.pig.udf.tuple;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class MapKeysToTuple extends EvalFunc<Tuple> {
    @Override
    public Tuple exec(Tuple input) throws IOException {

        TupleFactory instance = TupleFactory.getInstance();
        Tuple tuple = instance.newTuple();

        Map<String, Object> map = getMap(input);

        Collection<String> values = map.keySet();

        for (String value : values) {
            tuple.append(value);
        }

        return tuple;
    }

    public Map<String, Object> getMap(Tuple t) throws ExecException {
        return (Map<String, Object>) t.get(0);
    }
}
