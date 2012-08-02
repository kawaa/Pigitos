/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.ceon.research.pigitos.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public abstract class MapFieldToBag extends EvalFunc<DataBag> {

    public abstract Set<?> getFieldSet(Map<String, Object> map);

    public Map<String, Object> getMap(Tuple t) throws ExecException {
        return (Map<String, Object>) t.get(0);
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = getMap(input);
            DataBag bag = null;
            if (map != null) {
                bag = BagFactory.getInstance().newDefaultBag();
                for (Object field : getFieldSet(map)) {
                    Tuple tuple = TupleFactory.getInstance().newTuple(1);
                    tuple.set(0, field);
                    bag.add(tuple);
                }
            }
            return bag;

        } catch (Exception e) {
            throw new RuntimeException("Error while creating a bag", e);
        }
    }
}