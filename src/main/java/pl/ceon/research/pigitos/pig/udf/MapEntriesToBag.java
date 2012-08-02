/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.ceon.research.pigitos.pig.udf;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class MapEntriesToBag extends EvalFunc<DataBag> {

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) input.get(0);
            DataBag bag = null;
            if (map != null) {
                bag = BagFactory.getInstance().newDefaultBag();
                for (Entry<String, Object> entry : map.entrySet()) {
                    Tuple tuple = TupleFactory.getInstance().newTuple(2);
                    tuple.set(0, entry.getKey());
                    tuple.set(1, entry.getValue());
                    bag.add(tuple);
                }
            }
            return bag;

        } catch (Exception e) {
            // Throwing an exception will cause the task to fail.
            throw new RuntimeException("Error while creating a bag", e);
        }
    }
}