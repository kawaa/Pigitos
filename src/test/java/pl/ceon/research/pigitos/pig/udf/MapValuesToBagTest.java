/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.ceon.research.pigitos.pig.udf;

import pl.ceon.research.pigitos.pig.udf.MapValuesToBag;
import java.io.IOException;
import java.util.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import junit.framework.TestCase;
import org.apache.pig.data.DataBag;

/**
 *
 * @author akawa
 */
public class MapValuesToBagTest extends TestCase {

    private MapValuesToBag udf;

    public void setUp() throws Exception {
        udf = new MapValuesToBag();
    }

    public void tearDown() throws Exception {
    }

    public void testOne() throws ExecException, IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("key0", "value0");

        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, map);

        DataBag result = udf.exec(tuple);
        assertEquals("value0", result.iterator().next().get(0));
        
    }

    public void testMany() throws ExecException, IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        Set<String> valuesSet = new HashSet<String>();
        int N = 100;
        for (int i = 0; i < N; ++i) {
            map.put("key" + i, "value" + i);
            valuesSet.add("value" + i);
        }

        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, map);

        DataBag result = udf.exec(tuple);

        Iterator<Tuple> iterator = result.iterator();
        while (iterator.hasNext()) {
            assertTrue(valuesSet.remove(iterator.next().get(0)));
        }

        assertEquals(0, valuesSet.size());
    }

    public void testEmpty() throws ExecException, IOException {
        Map<String, Object> map = new HashMap<String, Object>();

        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, map);

        DataBag result = udf.exec(tuple);
        assertEquals(0, result.size());
    }

    public void testNull() throws ExecException, IOException {
        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, null);

        DataBag result = udf.exec(tuple);
        assertEquals(null, result);
    }
}
