/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.ceon.research.pigitos.pig.udf;

import pl.ceon.research.pigitos.pig.udf.MapEntriesToBag;
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
public class MapEntriesToBagTest extends TestCase {

    private MapEntriesToBag udf;

    public void setUp() throws Exception {
        udf = new MapEntriesToBag();
    }

    public void tearDown() throws Exception {
    }

    public void testOne() throws ExecException, IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("key0", "value0");

        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, map);

        DataBag result = udf.exec(tuple);
        Tuple t = result.iterator().next();
        assertEquals("key0", t.get(0));
        assertEquals("value0", t.get(1));
    }

    public void testMany() throws ExecException, IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        int N = 100;
        Set<String> kv = new HashSet<String>();
         for (int i = 0; i < N; ++i) {
            map.put("key" + i, "value" + i);
            kv.add("key" + i + "value" + i);
        }

        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, map);

        DataBag result = udf.exec(tuple);
        Iterator<Tuple> iterator = result.iterator();
        while (iterator.hasNext()) {
            Tuple t = iterator.next();
            assertTrue(kv.remove(t.get(0) + "" + t.get(1)));
        }

        assertEquals(0, kv.size());
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
