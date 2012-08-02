/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.ceon.research.pigitos.pig.udf;

import pl.ceon.research.pigitos.pig.udf.MapSize;
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
public class MapSizeTest extends TestCase {

    private MapSize udf;

    public void setUp() throws Exception {
        udf = new MapSize();
    }

    public void tearDown() throws Exception {
    }

    public void testOne() throws ExecException, IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("key0", "value0");

        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, map);

        int result = udf.exec(tuple);
        assertEquals(1, result);
        
    }

    public void testMany() throws ExecException, IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        int N = 100;
        for (int i = 0; i < N; ++i) {
            map.put("key" + i, "value" + i);
        }

        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, map);

        int result = udf.exec(tuple);
        assertEquals(N, result);
    }

    public void testEmpty() throws ExecException, IOException {
        Map<String, Object> map = new HashMap<String, Object>();

        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, map);

        int result = udf.exec(tuple);
        assertEquals(0, result);
    }

    public void testNull() throws ExecException, IOException {
        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, null);

        Integer result = udf.exec(tuple);
        assertEquals(null, result);
    }
}
