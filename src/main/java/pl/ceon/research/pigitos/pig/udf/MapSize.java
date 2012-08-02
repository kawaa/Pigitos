/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.ceon.research.pigitos.pig.udf;

import java.io.IOException;
import java.util.Map;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class MapSize extends EvalFunc<Integer> {

    @Override
    public Integer exec(Tuple input) throws IOException {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) input.get(0);
            return (map != null ? map.size() : null);

        } catch (Exception e) {
            // Throwing an exception will cause the task to fail.
            throw new RuntimeException("Error while calculation size of a map", e);
        }
    }
}