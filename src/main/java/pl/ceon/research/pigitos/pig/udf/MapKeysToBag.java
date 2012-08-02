/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.ceon.research.pigitos.pig.udf;

import java.util.Map;
import java.util.Set;

public class MapKeysToBag extends MapFieldToBag {

    @Override
    public Set<String> getFieldSet(Map<String, Object> map) {
        return map.keySet();
    }    
}
