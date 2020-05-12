package mammoth.server;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class MyHashMap<K, V> extends LinkedHashMap<K, V> {

	
    // 重写HashMapSon类的toString()方法
    @Override
    public String toString() {
        Set<Map.Entry<K, V>> keySet = this.entrySet();
        Iterator<Map.Entry<K, V>> i = keySet.iterator();
        if (!i.hasNext())
            return "";
        StringBuilder buffer = new StringBuilder();
        for (;;) {
            Map.Entry<K, V> me = i.next();
            K key = me.getKey();
            V value = me.getValue();
            buffer.append("[" + key.toString() + ",");
            buffer.append(value.toString() + "],");
            if (!i.hasNext()){
            	buffer.deleteCharAt(buffer.length() - 1);
            	return buffer.toString();
            }
        }
    }

}
