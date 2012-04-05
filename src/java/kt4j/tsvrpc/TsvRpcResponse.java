package kt4j.tsvrpc;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import kt4j.Bytes;
import kt4j.Bytes.ByteArrayWrapper;
import kt4j.Response;

/**
 * 
 * @author kumai
 */
class TsvRpcResponse implements Response {
    private static final ByteArrayWrapper NUM_KEY = new ByteArrayWrapper(Bytes.utf8("num"));
    private static final ByteArrayWrapper VALUE_KEY = new ByteArrayWrapper(Bytes.utf8("value"));
    final int status;
    
    private Map<ByteArrayWrapper, byte[]> values = new HashMap<ByteArrayWrapper, byte[]>();
    
    TsvRpcResponse(int status) {
        this.status = status;
    }
    
    void put(byte[] key, byte[] value) {
        values.put(new ByteArrayWrapper(key), value);
    }
    
    long getNumber() {
        byte[] numString = values.get(NUM_KEY);
        if (numString != null) {
            return Long.parseLong(Bytes.utf8(numString));
        } else {
            return -1;
        }
    }
    
    double getNumberDouble() {
        byte[] numString = values.get(NUM_KEY);
        if (numString != null) {
            return Double.parseDouble(Bytes.utf8(numString));
        } else {
            return -1;
        }
    }
    
    byte[] getValue() {
        return values.get(VALUE_KEY);
    }
    
    Map<byte[], byte[]> getBulkResult() {
        ByteArrayMap result = new ByteArrayMap();
        for (Map.Entry<ByteArrayWrapper, byte[]> entry : values.entrySet()) {
            byte[] key = entry.getKey().array;
            if (key[0] == '_') {
                byte[] buf = new byte[key.length - 1];
                System.arraycopy(key, 1, buf, 0, key.length - 1);
                result.put(buf, entry.getValue());
            }
        }
        return result;
    }
    
    Map<byte[], byte[]> getRawResult() {
        ByteArrayMap result = new ByteArrayMap();
        for (Map.Entry<ByteArrayWrapper, byte[]> entry : values.entrySet()) {
            result.put(entry.getKey().array, entry.getValue());
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder()
            .append("{")
            .append("status=").append(status)
            .append(", ")
            .append("values=").append(values)
            .append("}");
        return sb.toString();
    }

    /* (non-Javadoc)
     * @see kt4j.KyotoTycoonResponse#isSucceeded()
     */
    @Override
    public boolean isSucceeded() {
        return (status == 200 || status == 450);
    }
    
    private static class ByteArrayMap implements Map<byte[], byte[]> {
        private final Map<ByteArrayWrapper, byte[]> map = new HashMap<Bytes.ByteArrayWrapper, byte[]>();
        
        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            if (key instanceof byte[]) {
                return map.containsKey(new ByteArrayWrapper((byte[]) key));
            } else {
                return map.containsKey(key);
            }
        }

        @Override
        public boolean containsValue(Object value) {
            return map.containsValue(value);
        }

        @Override
        public byte[] get(Object key) {
            if (key instanceof byte[]) {
                return map.get(new ByteArrayWrapper((byte[]) key));
            } else {
                return map.get(key);
            }
        }
        
        @Override
        public byte[] put(byte[] key, byte[] value) {
            return map.put(new ByteArrayWrapper(key), value);
        }

        @Override
        public byte[] remove(Object key) {
            if (key instanceof byte[]) {
                return map.remove(new ByteArrayWrapper((byte[]) key));
            } else {
                return map.remove(key);
            }
        }

        @Override
        public void putAll(Map<? extends byte[], ? extends byte[]> m) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<byte[]> keySet() {
            LinkedHashSet<byte[]> keys = new LinkedHashSet<byte[]>(map.size());
            for (ByteArrayWrapper bytes : map.keySet()) {
                keys.add(bytes.array);
            }
            return keys;
        }

        @Override
        public Collection<byte[]> values() {
            return map.values();
        }

        @Override
        public Set<Map.Entry<byte[], byte[]>> entrySet() {
            LinkedHashSet<Map.Entry<byte[], byte[]>> set = new LinkedHashSet<Map.Entry<byte[],byte[]>>();
            for (Map.Entry<ByteArrayWrapper, byte[]> entry : map.entrySet()) {
                set.add(new SimpleImmutableEntry<byte[], byte[]>(
                        entry.getKey().array, entry.getValue()));
            }
            return set;
        }
    }

}
