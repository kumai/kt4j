package kt4j.binary;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kt4j.Bytes.ByteArrayWrapper;

class GetBulkResponse extends BinaryResponse {
    private final Map<ByteArrayWrapper, Record> results = new HashMap<ByteArrayWrapper, Record>();
    
    GetBulkResponse(long numHits) {
        super(GET_BULK, numHits);
    }

    void put(byte[] key, byte[] value, long xt) {
        put(key, value, xt, (short) 0);
    }

    void put(byte[] key, byte[] value, long xt, int dbidx) {
        results.put(new ByteArrayWrapper(key), new Record(dbidx, key, value, xt));
    }
    
    byte[] getValue(byte[] key) {
        Record record = results.get(new ByteArrayWrapper(key));
        return (record != null) ? record.value : null;
    }
    
    Map<byte[], byte[]> getValues() {
        return new KeyValueWrapper(results);
    }
    
    private static class KeyValueWrapper extends AbstractKeyValueWrapper<Record> {
        KeyValueWrapper(Map<ByteArrayWrapper, Record> source) {
            super(source);
        }

        @Override
        public byte[] get(Object key) {
            if (key instanceof byte[]) {
                Record rec = source.get(new ByteArrayWrapper((byte[]) key));
                if (rec != null) {
                    return rec.value;
                }
            }
            return null;
        }

        @Override
        public Set<byte[]> keySet() {
            HashSet<byte[]> set = new HashSet<byte[]>();
            for (Record rec : source.values()) {
                set.add(rec.key);
            }
            return Collections.unmodifiableSet(set);
        }

        @Override
        public Collection<byte[]> values() {
            HashSet<byte[]> set = new HashSet<byte[]>();
            for (Record rec : source.values()) {
                set.add(rec.value);
            }
            return Collections.unmodifiableSet(set);
        }

        @Override
        public Set<Map.Entry<byte[], byte[]>> entrySet() {
            HashSet<Map.Entry<byte[], byte[]>> set = new HashSet<Map.Entry<byte[], byte[]>>();
            for (Record rec : source.values()) {
                set.add(new SimpleImmutableEntry<byte[], byte[]>(rec.key, rec.value));
            }
            return Collections.unmodifiableSet(set);
        }
    }
}
