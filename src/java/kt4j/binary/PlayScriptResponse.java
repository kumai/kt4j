package kt4j.binary;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.AbstractMap.SimpleImmutableEntry;

import kt4j.Bytes.ByteArrayWrapper;

class PlayScriptResponse extends BinaryResponse {
    private final Map<ByteArrayWrapper, byte[]> results = new HashMap<ByteArrayWrapper, byte[]>();

    PlayScriptResponse(long numRecords) {
        super(PLAY_SCRIPT, numRecords);
    }
    
    void put(byte[] key, byte[] value) {
        results.put(new ByteArrayWrapper(key), value);
    }
    
    byte[] getValue(byte[] key) {
        return results.get(new ByteArrayWrapper(key));
    }
    
    Map<byte[], byte[]> getValues() {
        return new KeyValueWrapper(results);
    }

    private static class KeyValueWrapper extends AbstractKeyValueWrapper<byte[]> {
        KeyValueWrapper(Map<ByteArrayWrapper, byte[]> source) {
            super(source);
        }

        @Override
        public byte[] get(Object key) {
            if (key instanceof byte[]) {
                return source.get(new ByteArrayWrapper((byte[]) key));
            }
            return null;
        }

        @Override
        public Set<byte[]> keySet() {
            HashSet<byte[]> set = new HashSet<byte[]>();
            for (ByteArrayWrapper key  : source.keySet()) {
                set.add(key.array);
            }
            return Collections.unmodifiableSet(set);
        }

        @Override
        public Collection<byte[]> values() {
            HashSet<byte[]> set = new HashSet<byte[]>();
            for (byte[] value : source.values()) {
                set.add(value);
            }
            return Collections.unmodifiableSet(set);
        }

        @Override
        public Set<Map.Entry<byte[], byte[]>> entrySet() {
            HashSet<Map.Entry<byte[], byte[]>> set = new HashSet<Map.Entry<byte[], byte[]>>();
            for (Map.Entry<ByteArrayWrapper, byte[]> entry : source.entrySet()) {
                set.add(new SimpleImmutableEntry<byte[], byte[]>(
                        entry.getKey().array, entry.getValue()));
            }
            return Collections.unmodifiableSet(set);
        }
    }
}
