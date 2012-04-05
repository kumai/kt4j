package kt4j.binary;

import java.util.Map;

import kt4j.Bytes.ByteArrayWrapper;

abstract class AbstractKeyValueWrapper<V> implements Map<byte[], byte[]> {
    protected final Map<ByteArrayWrapper, V> source;

    protected AbstractKeyValueWrapper(Map<ByteArrayWrapper, V> source) {
        this.source = source;
    }

    @Override
    public int size() {
        return source.size();
    }

    @Override
    public boolean isEmpty() {
        return source.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return (get(key) != null);
    }

    @Override
    public boolean containsValue(Object value) {
        return source.containsValue(value);
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("This map is unmodifiable.");
    }

    @Override
    public byte[] remove(Object key) {
        throw new UnsupportedOperationException("This map is unmodifiable.");
    }

    @Override
    public void putAll(Map<? extends byte[], ? extends byte[]> m) {
        throw new UnsupportedOperationException("This map is unmodifiable.");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This map is unmodifiable.");
    }

}
