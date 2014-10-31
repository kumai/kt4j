package kt4j.tsvrpc;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import kt4j.Bytes;

/**
 * 
 * @author kumai
 *
 */
public enum TsvColumnCodec {

    BASE_64("text/tab-separated-values; colenc=B") {
        @Override
        public byte[] encode(Object value) {
            byte[] bytes = toEncodeableBytes(value);
            return Bytes.base64Encode(bytes);
        }

        @Override
        public byte[] decode(byte[] value) {
            return Bytes.base64Decode(value);
        }
        
    },
    
    URL_ENCODING("text/tab-separated-values; colenc=U") {
        @Override
        public byte[] encode(Object value) {
            String str;
            if (value instanceof String) {
                str = (String) value;
            } else {
                byte[] buf = toEncodeableBytes(value);
                str = new String(buf, charset());
            }
            
            String encoded;
            try {
                encoded = URLEncoder.encode(str, charset().name());
            } catch (UnsupportedEncodingException e) {
                throw new Error("Shouldn't reached here.", e);
            }
            
            return encoded.getBytes(charset());
        }

        @Override
        public byte[] decode(byte[] value) {
            String str = new String(value, charset());
            String decoded;
            try {
                decoded = URLDecoder.decode(str, charset().name());
            } catch (UnsupportedEncodingException e) {
                throw new Error("Shouldn't reached here.", e);
            }
            
            return decoded.getBytes(charset());
        }
    },
    
    NONE("text/tab-separated-values") {
        @Override
        public byte[] encode(Object value) {
            return toEncodeableBytes(value);
        }

        @Override
        public byte[] decode(byte[] value) {
            return value;
        }
    },
    ;
    
    static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    
    public final String contentType;
    
    private TsvColumnCodec(String contentType) {
        this.contentType = contentType;
    }
    
    public Charset charset() {
        return DEFAULT_CHARSET;
    }
    
    public abstract byte[] encode(Object value);

    public abstract byte[] decode(byte[] value);
    
    public static final TsvColumnCodec forContentType(String contentType)
            throws NullPointerException, IllegalArgumentException {
        for (TsvColumnCodec enc : TsvColumnCodec.values()) {
            if (enc.contentType.equalsIgnoreCase(contentType)) {
                return enc;
            }
        }

        throw new IllegalArgumentException("Unsupported Content-Type: " + contentType);
    }
    
    /**
     * Transform a value to be encoded.
     * Supported types are below:
     * <li>{@code String}
     * <li>{@code ByteBuffer}
     * <li>{@code byte[]}
     * @param value
     * @return
     */
    protected byte[] toEncodeableBytes(Object value) {
        byte[] bytes = null;
        
        if (value instanceof byte[]) {
            bytes = (byte[]) value;
        } else if (value instanceof String) {
            bytes = ((String) value).getBytes(charset());
        } else if (value instanceof ByteBuffer) {
            ByteBuffer bb = (ByteBuffer) value;
            bytes = new byte[bb.limit()];
            bb.get(bytes);
        } else {
            throw new UnsupportedEncodingValueTypeException(value.getClass());
        }
        
        return bytes;
    }
    
    public static class UnsupportedEncodingValueTypeException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public UnsupportedEncodingValueTypeException(Class<?> valueType) {
            super("Unsupported type to encode: " + valueType.toString());
        }
    }
}
