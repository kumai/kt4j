package kt4j;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.base64.Base64;

/**
 * A utility class that provides various common operations related with bytes.
 * 
 * @author kumai
 */
public class Bytes {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private Bytes() {}
    
    /**
     * Returns a byte array encoded by UTF-8 for the specified string.
     */
    public static byte[] utf8(String str) {
        if (str != null) {
            return str.getBytes(UTF8);
        } else {
            return null;
        }
    }
    
    /**
     * Returns a string for the specified bytes encoded by UTF-8.
     */
    public static String utf8(byte[] data) {
        if (data != null) {
            return new String(data, UTF8);
        } else {
            return null;
        }
    }
    
    /**
     * Gets 32-bit big-endian bytes for the specified integer value.
     */
    public static byte[] bytesWithInt(int value) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((value >>> 24) & 0xFF);
        bytes[1] = (byte) ((value >>> 16) & 0xFF);
        bytes[2] = (byte) ((value >>>  8) & 0xFF);
        bytes[3] = (byte) ((value >>>  0) & 0xFF);
        return bytes;
    }
    
    /**
     * Gets 64-bit big-endian bytes for the specified long integer value.
     */
    public static byte[] bytesWithLong(long value) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) ((value >>> 56) & 0xFF);
        bytes[1] = (byte) ((value >>> 48) & 0xFF);
        bytes[2] = (byte) ((value >>> 40) & 0xFF);
        bytes[3] = (byte) ((value >>> 32) & 0xFF);
        bytes[4] = (byte) ((value >>> 24) & 0xFF);
        bytes[5] = (byte) ((value >>> 16) & 0xFF);
        bytes[6] = (byte) ((value >>>  8) & 0xFF);
        bytes[7] = (byte) ((value >>>  0) & 0xFF);
        return bytes;
    }
    
    /**
     * Encodes the specified bytes to <a href="http://en.wikipedia.org/wiki/Base64">Base64</a> notation.
     */
    public static byte[] base64Encode(byte[] bytes) {
        ChannelBuffer src = ChannelBuffers.wrappedBuffer(bytes);
        ChannelBuffer encodedBuff = Base64.encode(src);
        byte[] encodedBytes = new byte[encodedBuff.readableBytes()];
        encodedBuff.readBytes(encodedBytes);
        return encodedBytes;
    }

    /**
     * Decodes the specified bytes from <a href="http://en.wikipedia.org/wiki/Base64">Base64</a> notation.
     */
    public static byte[] base64Decode(byte[] encodedBytes) {
        ChannelBuffer src = ChannelBuffers.wrappedBuffer(encodedBytes);
        ChannelBuffer decodedBuff = Base64.decode(src);
        byte[] decodedBytes = new byte[decodedBuff.readableBytes()];
        decodedBuff.readBytes(decodedBytes);
        return decodedBytes;
    }

    /**
     * A implementation that wraps byte array. This can be used as keys of {@link Map}.
     * 
     * @author kumai
     */
    public static class ByteArrayWrapper {
        /**
         * The byte array that backs this wrapper.
         */
        public final byte[] array;
        
        /**
         * Creates a new instance that wraps the specified byte array. 
         */
        public ByteArrayWrapper(byte[] array) {
            this.array = array;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ByteArrayWrapper) {
                return Arrays.equals(array, ((ByteArrayWrapper) obj).array);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return Arrays.toString(array);
        }
        
        /**
         * Returns the length of this byte array.
         */
        public int length() {
            return array.length;
        }
    }
}
