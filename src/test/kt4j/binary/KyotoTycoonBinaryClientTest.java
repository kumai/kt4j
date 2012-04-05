package kt4j.binary;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class KyotoTycoonBinaryClientTest {

    final KyotoTycoonBinaryClient client = new KyotoTycoonBinaryClient("127.0.0.1", 1978);
    //final KyotoTycoonBinaryClient client = new KyotoTycoonBinaryClient("10.33.16.141", 1978);
    /*
    final KyotoTycoonBinaryClient client = new KyotoTycoonBinaryClient(
            new InetSocketAddress("10.33.16.141", 1978),
            new InetSocketAddress("10.33.16.141", 1979));
            */

    @Before
    public void setup() {
        client.start();
        client.clear();
    }
    
    @After
    public void tearDown() {
        client.stop();
    }
    
    @Test
    @Ignore
    public void test() throws Exception {
        String key = "bin_key";
        String value = "bin_value";
        client.set(key, value);
        assertEquals(value, client.get(key));
        assertTrue(client.remove(key));
    }
    
    @Test
    @Ignore
    public void testBulkOperations() throws Exception {
        byte[] key1 = "bulk1".getBytes("UTF-8");
        byte[] val1 = "v1".getBytes("UTF-8");
        byte[] key2 = "bulk2".getBytes("UTF-8");
        byte[] val2 = "v2".getBytes("UTF-8");
        byte[] key3 = "bulk3".getBytes("UTF-8");
        byte[] val3 = "v3".getBytes("UTF-8");
        
        HashMap<byte[], byte[]> kv = new HashMap<byte[], byte[]>();
        kv.put(key1, val1);
        kv.put(key2, val2);
        kv.put(key3, val3);
        client.setBulk(kv, null, true);
        
        Map<byte[], byte[]> result = client.getBulk(Arrays.asList(key1, key2, key3));
        assertEquals(3, result.size());
        assertArrayEquals(val1, result.get(key1));
        assertArrayEquals(val2, result.get(key2));
        assertArrayEquals(val3, result.get(key3));
        
        assertEquals(3, client.removeBulk(Arrays.asList(key1, key2, key3)));
        assertNull(client.get(key1));
        assertNull(client.get(key2));
        assertNull(client.get(key3));
    }
    
    @Test
    @Ignore
    public void testBulkStringOperations() throws Exception {
        String key1 = "bulk1";
        String val1 = "v1";
        String key2 = "bulk2";
        String val2 = "v2";
        String key3 = "bulk3";
        String val3 = "v3";
        
        HashMap<String, String> kv = new HashMap<String, String>();
        kv.put(key1, val1);
        kv.put(key2, val2);
        kv.put(key3, val3);
        client.setBulkString(kv, null, false);
        
        Map<String, String> result = client.getBulkString(Arrays.asList(key1, key2, key3), false);
        assertEquals(val1, result.get(key1));
        assertEquals(val2, result.get(key2));
        assertEquals(val3, result.get(key3));
        
        assertEquals(3, client.removeBulkString(Arrays.asList(key1, key2, key3), false));
        
        assertEquals(0, client.getBulkString(Arrays.asList(key1, key2, key3), false).size());
    }

    @Test
    @Ignore
    public void testPlayScript() {
        byte[] key1 = new byte[] {0, 1};
        byte[] key2 = new byte[] {1, 2};
        byte[] value1 = new byte[] {1, 0};
        byte[] value2 = new byte[] {2, 1};
        HashMap<byte[], byte[]> params = new HashMap<byte[], byte[]>();
        params.put(key1, value1);
        params.put(key2, value2);
        
        Map<byte[], byte[]> result = client.playScript("echo", params);
        
        assertEquals(2, result.size());
        for (byte[] key : result.keySet()) {
            assertTrue(Arrays.equals(key, key1) || Arrays.equals(key, key2));
            byte[] value = result.get(key);
            assertTrue(Arrays.equals(value, value1) || Arrays.equals(value, value2));
        }
    }
}
