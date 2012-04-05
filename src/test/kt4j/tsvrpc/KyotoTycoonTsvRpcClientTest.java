package kt4j.tsvrpc;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kt4j.Bytes;
import kt4j.ExpirationTime;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class KyotoTycoonTsvRpcClientTest {
    
    final KyotoTycoonTsvRpcClient client = new KyotoTycoonTsvRpcClient("127.0.0.1", 1978);
    //final KyotoTycoonTsvRpcClient client = new KyotoTycoonTsvRpcClient("10.33.16.141", 1978);
    
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
        String key = "KyotoTycoonTsvRpcClientTest";
        String value = "this is a test value.";
        
        client.set(key, value);
        
        assertEquals(value, client.get(key));
        assertTrue(client.remove(key));
        assertFalse(client.remove(key));
    }

    @Test
    @Ignore
    public void testSetWithExpirationTime() throws Exception {
        String key = "testSetWithExpirationTime";
        String value = "this is a test value.";
        
        client.set(key, value, ExpirationTime.after(1));
        assertEquals(value, client.get(key));
        Thread.sleep(2000);
        assertNull(client.get(key));
    }

    @Test
    @Ignore
    public void testBulkOperations() throws Exception {
        byte[] key1 = Bytes.utf8("testSetBulk 1");
        byte[] key2 = Bytes.utf8("testSetBulk 2");
        byte[] value1 = Bytes.utf8("this is a test value 1.");
        byte[] value2 = Bytes.utf8("this is a test value 2.");
        
        HashMap<byte[], byte[]> values = new HashMap<byte[], byte[]>();
        values.put(key1, value1);
        values.put(key2, value2);
        
        client.setBulk(values, null, true);
        
        Map<byte[], byte[]> result = client.getBulk(Arrays.asList(key1, key2));
        assertEquals(2, result.size());
        assertArrayEquals(value1, result.get(key1));
        assertArrayEquals(value2, result.get(key2));
        
        assertEquals(2, client.removeBulk(Arrays.asList(key1, key2)));
        assertNull(client.get(key1));
        assertNull(client.get(key2));
    }

    @Test
    @Ignore
    public void testBulkStringOperations() throws Exception {
        String key1 = "key1";
        String key2 = "key2";
        String value1 = "value1";
        String value2 = "value2";
        
        HashMap<String, String> values = new HashMap<String, String>();
        values.put(key1, value1);
        values.put(key2, value2);
        
        client.setBulkString(values, null, true);
        
        Map<String, String> result = client.getBulkString(Arrays.asList(key1, key2));
        
        assertEquals(2, result.size());
        assertEquals(value1, result.get(key1));
        assertEquals(value2, result.get(key2));
    }
    
    @Test
    @Ignore
    public void testClear() {
        client.set("A", "1");
        client.set("B", "1");
        
        assertNotNull(client.get("A"));
        assertNotNull(client.get("B"));
        
        client.clear();
        
        assertNull(client.get("A"));
        assertNull(client.get("B"));
    }

    @Test
    @Ignore
    public void testIncrement() throws Exception {
        String key = "testIncrement";
        
        assertEquals(1, client.increment(key));
        assertEquals(2, client.increment(key));
        
        assertTrue(client.remove(key));
    }

    @Test
    @Ignore
    public void testIncrementWithXtNumOrigin() throws Exception {
        String key = "testIncrement";
        long xtSec = 2;
        assertEquals(110, client.increment(key, 10, 100, ExpirationTime.after(xtSec)));
        Thread.sleep((xtSec+1)*1000);
        
        assertNull(client.get(key));
    }
    
    @Test
    @Ignore
    public void testIncrementDouble() throws Exception {
        String key = "testIncrementDouble";
        
        double result = client.incrementDouble(key, 1.0, null);
        assertEquals(1.0, result, 0);

        result = client.incrementDouble(key, 1.5, null);
        assertEquals(2.5, result, 0);
    }

    @Test
    @Ignore
    public void testCas() throws Exception {
        String key = "testCas";
        client.set(key, "expected");
        
        assertTrue(client.cas(key, "expected", "updated"));
        assertEquals("updated", client.get(key));
    }

    @Test
    @Ignore
    public void testCasFailedToAssumpt() throws Exception {
        String key = "testCas";
        
        assertFalse(client.cas(key, "expected", "update"));
    }
    
    @Test
    @Ignore
    public void testSeize() throws Exception {
        String key = "testSeize";
        String value = "val";
        assertNull(client.seize(key));
        client.set(key, value);
        String result = client.seize(key);
        assertEquals(value, result);
        assertNull(client.get(key));
    }
    
    @Test
    @Ignore
    public void testReplace() throws Exception {
        String key = "testReplace";
        client.set(key, "A");
        String newValue = "B";

        assertTrue(client.replace(key, newValue));
        assertEquals(newValue, client.get(key));
        
        assertFalse(client.replace("XXX", "X"));
    }
    
    @Test
    @Ignore
    public void testReplaceWithXT() throws Exception {
        String key = "testReplace";
        client.set(key, "A");
        String newValue = "B";

        assertTrue(client.replace(key, newValue, ExpirationTime.after(1)));
        assertEquals(newValue, client.get(key));
        Thread.sleep(2000);
        assertNull(client.get(key));
    }
    
    @Test
    @Ignore
    public void testAdd() throws Exception {
        String key = "testAdd";
        String newValue = "B";

        assertTrue(client.add(key, newValue));
        assertEquals(newValue, client.get(key));
        assertFalse(client.add(key, newValue));
    }
    
    @Test
    @Ignore
    public void testAddWithXT() throws Exception {
        String key = "testAdd";
        String newValue = "B";

        assertTrue(client.add(key, newValue, ExpirationTime.after(1)));
        assertEquals(newValue, client.get(key));
        Thread.sleep(2000);
        assertNull(client.get(key));
    }
    
    @Test
    @Ignore
    public void testMatchRegex() throws Exception {
        String key1 = "key1";
        String key2 = "KEy2";
        String key3 = "key3";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value2";
        
        client.set(key1, value1);
        client.set(key2, value2);
        client.set(key3, value3);
        
        List<String> result = client.matchRegex("[a-zA-Z].y[12]");
        
        assertEquals(2, result.size());
        assertTrue(result.contains(key1));
        assertTrue(result.contains(key2));
    }
    
    @Test
    @Ignore
    public void testMatchRegexWithMax() throws Exception {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value2";
        
        client.set(key1, value1);
        client.set(key2, value2);
        client.set(key3, value3);
        
        List<String> result = client.matchRegex("k.*", 1);
        
        assertEquals(1, result.size());
        assertTrue(result.contains(key1) || result.contains(key2) || result.contains(key3));
    }
    
    @Test
    @Ignore
    public void testMatchPrefix() throws Exception {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        String key11 = "key11";
        
        client.set(key1, "v1");
        client.set(key2, "v2");
        client.set(key3, "v3");
        client.set(key11, "v11");
        
        List<String> result = client.matchPrefix("key1");
        
        assertEquals(2, result.size());
        assertTrue(result.contains(key1) || result.contains(key11));
    }
    
    @Test
    @Ignore
    public void testMatchPrefixWithMax() throws Exception {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        String key11 = "key11";
        
        client.set(key1, "v1");
        client.set(key2, "v2");
        client.set(key3, "v3");
        client.set(key11, "v11");
        
        List<String> result = client.matchPrefix("key1", 1);
        
        assertEquals(1, result.size());
        assertTrue(result.contains(key1) || result.contains(key11));
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
    
    @Test
    @Ignore
    public void testPlayScriptString() {
        String key1 = "key1";
        String key2 = "key2";
        String value1 = "value1";
        String value2 = "value2";
        
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(key1, value1);
        params.put(key2, value2);
        
        Map<String, String> result = client.playScriptString("echo", params);
        
        assertEquals(2, result.size());
        assertEquals(value1, result.get(key1));
        assertEquals(value2, result.get(key2));
    }
    
    @Test
    @Ignore
    public void testPing() {
        client.ping();
    }
    
    @Test
    @Ignore
    public void testSynchronize() {
        client.synchronize(true, null);
    }
    
    @Test
    @Ignore
    public void testVacuum() {
        client.vacuum(-1);
    }
    
    @Test
    @Ignore
    public void testGetStatus() {
        client.set("key1", "value1");
        client.set("key2", "value2");
        
        Map<String, String> status = client.getStatus();
        assertEquals("2", status.get("count"));
    }
    
    @Test
    @Ignore
    public void testGetReport() {
        Map<String, String> result = client.getReport();
        assertNotNull(result.get("db_0"));
    }
    
    @Test
    @Ignore
    public void testEchoString() {
        HashMap<String, String> input = new HashMap<String, String>();
        input.put("k1", "v1");
        input.put("k2", "v2");
        Map<String, String> result = client.echoString(input);
        assertEquals(2, result.size());
        assertEquals("v1", result.get("k1"));
        assertEquals("v2", result.get("k2"));
    }
}
