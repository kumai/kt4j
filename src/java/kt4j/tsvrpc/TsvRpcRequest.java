package kt4j.tsvrpc;

import static kt4j.Bytes.utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kt4j.Bytes;
import kt4j.ExpirationTime;
import kt4j.Request;

/**
 * 
 * @author kumai
 *
 */
class TsvRpcRequest extends Request {
    private final TsvColumnCodec columnCodec = TsvColumnCodec.BASE_64;
    
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String XT = "xt";
    private static final String NUM = "num";
    private static final String ORIG = "orig";
    private static final String ATOMIC = "atomic";
    private static final String OVAL = "oval";
    private static final String NVAL = "nval";
    
    private static final byte[] EMPTY_VALUE = new byte[0];
    
    private final Map<Object, Object> values = new HashMap<Object, Object>();
    
    TsvRpcRequest(Command operation) {
        super(operation);
    }
    
    public void setDatabaseIdentifier(String db) {
        if (db != null) {
            values.put("DB", utf8(db));
        } else {
            values.remove("DB");
        }
    }
    
    public void setRpcParam(Object name, Object value) {
        values.put(name, value);
    }
    
    public String getPath() {
        return "/rpc/" + command.procedureName;
    }
    
    public String getContentType() {
        return columnCodec.contentType;
    }
    
    public byte[] getEncodedContent() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            for (Map.Entry<Object, Object> entry : values.entrySet()) {
                out.write(columnCodec.encode((entry.getKey())));
                out.write('\t');
                out.write(columnCodec.encode(entry.getValue()));
                out.write('\r');
                out.write('\n');
            }
        } catch (IOException e) {
            return null;
        }
        
        return out.toByteArray();
    }
    
    static TsvRpcRequest createGet(byte[] key) {
        TsvRpcRequest request = new TsvRpcRequest(Command.GET);
        request.setRpcParam(KEY, key);
        return request;
    }
    
    static TsvRpcRequest createGetBulk(List<byte[]> keys, boolean atomic) {
        TsvRpcRequest request = new TsvRpcRequest(Command.GET_BULK);
        
        if (atomic) {
            request.setRpcParam(ATOMIC, EMPTY_VALUE);
        }
        
        for (byte[] key : keys) {
            byte[] keyBuff = new byte[key.length + 1];
            keyBuff[0] = '_';
            System.arraycopy(key, 0, keyBuff, 1, key.length);
            request.setRpcParam(keyBuff, EMPTY_VALUE);
        }
        
        return request;
    }
    
    static TsvRpcRequest createSet(byte[] key, byte[] value, ExpirationTime xt) {
        TsvRpcRequest request = new TsvRpcRequest(Command.SET);
        request.setRpcParam(KEY, key);
        request.setRpcParam(VALUE, value);
        if (xt != null) {
            request.setRpcParam(XT, xt.toString());
        }
        return request;
    }
    
    static TsvRpcRequest createSetBulk(Map<?, ?> keyValuePairs, ExpirationTime xt, boolean atomic) {
        TsvRpcRequest request = new TsvRpcRequest(Command.SET_BULK);
        
        if (xt != null) {
            request.setRpcParam(XT, xt.toString());
        }
        
        if (atomic) {
            request.setRpcParam(ATOMIC, EMPTY_VALUE);
        }
        
        for (Map.Entry<?, ?> kv : keyValuePairs.entrySet()) {
            byte[] key = (kv.getKey() instanceof String) ?
                    Bytes.utf8((String) kv.getKey()) : (byte[]) kv.getKey();
            byte[] keyName = new byte[key.length + 1];
            keyName[0] = '_';
            System.arraycopy(key, 0, keyName, 1, key.length);
            request.setRpcParam(keyName, kv.getValue());
        }
        
        return request;
    }
    
    static TsvRpcRequest createRemove(byte[] key) {
        TsvRpcRequest request = new TsvRpcRequest(Command.REMOVE);
        request.setRpcParam(KEY, key);
        return request;
    }
    
    static TsvRpcRequest createRemoveBulk(List<byte[]> keys, boolean atomic) {
        TsvRpcRequest request = new TsvRpcRequest(Command.REMOVE_BULK);
        
        if (atomic) {
            request.setRpcParam(ATOMIC, EMPTY_VALUE);
        }
        
        for (byte[] key : keys) {
            byte[] keyBuff = new byte[key.length + 1];
            keyBuff[0] = '_';
            System.arraycopy(key, 0, keyBuff, 1, key.length);
            request.setRpcParam(keyBuff, EMPTY_VALUE);
        }
        
        return request;
    }
    
    static final TsvRpcRequest createIncrement(byte[] key, long num, long origin, ExpirationTime xt) {
        TsvRpcRequest request = new TsvRpcRequest(Command.INCREMENT);
        request.setRpcParam(KEY, key);
        request.setRpcParam(NUM, String.valueOf(num));
        request.setRpcParam(ORIG, String.valueOf(origin));
        
        if (xt != null) {
            request.setRpcParam(XT, xt.toString());
        }
        
        return request;
    }
    
    static TsvRpcRequest createIncrementDouble(byte[] key, double num, Double origin, ExpirationTime xt) {
        TsvRpcRequest request = new TsvRpcRequest(Command.INCREMENT_DOUBLE);
        request.setRpcParam(KEY, key);
        request.setRpcParam(NUM, String.valueOf(num));
        
        if (origin != null) {
            if (origin.doubleValue() == Double.POSITIVE_INFINITY) {
                request.setRpcParam(ORIG, "set");
            } else if (origin.doubleValue() == Double.NEGATIVE_INFINITY) {
                request.setRpcParam(ORIG, "try");
            } else {
                request.setRpcParam(ORIG, origin.toString());
            }
        }
        
        if (xt != null) {
            request.setRpcParam(XT, xt.toString());
        }
        
        return request;
    }

    static TsvRpcRequest createCas(byte[] key, byte[] expect, byte[] update, ExpirationTime xt) {
        TsvRpcRequest request = new TsvRpcRequest(Command.CAS);
        request.setRpcParam(KEY, key);
        request.setRpcParam(OVAL, expect);
        request.setRpcParam(NVAL, update);

        if (xt != null) {
            request.setRpcParam(XT, xt.toString());
        }
        
        return request;
    }
    
    static TsvRpcRequest createClear() {
        TsvRpcRequest request = new TsvRpcRequest(Command.CLEAR);
        return request;
    }
    
    static TsvRpcRequest createSeize(byte[] key) {
        TsvRpcRequest request = new TsvRpcRequest(Command.SEIZE);
        request.setRpcParam(KEY, key);
        return request;
    }
    
    static TsvRpcRequest createReplace(byte[] key, byte[] value, ExpirationTime xt) {
        TsvRpcRequest request = new TsvRpcRequest(Command.REPLACE);
        request.setRpcParam(KEY, key);
        request.setRpcParam(VALUE, value);
        
        if (xt != null) {
            request.setRpcParam(XT, xt.toString());
        }
        
        return request;
    }
    
    static TsvRpcRequest createAdd(byte[] key, byte[] value, ExpirationTime xt) {
        TsvRpcRequest request = new TsvRpcRequest(Command.ADD);
        request.setRpcParam(KEY, key);
        request.setRpcParam(VALUE, value);
        
        if (xt != null) {
            request.setRpcParam(XT, xt.toString());
        }
        
        return request;
    }
    
    static TsvRpcRequest createMatchPrefix(byte[] prefix, long max) {
        TsvRpcRequest request = new TsvRpcRequest(Command.MATCH_PREFIX);
        request.setRpcParam("prefix", prefix);
        if (max > -1) {
            request.setRpcParam("max", String.valueOf(max));
        }
        return request;
    }
    
    static TsvRpcRequest createMatchRegex(byte[] regex, long max) {
        TsvRpcRequest request = new TsvRpcRequest(Command.MATCH_REGEX);
        request.setRpcParam("regex", regex);
        if (max > -1) {
            request.setRpcParam("max", String.valueOf(max));
        }
        return request;
    }
    
    static TsvRpcRequest createPlayScript(String procedureName, Map<byte[], ?> params) {
        TsvRpcRequest request = new TsvRpcRequest(Command.PLAY_SCRIPT);
        request.setRpcParam("name", procedureName);
        if (params != null) {
            for (Map.Entry<byte[], ?> kv : params.entrySet()) {
                byte[] keyName = new byte[kv.getKey().length + 1];
                keyName[0] = '_';
                System.arraycopy(kv.getKey(), 0, keyName, 1, kv.getKey().length);
                request.setRpcParam(keyName, kv.getValue());
            }
        }
        return request;
    }
    
    static TsvRpcRequest createVoid() {
        TsvRpcRequest request = new TsvRpcRequest(Command.VOID);
        return request;
    }

    static TsvRpcRequest createSynchronize(boolean hard, String command) {
        TsvRpcRequest request = new TsvRpcRequest(Command.SYNCHRONIZE);
        if (hard) {
            request.setRpcParam("hard", EMPTY_VALUE);
        }
        if (command != null) {
            request.setRpcParam("command", command);
        }
        return request;
    }

    static TsvRpcRequest createVacuum(int step) {
        TsvRpcRequest request = new TsvRpcRequest(Command.VACUUM);
        request.setRpcParam("step", String.valueOf(step));
        return request;
    }

    static TsvRpcRequest createStatus() {
        TsvRpcRequest request = new TsvRpcRequest(Command.STATUS);
        return request;
    }

    static TsvRpcRequest createReport() {
        TsvRpcRequest request = new TsvRpcRequest(Command.REPORT);
        return request;
    }

    static TsvRpcRequest createEcho(Map<?, ?> input) {
        TsvRpcRequest request = new TsvRpcRequest(Command.ECHO);
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            request.setRpcParam(entry.getKey(), entry.getValue());
        }
        return request;
    }
}
