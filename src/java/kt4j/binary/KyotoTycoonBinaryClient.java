package kt4j.binary;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import kt4j.Bytes;
import kt4j.ExpirationTime;
import kt4j.Operation;
import kt4j.KyotoTycoonOperationFailedException;
import kt4j.tsvrpc.KyotoTycoonTsvRpcClient;

/**
 * Kyoto Tycoon client using binary protocol and TSV-RPC.
 * 
 * This class supports Kyoto Tycoon's binary protocol.
 * When operations that Kyoto Tycoon doesn't support with binary protocol are required, this class calls the Kyoto Tycooon using TSV-RPC protocol. 
 * 
 * @author kumai
 */
public class KyotoTycoonBinaryClient extends KyotoTycoonTsvRpcClient {
    private int databaseIndex;
    
    public KyotoTycoonBinaryClient(String hostname, int port) {
        super(hostname, port);
    }

    /**
     * Use {@link #setDatabase(String, int)} instead.
     * Always throws {@link UnsupportedOperationException}.
     */
    @Override
    public void setDatabase(String database) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("setDatabase(String)");
    }

    /**
     * Sets the target database.
     * With the binary protocol, uses index of database instead of database string.
     * 
     * @param database the database identifier
     * @param databaseIndex the database index
     */
    public void setDatabase(String database, int databaseIndex) {
        if (databaseIndex < 0) {
            throw new IllegalArgumentException("databaseIndex");
        }
        super.setDatabase(database);
        this.databaseIndex = databaseIndex;
    }
    
    @Override
    public void set(byte[] key, byte[] value, ExpirationTime xt) {
        SetBulkRequest setbulk = new SetBulkRequest(key, value, xt, databaseIndex);
        Operation operation = call(setbulk);
        if (!operation.isSucceeded()) {
            throw new KyotoTycoonOperationFailedException("Failed to set(bin): key=" + Arrays.toString(key));
        }
    }

    /**
     * Store records at once.
     * <p>
     * When <code>atomic</code> parameter is true, this method calls 
     * the Kyoto Tycoon server using TSV-RPC since Kyoto Tycoon's binary protocol doesn't support
     * <code>atomic</code> operation.
     * When <code>atomic</code> parameter is false, uses binary protocol.
     * </p>
     * @param keyValuePairs
     *      key-value pairs to store.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     * @param atomic
     *          true to perform all operations atomically, or false for non-atomic operations.
     */
    @Override
    public void setBulk(Map<byte[], byte[]> keyValuePairs, ExpirationTime xt, boolean atomic)
            throws KyotoTycoonOperationFailedException {
        if (atomic) {
            super.setBulk(keyValuePairs, xt, atomic);
        } else {
            SetBulkRequest setbulk = new SetBulkRequest();
            for (Map.Entry<byte[], byte[]> entry : keyValuePairs.entrySet()) {
                setbulk.add(entry.getKey(), entry.getValue(), xt, databaseIndex);
            }
            Operation operation = call(setbulk);
            if (!operation.isSucceeded()) {
                throw new KyotoTycoonOperationFailedException("Failed to set_bulk(bin)");
            }
        }
    }

    /**
     * Store records at once.
     * <p>
     * When <code>atomic</code> parameter is true, this method calls 
     * the Kyoto Tycoon server using TSV-RPC since Kyoto Tycoon's binary protocol doesn't support
     * <code>atomic</code> operation.
     * When <code>atomic</code> parameter is false, uses binary protocol.
     * </p>
     * @param keyValuePairs
     *      key-value pairs to store.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     * @param atomic
     *          true to perform all operations atomically, or false for non-atomic operations.
     */
    @Override
    public void setBulkString(Map<String, String> keyValuePairs, ExpirationTime xt, boolean atomic)
            throws KyotoTycoonOperationFailedException {
        if (atomic) {
            super.setBulkString(keyValuePairs, xt, atomic);
        } else {
            SetBulkRequest setbulk = new SetBulkRequest();
            for (Map.Entry<String, String> entry : keyValuePairs.entrySet()) {
                setbulk.add(Bytes.utf8(entry.getKey()), Bytes.utf8(entry.getValue()), xt,
                        databaseIndex);
            }
            Operation operation = call(setbulk);
            if (!operation.isSucceeded()) {
                throw new KyotoTycoonOperationFailedException("Failed to set_bulk(bin)");
            }
        }
    }

    @Override
    public byte[] get(byte[] key) throws NullPointerException, KyotoTycoonOperationFailedException {
        GetBulkRequest getbulk = new GetBulkRequest(key, databaseIndex);
        Operation operation = call(getbulk);
        if (!operation.isSucceeded()) {
            throw new KyotoTycoonOperationFailedException("Failed to get(bin): key=" + Arrays.toString(key));
        }
        GetBulkResponse response = (GetBulkResponse) operation.getResponse();
        return response.getValue(key);
    }

    @Override
    public boolean remove(byte[] key) {
        RemoveBulkRequest request = new RemoveBulkRequest(key, databaseIndex);
        Operation operation = call(request);
        if (!operation.isSucceeded()) {
            throw new KyotoTycoonOperationFailedException("Failed to get(bin): key=" + Arrays.toString(key));
        }
        
        BinaryResponse response = (BinaryResponse) operation.getResponse();
        if (response.getNumber() > 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public long removeBulk(List<byte[]> keys) throws KyotoTycoonOperationFailedException {
        RemoveBulkRequest request = new RemoveBulkRequest();
        for (byte[] key : keys) {
            request.add(key, databaseIndex);
        }
        
        Operation operation = call(request);
        if (!operation.isSucceeded()) {
            throw new KyotoTycoonOperationFailedException("Failed to remove_bulk(bin)");
        }
        
        BinaryResponse response = (BinaryResponse) operation.getResponse();
        return response.getNumber();
    }

    /**
     * Remove records at once.
     * 
     * When <code>atomic</code> parameter is true, this method calls 
     * the Kyoto Tycoon server using TSV-RPC since Kyoto Tycoon's binary protocol doesn't support
     * <code>atomic</code> operation.
     * When <code>atomic</code> parameter is false, uses binary protocol.
     */
    @Override
    public long removeBulk(List<byte[]> keys, boolean atomic)
            throws KyotoTycoonOperationFailedException {
        if (atomic) {
            return super.removeBulk(keys, atomic);
        } else {
            return this.removeBulk(keys);
        }
    }

    @Override
    public Map<byte[], byte[]> getBulk(List<byte[]> keys) throws KyotoTycoonOperationFailedException {
        GetBulkRequest getbulk = new GetBulkRequest();
        for (byte[] key : keys) {
            getbulk.add(key, databaseIndex);
        }
        Operation operation = call(getbulk);
        if (!operation.isSucceeded()) {
            throw new KyotoTycoonOperationFailedException("Failed to get_bulk(bin)");
        }
        
        GetBulkResponse response = (GetBulkResponse) operation.getResponse();
        return response.getValues();
    }

    /**
     * Retreives records at once.
     * 
     * When <code>atomic</code> parameter is true, this method calls 
     * the Kyoto Tycoon server using TSV-RPC since Kyoto Tycoon's binary protocol doesn't support
     * <code>atomic</code> operation.
     * When <code>atomic</code> parameter is false, uses binary protocol.
     */
    @Override
    public Map<byte[], byte[]> getBulk(List<byte[]> keys, boolean atomic)
            throws KyotoTycoonOperationFailedException {
        if (atomic) {
            return super.getBulk(keys, atomic);
        } else {
            return getBulk(keys);
        }
    }

    @Override
    public Map<byte[], byte[]> playScript(String procedureName, Map<byte[], byte[]> params)
            throws KyotoTycoonOperationFailedException {
        PlayScriptRequest playScript = new PlayScriptRequest(procedureName, params);
        Operation operation = call(playScript);
        if (!operation.isSucceeded()) {
            throw new KyotoTycoonOperationFailedException(
                    "Failed to play_script(bin)", operation.getException());
        }
        
        PlayScriptResponse response = (PlayScriptResponse) operation.getResponse();
        return response.getValues();
    }
}
