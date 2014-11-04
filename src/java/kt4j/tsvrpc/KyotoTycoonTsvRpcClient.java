package kt4j.tsvrpc;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kt4j.Bytes;
import kt4j.ExpirationTime;
import kt4j.Operation;
import kt4j.KyotoTycoonOperationFailedException;
import kt4j.AbstractKyotoTycoonClient;

/**
 * Kyoto Tycoon Client using TSV-RPC over HTTP.
 * 
 * @author kumai
 */
public class KyotoTycoonTsvRpcClient extends AbstractKyotoTycoonClient {

    private String database;
    private TsvColumnCodec codec;

    public KyotoTycoonTsvRpcClient(String hostname, int port) {
        this(hostname, port, TsvColumnCodec.BASE_64);
    }
    
    public KyotoTycoonTsvRpcClient(String hostname, int port, TsvColumnCodec codec) {
        super(new InetSocketAddress(hostname, port));
        this.codec = codec;
    }

    /**
     * Sets the target database identifier.
     * 
     * @param database the database identifier.
     */
    public void setDatabase(String database) {
        this.database = database;
    }
    
    @Override
    public void set(byte[] key, byte[] value, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createSet(key, value, xt, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        if (!operation.isSucceeded()) {
            if (operation.getException() != null) {
                throw new KyotoTycoonOperationFailedException(
                        "Failed to set: key=" + Arrays.toString(key), operation.getException());
            } else {
                int status = ((TsvRpcResponse) operation.getResponse()).status;
                throw new KyotoTycoonOperationFailedException(
                        "Failed to set: key=" + Arrays.toString(key) + ", status=" + status);
            }
        }
    }

    @Override
    public void setBulkString(Map<String, String> keyValuePairs, ExpirationTime xt, boolean atomic)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createSetBulk(keyValuePairs, xt, atomic, codec);
        doSetBulk(request);
    }

    @Override
    public void setBulk(Map<byte[], byte[]> keyValuePairs, ExpirationTime xt, boolean atomic)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createSetBulk(keyValuePairs, xt, atomic, codec);
        doSetBulk(request);
    }
    
    private void doSetBulk(TsvRpcRequest request) {
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        if (!operation.isSucceeded()) {
            if (operation.getException() != null) {
                throw new KyotoTycoonOperationFailedException(
                        "Failed to setbulk.", operation.getException());
            } else {
                int status = ((TsvRpcResponse) operation.getResponse()).status;
                throw new KyotoTycoonOperationFailedException(
                        "Failed to setbulk: status=" + status);
            }
        }
    }

    @Override
    public byte[] get(byte[] key) throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createGet(key, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        if (operation.isSucceeded()) {
            TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
            byte[] value = response.getValue();
            return value;
        } else {
            if (operation.getException() != null) {
                throw new KyotoTycoonOperationFailedException(
                        "Failed to get: key=" + Arrays.toString(key), operation.getException());
            } else {
                int status = ((TsvRpcResponse) operation.getResponse()).status;
                throw new KyotoTycoonOperationFailedException(
                        "Failed to get: key=" + Arrays.toString(key) + ", status=" + status);
            }
        }
    }

    @Override
    public byte[] seize(byte[] key) throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createSeize(key, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (operation.isSucceeded()) {
            byte[] value = response.getValue();
            return value;
        } else {
            if (operation.getException() != null) {
                throw new KyotoTycoonOperationFailedException(
                        "Failed to seize: key=" + Arrays.toString(key), operation.getException());
            } else {
                throw new KyotoTycoonOperationFailedException(
                        "Failed to seize: key=" + Arrays.toString(key) + ", status=" + response.status);
            }
        }
    }

    @Override
    public boolean remove(byte[] key) throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createRemove(key, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        if (operation.isSucceeded()) {
            TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
            return (response.status == 200);
        } else {
            if (operation.getException() != null) {
                throw new KyotoTycoonOperationFailedException(
                        "Failed to remove: key=" + Arrays.toString(key), operation.getException());
            } else {
                int status = ((TsvRpcResponse) operation.getResponse()).status;
                throw new KyotoTycoonOperationFailedException(
                        "Failed to remove: key=" + Arrays.toString(key) + ", status=" + status);
            }
        }
    }

    @Override
    public long increment(byte[] key, long num, long origin, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createIncrement(key, num, origin, xt, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        if (operation.isSucceeded()) {
            TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
            if (response.status == 450) {
                throw new KyotoTycoonOperationFailedException(
                        "Failed to increment(the existing record is not compatible): key=" + Arrays.toString(key),
                        operation.getException());
            }
            
            long result = response.getNumber();
            return result;
        } else {
            if (operation.getException() != null) {
                throw new KyotoTycoonOperationFailedException(
                        "Failed to increment: key=" + Arrays.toString(key), operation.getException());
            } else {
                int status = ((TsvRpcResponse) operation.getResponse()).status;
                throw new KyotoTycoonOperationFailedException(
                        "Failed to increment: key=" + Arrays.toString(key) + ", status=" + status);
            }
        }
    }

    @Override
    public double incrementDouble(byte[] key, double num, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        return doIncrementDouble(key, num, null, xt);
    }

    @Override
    public double incrementDouble(byte[] key, double num, double origin, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        return doIncrementDouble(key, num, origin, xt);
    }
    
    private double doIncrementDouble(byte[] key, double num, Double origin, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createIncrementDouble(key, num, origin, xt, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        if (operation.isSucceeded()) {
            TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
            if (response.status == 450) {
                throw new KyotoTycoonOperationFailedException(
                        "Failed to increment_double(the existing record is not compatible): key=" + Arrays.toString(key),
                        operation.getException());
            }
            
            double result = response.getNumberDouble();
            return result;
        } else {
            if (operation.getException() != null) {
                throw new KyotoTycoonOperationFailedException(
                        "Failed to increment_double: key=" + Arrays.toString(key), operation.getException());
            } else {
                int status = ((TsvRpcResponse) operation.getResponse()).status;
                throw new KyotoTycoonOperationFailedException(
                        "Failed to increment_double: key=" + Arrays.toString(key) + ", status=" + status);
            }
        }
    }

    @Override
    public boolean cas(byte[] key, byte[] expect, byte[] update, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createCas(key, expect, update, xt, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        if (operation.isSucceeded()) {
            TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
            if (response.status == 450) {
                // the old value assumption was failed
                return false;
            }
            return true;
        } else {
            int status = ((TsvRpcResponse) operation.getResponse()).status;
            throw new KyotoTycoonOperationFailedException(
                    "Failed to cas: key=" + Arrays.toString(key) + ", status=" + status);
        }
    }

    @Override
    public boolean replace(byte[] key, byte[] value, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createReplace(key, value, xt, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        if (operation.isSucceeded()) {
            TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
            if (response.status == 450) {
                // no record was corresponding
                return false;
            }
            return true;
        } else {
            int status = ((TsvRpcResponse) operation.getResponse()).status;
            throw new KyotoTycoonOperationFailedException(
                    "Failed to replace: key=" + Arrays.toString(key) + ", status=" + status);
        }
    }

    @Override
    public boolean add(byte[] key, byte[] value, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createAdd(key, value, xt, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (operation.isSucceeded()) {
            if (response.status == 450) {
                // the record already exists.
                return false;
            }
            return true;
        } else {
            throw new KyotoTycoonOperationFailedException(
                    "Failed to add: key=" + Arrays.toString(key) + ", status=" + response.status);
        }
    }

    @Override
    public Map<String, String> getBulkString(List<String> keys, boolean atomic)
            throws KyotoTycoonOperationFailedException {
        ArrayList<byte[]> byteKeys = new ArrayList<byte[]>(keys.size());
        for (String key : keys) {
            byteKeys.add(Bytes.utf8(key));
        }
        Map<byte[], byte[]> result = getBulk(byteKeys, atomic);
        return toStringMap(result);
    }

    @Override
    public Map<byte[], byte[]> getBulk(List<byte[]> keys, boolean atomic)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createGetBulk(keys, atomic, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (operation.isSucceeded()) {
            Map<byte[], byte[]> result = response.getBulkResult();
            return Collections.unmodifiableMap(result);
        } else {
            int status = (response != null) ? response.status : -1;
            throw new KyotoTycoonOperationFailedException(
                    "Failed to get_bulk: status=" + status + ", keys=" + keys);
        }
    }

    @Override
    public long removeBulk(List<byte[]> keys, boolean atomic)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createRemoveBulk(keys, atomic, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (operation.isSucceeded()) {
            long num = response.getNumber();
            return num;
        } else {
            int status = (response != null) ? response.status : -1;
            throw new KyotoTycoonOperationFailedException(
                    "Failed to remove_bulk: status=" + status + ", keys=" + keys);
        }
    }

    @Override
    public long removeBulkString(List<String> keys, boolean atomic)
            throws KyotoTycoonOperationFailedException {
        ArrayList<byte[]> keyBytes = new ArrayList<byte[]>();
        for (String key : keys) {
            keyBytes.add(Bytes.utf8(key));
        }
        return removeBulk(keyBytes, atomic);
    }

    @Override
    public void clear() throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createClear(codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        if (!operation.isSucceeded()) {
            int status = ((TsvRpcResponse) operation.getResponse()).status;
            throw new KyotoTycoonOperationFailedException(
                    "Failed to clear: status=" + status);
        }
    }

    @Override
    public List<byte[]> matchRegex(byte[] regex, long max)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createMatchRegex(regex, max, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (operation.isSucceeded()) {
            return new ArrayList<byte[]>(response.getBulkResult().keySet());
        } else {
            throw new KyotoTycoonOperationFailedException(
                    "Failed to match_regex: status=" + ((response != null) ? response.status : "?"));
        }
    }

    @Override
    public List<byte[]> matchPrefix(byte[] prefix, long max)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createMatchPrefix(prefix, max, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (operation.isSucceeded()) {
            return new ArrayList<byte[]>(response.getBulkResult().keySet());
        } else {
            throw new KyotoTycoonOperationFailedException(
                    "Failed to match_preifx: status=" + ((response != null) ? response.status : "?"));
        }
    }

    @Override
    public Map<byte[], byte[]> playScript(String procedureName, Map<byte[], byte[]> params)
            throws KyotoTycoonOperationFailedException {
        Operation operation = call(TsvRpcRequest.createPlayScript(procedureName, params, codec));
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (operation.isSucceeded()) {
            Map<byte[], byte[]> result = response.getBulkResult();
            return Collections.unmodifiableMap(result);
        } else {
            int status = (response != null) ? response.status : -1;
            throw new KyotoTycoonOperationFailedException(
                    "Failed to play_script: status=" + status + ", params=" + params);
        }
    }

    @Override
    public void ping() throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createVoid(codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (response == null || response.status != 200) {
            int status = (response != null) ? response.status : -1;
            throw new KyotoTycoonOperationFailedException("Failed to void: status=" + status);
        }
    }

    @Override
    public void synchronize(boolean hard, String command) throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createSynchronize(hard, command, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (response == null || response.status != 200) {
            int status = (response != null) ? response.status : -1;
            if (status == 450) {
                throw new KyotoTycoonOperationFailedException(
                        "Failed to synchronize - the postprocessing command failed: command=" + command);
            } else {
                throw new KyotoTycoonOperationFailedException("Failed to synchronize: "
                        + "status=" + status + ", hard=" + hard + ", command=" + command);
            }
        }
    }

    @Override
    public void vacuum(int step) throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createVacuum(step, codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (response == null || response.status != 200) {
            int status = (response != null) ? response.status : -1;
            throw new KyotoTycoonOperationFailedException("Failed to vacuum: "
                    + "status=" + status + ", step=" + step);
        }
    }

    @Override
    public Map<String, String> getStatus() throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createStatus(codec);
        if (database != null) {
            request.setDatabaseIdentifier(database);
        }
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (response == null || response.status != 200) {
            int status = (response != null) ? response.status : -1;
            throw new KyotoTycoonOperationFailedException("Failed to status: status=" + status);
        }
        return toStringMap(response.getRawResult());
    }

    @Override
    public Map<String, String> getReport() throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createReport(codec);
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (response == null || response.status != 200) {
            int status = (response != null) ? response.status : -1;
            throw new KyotoTycoonOperationFailedException("Failed to report: status=" + status);
        }
        return toStringMap(response.getRawResult());
    }

    @Override
    public Map<byte[], byte[]> echo(Map<byte[], byte[]> input)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createEcho(input, codec);
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (response == null || response.status != 200) {
            int status = (response != null) ? response.status : -1;
            throw new KyotoTycoonOperationFailedException("Failed to echo: status=" + status);
        }
        return response.getRawResult();
    }

    @Override
    public Map<String, String> echoString(Map<String, String> input)
            throws KyotoTycoonOperationFailedException {
        TsvRpcRequest request = TsvRpcRequest.createEcho(input, codec);
        Operation operation = call(request);
        TsvRpcResponse response = (TsvRpcResponse) operation.getResponse();
        if (response == null || response.status != 200) {
            int status = (response != null) ? response.status : -1;
            throw new KyotoTycoonOperationFailedException("Failed to echo: status=" + status);
        }
        
        return toStringMap(response.getRawResult());
    }
    
    private Map<String, String> toStringMap(Map<byte[], byte[]> source) {
        HashMap<String, String> result = new HashMap<String, String>();
        for (Map.Entry<byte[], byte[]> entry : source.entrySet()) {
            result.put(Bytes.utf8(entry.getKey()), Bytes.utf8(entry.getValue()));
        }
        return result;
    }
}
