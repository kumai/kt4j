package kt4j.binary;

import java.util.HashMap;
import java.util.Map;

import kt4j.Request;

class RemoveBulkRequest extends BinaryRequest {
    private final Map<byte[], Integer> keysWithDbIndex = new HashMap<byte[], Integer>();

    RemoveBulkRequest() {
        super(Request.Command.REMOVE_BULK);
    }

    RemoveBulkRequest(byte[] key, int databaseIndex) {
        this();
        add(key, databaseIndex);
    }

    void add(byte[] key, int dbidx) throws NullPointerException {
        if (key == null) {
            throw new NullPointerException("key");
        }
        keysWithDbIndex.put(key, dbidx);
    }

    @Override
    public byte[] encode() {
        int numRecords = getNumberOfRecords();
        
        int messageLength = HEADER_BYTE_LENGTH;
        
        for (Map.Entry<byte[], Integer> entry : keysWithDbIndex.entrySet()) {
            messageLength += 2;     // size of dbidx
            messageLength += 4;     // size of ksiz
            messageLength += entry.getKey().length;
        }
        
        byte[] message = new byte[messageLength];
        
        int i = 0;
        
        message[i++] = command.magic;
        
        // flags
        message[i++] = (byte) ((flags >>> 24) & 0xFF);
        message[i++] = (byte) ((flags >>> 16) & 0xFF);
        message[i++] = (byte) ((flags >>>  8) & 0xFF);
        message[i++] = (byte) ((flags >>>  0) & 0xFF);
        
        // number of records
        message[i++] = (byte) ((numRecords >>> 24) & 0xFF);
        message[i++] = (byte) ((numRecords >>> 16) & 0xFF);
        message[i++] = (byte) ((numRecords >>>  8) & 0xFF);
        message[i++] = (byte) ((numRecords >>>  0) & 0xFF);

        for (Map.Entry<byte[], Integer> entry : keysWithDbIndex.entrySet()) {
            // dbidx
            message[i++] = (byte) ((entry.getValue() >>> 8) & 0xFF);
            message[i++] = (byte) ((entry.getValue() >>> 0) & 0xFF);
            
            // ksiz
            message[i++] = (byte) ((entry.getKey().length >>> 24) & 0xFF);
            message[i++] = (byte) ((entry.getKey().length >>> 16) & 0xFF);
            message[i++] = (byte) ((entry.getKey().length >>>  8) & 0xFF);
            message[i++] = (byte) ((entry.getKey().length >>>  0) & 0xFF);

            System.arraycopy(entry.getKey(), 0, message, i, entry.getKey().length);
            i += entry.getKey().length;
        }
        
        return message;
    }

    public int getNumberOfRecords() {
        return keysWithDbIndex.size();
    }

}
