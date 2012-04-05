package kt4j.binary;

import java.util.HashMap;
import java.util.Map;

import kt4j.Request;

/**
 * 
 * @author kumai
 *
 */
class GetBulkRequest extends BinaryRequest {
    private final Map<byte[], Integer> keysWithDbIndex = new HashMap<byte[], Integer>();
    
    GetBulkRequest() {
        super(Request.Command.GET_BULK);
    }

    GetBulkRequest(byte[] key, int dbidx) {
        this();
        add(key, dbidx);
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
        
        // flags (reserved and not used now. It should be 0.)
        message[i++] = 0x00;
        message[i++] = 0x00;
        message[i++] = 0x00;
        message[i++] = 0x00;
        
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
