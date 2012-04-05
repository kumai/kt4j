package kt4j.binary;

import java.util.ArrayList;
import java.util.List;

import kt4j.ExpirationTime;
import kt4j.Request;

/**
 * Payload for set_bulk operation.
 * 
 * @author kumai
 */
class SetBulkRequest extends BinaryRequest {
    
    private final List<Record> records = new ArrayList<Record>();
    
    SetBulkRequest() {
        super(Request.Command.SET_BULK);
    }
    
    SetBulkRequest(byte[] key, byte[] value, ExpirationTime xt, int databaseIndex) {
        this();
        add(key, value, xt, databaseIndex);
    }
    
    public int getNumberOfRecords() {
        return records.size();
    }
    
    public SetBulkRequest add(byte[] key, byte[] value, ExpirationTime xt, int databaseIndex) {
        long expirationTime = (xt == null) ? Long.MAX_VALUE : xt.value;
        records.add(new Record(databaseIndex, key, value, expirationTime));
        return this;
    }
    
    @Override
    public byte[] encode() {
        int numRecords = getNumberOfRecords();
        
        int messageLength = HEADER_BYTE_LENGTH;
        for (Record record : records) {
            messageLength += record.length();
        }
        
        byte[] message = new byte[messageLength];
        
        int i = 0;
        
        message[i++] = command.magic;
        
        // flags
        message[i++] = (byte) ((flags >>> 24) & 0xFF);
        message[i++] = (byte) ((flags >>> 16) & 0xFF);
        message[i++] = (byte) ((flags >>>  8) & 0xFF);
        message[i++] = (byte) ((flags >>>  0) & 0xFF);
        
        message[i++] = (byte) ((numRecords >>> 24) & 0xFF);
        message[i++] = (byte) ((numRecords >>> 16) & 0xFF);
        message[i++] = (byte) ((numRecords >>>  8) & 0xFF);
        message[i++] = (byte) ((numRecords >>>  0) & 0xFF);
        
        // Record
        for (Record record : records) {
            message[i++] = (byte) ((record.dbidx >>> 8) & 0xFF);
            message[i++] = (byte) ((record.dbidx >>> 0) & 0xFF);
            
            message[i++] = (byte) ((record.key.length >>> 24) & 0xFF);
            message[i++] = (byte) ((record.key.length >>> 16) & 0xFF);
            message[i++] = (byte) ((record.key.length >>>  8) & 0xFF);
            message[i++] = (byte) ((record.key.length >>>  0) & 0xFF);
            
            message[i++] = (byte) ((record.value.length >>> 24) & 0xFF);
            message[i++] = (byte) ((record.value.length >>> 16) & 0xFF);
            message[i++] = (byte) ((record.value.length >>>  8) & 0xFF);
            message[i++] = (byte) ((record.value.length >>>  0) & 0xFF);

            message[i++] = (byte) ((record.xt >>> 56) & 0xFF);
            message[i++] = (byte) ((record.xt >>> 48) & 0xFF);
            message[i++] = (byte) ((record.xt >>> 40) & 0xFF);
            message[i++] = (byte) ((record.xt >>> 32) & 0xFF);
            message[i++] = (byte) ((record.xt >>> 24) & 0xFF);
            message[i++] = (byte) ((record.xt >>> 16) & 0xFF);
            message[i++] = (byte) ((record.xt >>>  8) & 0xFF);
            message[i++] = (byte) ((record.xt >>>  0) & 0xFF);
            
            System.arraycopy(record.key, 0, message, i, record.key.length);
            i += record.key.length;
            System.arraycopy(record.value, 0, message, i, record.value.length);
            i += record.value.length;
        }
        
        return message;
    }
}
