package kt4j.binary;

import java.util.Collections;
import java.util.Map;

import kt4j.Bytes;

class PlayScriptRequest extends BinaryRequest {
    private static final int NSIZ_LENGTH = 4;   // uint32_t
    
    private final Map<byte[], byte[]> inputParams;
    final String procedureName;

    PlayScriptRequest(String procedureName) {
        this(procedureName, null);
    }

    PlayScriptRequest(String procedureName, Map<byte[], byte[]> params) {
        super(Command.PLAY_SCRIPT);
        this.procedureName = procedureName;
        if (params != null) {
            this.inputParams = params;
        } else {
            this.inputParams = Collections.emptyMap();
        }
    }
    
    @Override
    public byte[] encode() {
        byte[] procedureNameBytes = Bytes.utf8(procedureName);
        
        int numInputRecords = inputParams.size();
        
        int headerLength = MAGIC_BYTE_LENGTH + FLAGS_BYTE_LENGTH
                + NSIZ_LENGTH + RNUM_BYTE_LENGTH + procedureNameBytes.length;
        int messageLength = headerLength;
        for (Map.Entry<byte[], byte[]> record : inputParams.entrySet()) {
            messageLength += Record.KSIZ_LENGTH + Record.VSIZ_LENGTH
                    + record.getKey().length + record.getValue().length;
        }
        
        byte[] message = new byte[messageLength];
        
        int i = 0;
        
        // magic
        message[i++] = command.magic;
        
        // flags
        message[i++] = (byte) ((flags >>> 24) & 0xFF);
        message[i++] = (byte) ((flags >>> 16) & 0xFF);
        message[i++] = (byte) ((flags >>>  8) & 0xFF);
        message[i++] = (byte) ((flags >>>  0) & 0xFF);
        
        // nsiz
        message[i++] = (byte) ((procedureNameBytes.length >>> 24) & 0xFF);
        message[i++] = (byte) ((procedureNameBytes.length >>> 16) & 0xFF);
        message[i++] = (byte) ((procedureNameBytes.length >>>  8) & 0xFF);
        message[i++] = (byte) ((procedureNameBytes.length >>>  0) & 0xFF);
        
        // rnum
        message[i++] = (byte) ((numInputRecords >>> 24) & 0xFF);
        message[i++] = (byte) ((numInputRecords >>> 16) & 0xFF);
        message[i++] = (byte) ((numInputRecords >>>  8) & 0xFF);
        message[i++] = (byte) ((numInputRecords >>>  0) & 0xFF);
        
        // name
        System.arraycopy(procedureNameBytes, 0, message, i, procedureNameBytes.length);
        i += procedureNameBytes.length;
        
        for (Map.Entry<byte[], byte[]> record : inputParams.entrySet()) {
            byte[] key = record.getKey();
            
            // ksiz
            message[i++] = (byte) ((key.length >>> 24) & 0xFF);
            message[i++] = (byte) ((key.length >>> 16) & 0xFF);
            message[i++] = (byte) ((key.length >>>  8) & 0xFF);
            message[i++] = (byte) ((key.length >>>  0) & 0xFF);
            
            byte[] value = record.getValue();
            
            // vsiz
            message[i++] = (byte) ((value.length >>> 24) & 0xFF);
            message[i++] = (byte) ((value.length >>> 16) & 0xFF);
            message[i++] = (byte) ((value.length >>>  8) & 0xFF);
            message[i++] = (byte) ((value.length >>>  0) & 0xFF);
            
            // key
            System.arraycopy(key, 0, message, i, key.length);
            i += key.length;
            
            // value
            System.arraycopy(value, 0, message, i, value.length);
            i += value.length;
        }
        
        return message;
    }
}
