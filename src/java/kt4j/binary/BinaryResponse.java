package kt4j.binary;

import kt4j.Response;

class BinaryResponse implements Response {
    static final byte ERROR = (byte) 0xBF;
    static final byte REPLICATION = (byte) 0xB1;
    static final byte PLAY_SCRIPT = (byte) 0xB4;
    static final byte SET_BULK = (byte) 0xB8;
    static final byte REMOVE_BULK = (byte) 0xB9;
    static final byte GET_BULK = (byte) 0xBA;
    
    final byte magic;
    private long number;
    
    protected BinaryResponse(byte magic) {
        this(magic, 0);
    }
    
    protected BinaryResponse(byte magic, long number) {
        this.magic = magic;
        this.number = number;
    }
    
    @Override
    public boolean isSucceeded() {
        return magic != ERROR;
    }
    
    /**
     * Returns the number of the result records
     */
    public long getNumber() {
        return number;
    }
}
