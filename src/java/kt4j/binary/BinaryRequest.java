package kt4j.binary;

import kt4j.Request;

abstract class BinaryRequest extends Request {
    // Message Header
    protected static final int MAGIC_BYTE_LENGTH = 1;
    protected static final int FLAGS_BYTE_LENGTH = 4;
    protected static final int RNUM_BYTE_LENGTH = 4;
    protected static final int HEADER_BYTE_LENGTH = MAGIC_BYTE_LENGTH + FLAGS_BYTE_LENGTH + RNUM_BYTE_LENGTH;
    
    protected int flags;
    
    protected BinaryRequest(Command command) {
        super(command);
    }

    public abstract byte[] encode();
    
}
