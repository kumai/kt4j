package kt4j.binary;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import static kt4j.binary.BinaryResponse.*;

/**
 * Decodes the received {@link ChannelBuffer}s into a Kyoto Tycoon's response object.
 * 
 * @author kumai
 */
public class BinaryProtocolDecoder extends FrameDecoder {
    private static final int HITS_LENGTH = 4;
    private static final int DBIDX_LENGTH = 2;
    private static final int KSIZ_LENGTH = 4;
    private static final int VSIZ_LENGTH = 4;
    private static final int XT_LENGTH = 8;
    private static final int RNUM_LENGTH = 4;

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer)
            throws Exception {
        if (!buffer.readable()) {
            return null;
        }

        buffer.markReaderIndex();

        byte op = buffer.readByte();
        switch (op) {
        case ERROR:
            return new BinaryResponse(ERROR);
            
        case SET_BULK:
        case REMOVE_BULK:
            if (buffer.readableBytes() < HITS_LENGTH) {
                buffer.resetReaderIndex();
                return null;
            } else {
                long hits = buffer.readUnsignedInt();
                return new BinaryResponse(op, hits);
            }
            
        case GET_BULK:
            GetBulkResponse getBulkResponse = decodeGetBulk(buffer);
            if (getBulkResponse == null) {
                buffer.resetReaderIndex();
            }
            return getBulkResponse;
            
        case PLAY_SCRIPT:
            PlayScriptResponse playScriptResponse = decodePlayScript(buffer);
            if (playScriptResponse == null) {
                buffer.resetReaderIndex();
            }
            return playScriptResponse;

        case REPLICATION:
            // not support
        default:
            buffer.resetReaderIndex();
            return buffer.readBytes(buffer.readableBytes());
        }
    }
    
    private GetBulkResponse decodeGetBulk(ChannelBuffer buffer) {
        if (buffer.readableBytes() < HITS_LENGTH) {
            return null;
        }
        
        long numHits = buffer.readUnsignedInt();
        
        GetBulkResponse resp = new GetBulkResponse(numHits);

        for (long rec = 0; rec < numHits; ++rec) {
            
            if (buffer.readableBytes() < (DBIDX_LENGTH + KSIZ_LENGTH + VSIZ_LENGTH + XT_LENGTH)) {
                return null;
            }
            
            int dbidx = buffer.readUnsignedShort();
            
            long ksiz = buffer.readUnsignedInt();
            
            long vsiz = buffer.readUnsignedInt();
            
            long xt = buffer.readLong();
            
            if (buffer.readableBytes() < (ksiz + vsiz)) {
                return null;
            }
            
            byte[] key = new byte[(int) ksiz];
            buffer.readBytes(key);
            
            byte[] value = new byte[(int) vsiz];
            buffer.readBytes(value);
            
            resp.put(key, value, xt, dbidx);
        }
        
        return resp;
    }
    
    private PlayScriptResponse decodePlayScript(ChannelBuffer buffer) {
        if (buffer.readableBytes() < RNUM_LENGTH) {
            return null;
        }
        
        long numRecords = buffer.readUnsignedInt();
        
        PlayScriptResponse resp = new PlayScriptResponse(numRecords);
        
        for (long rec = 0; rec < numRecords; ++rec) {
            if (buffer.readableBytes() < (KSIZ_LENGTH + VSIZ_LENGTH)) {
                return null;
            }
            
            long ksiz = buffer.readUnsignedInt();
            
            long vsiz = buffer.readUnsignedInt();
            
            if (buffer.readableBytes() < (ksiz + vsiz)) {
                return null;
            }
            
            byte[] key = new byte[(int) ksiz];
            buffer.readBytes(key);
            
            byte[] value = new byte[(int) vsiz];
            buffer.readBytes(value);
            
            resp.put(key, value);
        }
        
        return resp;
    }
}
