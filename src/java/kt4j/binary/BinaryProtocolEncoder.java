package kt4j.binary;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * Transforms a Kyoto Tycoon's binary request into a {@link ChannelBuffer}.
 * 
 * @author kumai
 */
public class BinaryProtocolEncoder extends OneToOneEncoder {
    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
            throws Exception {
        if (!(msg instanceof BinaryRequest)) {
            return msg;
        }
        
        BinaryRequest req = (BinaryRequest) msg;
        ChannelBuffer buff = ChannelBuffers.wrappedBuffer(req.encode());
        
        return buff;
    }
}
