package kt4j.tsvrpc;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * This class is a transcoder for {@link org.jboss.netty.handler.codec.http.HttpRequest}/{@link org.jboss.netty.handler.codec.http.HttpResponse} and {@link TsvRpcRequest}/{@link TsvRpcResponse}. 
 * 
 * @author kumai
 */
public class TsvRpcClientCodec implements ChannelUpstreamHandler, ChannelDownstreamHandler {
    private final Encoder encoder = new Encoder();
    private final Decoder decoder = new Decoder();
    
    @Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent evt) throws Exception {
        encoder.handleDownstream(ctx, evt);
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent evt) throws Exception {
        decoder.handleUpstream(ctx, evt);
    }
    
    private static class Encoder extends OneToOneEncoder {
        
        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            if (!(msg instanceof TsvRpcRequest)) {
                return msg;
            }
            
            TsvRpcRequest request = (TsvRpcRequest) msg;
            
            HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, request.getPath());
            InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
            HttpHeaders.setHost(httpRequest, remoteAddress.getHostName() + ":" + remoteAddress.getPort());
            HttpHeaders.setKeepAlive(httpRequest, true);
            
            ChannelBuffer content = ChannelBuffers.copiedBuffer(request.getEncodedContent());
            HttpHeaders.setHeader(httpRequest, HttpHeaders.Names.CONTENT_TYPE, request.getContentType());
            httpRequest.setHeader(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());
            httpRequest.setContent(content);
            
            return httpRequest;
        }
    }

    private static class Decoder extends OneToOneDecoder {

        /* (non-Javadoc)
         * @see org.jboss.netty.handler.codec.oneone.OneToOneDecoder#decode(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.Channel, java.lang.Object)
         */
        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            if (!(msg instanceof HttpResponse)) {
                return msg;
            }
            
            HttpResponse httpResponse = (HttpResponse) msg;
            
            int httpStatusCode = httpResponse.getStatus().getCode();
            
            ChannelBuffer content = httpResponse.getContent();
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            while (content.readable()) {
                byte[] buf = new byte[content.readableBytes()];
                content.readBytes(buf);
                byteArray.write(buf);
            }
            
            TsvRpcResponse response = new TsvRpcResponse(httpStatusCode);
            TsvColumnCodec columnCodec = TsvColumnCodec.forContentType(httpResponse.getHeader(HttpHeaders.Names.CONTENT_TYPE));
            byte[] responseBody = byteArray.toByteArray();
            int mark = 0;
            for (int i = 0; i < responseBody.length; ++i) {
                if (responseBody[i] == '\n' || i == (responseBody.length - 1)) {
                    byte[] row = Arrays.copyOfRange(responseBody, mark, i);
                    mark = i + 1;
                    for (int j = 0; j < row.length; ++j) {
                        if (row[j] == '\t') {
                            byte[] keyBytes = Arrays.copyOf(row, j);
                            byte[] decodedKey = columnCodec.decode(keyBytes);
                            byte[] value = (j+1 < row.length) ?
                                    columnCodec.decode(Arrays.copyOfRange(row, j+1, row.length)) : new byte[0];
                            response.put(decodedKey, value);
                        }
                    }
                }
            }
            
            return response;
        }
    }
}
