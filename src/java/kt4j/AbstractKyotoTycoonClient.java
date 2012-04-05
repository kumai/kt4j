package kt4j;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;

import kt4j.binary.BinaryProtocolDecoder;
import kt4j.binary.BinaryProtocolEncoder;
import kt4j.tsvrpc.TsvRpcClientCodec;

/**
 * A skeletal {@link KyotoTycoonClient} implementation.
 * 
 * @author kumai
 */
public abstract class AbstractKyotoTycoonClient implements KyotoTycoonClient {
    private final ClientBootstrap bootstrap;
    private final SocketAddress[] servers;
    private Channel channel;

    /**
     * Creates a new instance.
     * 
     * @param servers
     *      Kyoto Tycoon server addresses.
     *      Now uses only first one of the servers.
     */
    protected AbstractKyotoTycoonClient(SocketAddress... servers) {
        if (servers.length < 1) {
            throw new IllegalArgumentException("empty server addresses");
        }
        
        this.servers = servers;
        
        bootstrap = new ClientBootstrap(newChannelFactory());
        bootstrap.setPipelineFactory(newPipelineFactory());
    }
    
    /**
     * Creates a {@link ClientSocketChannelFactory} for communication with Kyoto Tycoon server.
     */
    protected ClientSocketChannelFactory newChannelFactory() {
        return new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
    }

    /**
     * Creates a {@link ChannelPipelineFactory} for processing Kyoto Tycoon messages.
     */
    protected ChannelPipelineFactory newPipelineFactory() {
        return new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("kt-bin-decoder", new BinaryProtocolDecoder());
                pipeline.addLast("http-codec", new HttpClientCodec());
                pipeline.addLast("http-aggregator", new HttpChunkAggregator(1024*1024));
                pipeline.addLast("kt-tsvrpc-codec", new TsvRpcClientCodec());
                pipeline.addLast("kt-bin-encoder", new BinaryProtocolEncoder());
                pipeline.addLast("kt-handler", new KTChannelHandler());
                return pipeline;
            }
        };
    }
    
    @Override
    public synchronized void start() {
        ChannelFuture channelFuture = bootstrap.connect(servers[0]);
        channelFuture.awaitUninterruptibly();
        if (channelFuture.isSuccess()) {
            this.channel = channelFuture.getChannel();
        } else {
            throw new KyotoTycoonOperationFailedException("Failed to connect: " + servers[0], channelFuture.getCause());
        }
    }

    @Override
    public synchronized void stop() {
        if (channel != null) {
            ChannelFuture closeFuture = channel.close();
            closeFuture.awaitUninterruptibly(10000);
            channel = null;
            bootstrap.releaseExternalResources();
        }
    }
    
    /**
     * Executes a Kyoto Tycoon RPC synchronously.
     * 
     * @param request
     *      A request to Kyoto Tycoon.
     * @return result of the RPC.
     */
    protected Operation call(Request request) {
        Operation operation = new Operation(request);
        if (channel == null) {
            throw new IllegalStateException("The channel is not ready.");
        }
        
        channel.write(operation);
        operation.awaitUninterruptibly();
        return operation;
    }
    
    private static class KTChannelHandler extends SimpleChannelHandler {
        private final BlockingQueue<Operation> requestedOperations = new LinkedBlockingQueue<Operation>();

        private final Lock lock = new ReentrantLock();

        @Override
        public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            Operation op = (Operation) e.getMessage();
            lock.lock();
            try {
                requestedOperations.offer(op);
                Channels.write(ctx, e.getFuture(), op.getRequest());
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            Operation op = requestedOperations.poll();
            Response response = (Response) e.getMessage();
            op.completed(response);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            Operation op = requestedOperations.poll();
            if (op != null) {
                op.exceptionCaught(e.getCause());
            } else {
                if (ctx.canHandleUpstream()) {
                    ctx.sendUpstream(e);
                }
            }
        }
    }
    
    @Override
    public void set(byte[] key, byte[] value) {
        set(key, value, null);
    }

    @Override
    public void set(String key, String value) {
        set(Bytes.utf8(key), Bytes.utf8(value));
    }

    @Override
    public void set(String key, String value, ExpirationTime xt) {
        set(Bytes.utf8(key), Bytes.utf8(value), xt);
    }
    
    @Override
    public void setBulkString(Map<String, String> keyValuePairs)
            throws KyotoTycoonOperationFailedException {
        setBulkString(keyValuePairs, null);
    }

    @Override
    public void setBulkString(Map<String, String> keyValuePairs, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        setBulkString(keyValuePairs, null, false);
    }

    @Override
    public String get(String key) throws KyotoTycoonOperationFailedException {
        byte[] value = get(Bytes.utf8(key));
        return Bytes.utf8(value);
    }

    @Override
    public Map<byte[], byte[]> getBulk(List<byte[]> keys)
            throws KyotoTycoonOperationFailedException {
        return getBulk(keys, false);
    }
    
    @Override
    public Map<String, String> getBulkString(List<String> keys)
            throws KyotoTycoonOperationFailedException {
        return getBulkString(keys, false);
    }
    
    @Override
    public long removeBulk(List<byte[]> keys) throws KyotoTycoonOperationFailedException {
        return removeBulk(keys, false);
    }

    @Override
    public long removeBulkString(List<String> keys) throws KyotoTycoonOperationFailedException {
        return removeBulkString(keys, false);
    }

    @Override
    public String seize(String key) throws KyotoTycoonOperationFailedException {
        byte[] value = seize(Bytes.utf8(key));
        return Bytes.utf8(value);
    }

    @Override
    public boolean remove(String key) throws KyotoTycoonOperationFailedException {
        return remove(Bytes.utf8(key));
    }

    @Override
    public long increment(String key) throws KyotoTycoonOperationFailedException {
        return increment(Bytes.utf8(key));
    }

    @Override
    public long increment(byte[] key) throws KyotoTycoonOperationFailedException {
        return increment(key, null);
    }

    @Override
    public long increment(String key, ExpirationTime xt) throws KyotoTycoonOperationFailedException {
        return increment(Bytes.utf8(key), xt);
    }

    @Override
    public long increment(byte[] key, ExpirationTime xt) throws KyotoTycoonOperationFailedException {
        return increment(key, 1, 0, xt);
    }

    @Override
    public long increment(String key, long num, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        return increment(key, num, 0, xt);
    }

    @Override
    public long increment(byte[] key, long num, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        return increment(key, num, 0, xt);
    }

    @Override
    public long increment(String key, long num, long origin, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        return increment(Bytes.utf8(key), num, origin, xt);
    }

    @Override
    public double incrementDouble(String key, double num)
            throws KyotoTycoonOperationFailedException {
        return incrementDouble(Bytes.utf8(key), num);
    }

    @Override
    public double incrementDouble(byte[] key, double num)
            throws KyotoTycoonOperationFailedException {
        return incrementDouble(key, num, null);
    }

    @Override
    public double incrementDouble(String key, double num, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        return incrementDouble(Bytes.utf8(key), num, xt);
    }

    @Override
    public double incrementDouble(String key, double num, double origin, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        return incrementDouble(Bytes.utf8(key), num, origin, xt);
    }

    @Override
    public boolean cas(String key, String expect, String update)
            throws KyotoTycoonOperationFailedException {
        return cas(Bytes.utf8(key), Bytes.utf8(expect), Bytes.utf8(update));
    }

    @Override
    public boolean cas(String key, String expect, String update, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        return cas(Bytes.utf8(key), Bytes.utf8(expect), Bytes.utf8(update), xt);
    }

    @Override
    public boolean cas(byte[] key, byte[] expect, byte[] update)
            throws KyotoTycoonOperationFailedException {
        return cas(key, expect, update, null);
    }

    @Override
    public boolean replace(String key, String value) throws KyotoTycoonOperationFailedException {
        return replace(key, value, null);
    }

    @Override
    public boolean replace(byte[] key, byte[] value) throws KyotoTycoonOperationFailedException {
        return replace(key, value, null);
    }

    @Override
    public boolean replace(String key, String value, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        return replace(Bytes.utf8(key), Bytes.utf8(value), xt);
    }

    @Override
    public boolean add(String key, String value) throws KyotoTycoonOperationFailedException {
        return add(key, value, null);
    }

    @Override
    public boolean add(byte[] key, byte[] value) throws KyotoTycoonOperationFailedException {
        return add(key, value, null);
    }

    @Override
    public boolean add(String key, String value, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException {
        return add(Bytes.utf8(key), Bytes.utf8(value), xt);
    }

    @Override
    public List<String> matchRegex(String regex) throws KyotoTycoonOperationFailedException {
        return matchRegex(regex, -1);
    }

    @Override
    public List<String> matchRegex(String regex, long max)
            throws KyotoTycoonOperationFailedException {
        List<byte[]> keys = matchRegex(Bytes.utf8(regex), max);
        ArrayList<String> result = new ArrayList<String>();
        for (byte[] key : keys) {
            result.add(Bytes.utf8(key));
        }
        return result;
    }

    @Override
    public List<byte[]> matchRegex(byte[] regex) throws KyotoTycoonOperationFailedException {
        return matchRegex(regex, -1);
    }

    @Override
    public List<String> matchPrefix(String prefix) throws KyotoTycoonOperationFailedException {
        return matchPrefix(prefix, -1);
    }

    @Override
    public List<String> matchPrefix(String prefix, long max)
            throws KyotoTycoonOperationFailedException {
        List<byte[]> keys = matchPrefix(Bytes.utf8(prefix), max);
        ArrayList<String> result = new ArrayList<String>();
        for (byte[] key : keys) {
            result.add(Bytes.utf8(key));
        }
        return result;
    }

    @Override
    public List<byte[]> matchPrefix(byte[] prefix) throws KyotoTycoonOperationFailedException {
        return matchPrefix(prefix, -1);
    }

    @Override
    public Map<String, String> playScriptString(String procedureName, Map<String, String> params)
            throws KyotoTycoonOperationFailedException {
        HashMap<byte[], byte[]> input = new HashMap<byte[], byte[]>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            input.put(Bytes.utf8(entry.getKey()), Bytes.utf8(entry.getValue()));
        }
        Map<byte[], byte[]> output = playScript(procedureName, input);
        HashMap<String, String> result = new HashMap<String, String>();
        for (Map.Entry<byte[], byte[]> entry : output.entrySet()) {
            result.put(Bytes.utf8(entry.getKey()), Bytes.utf8(entry.getValue()));
        }
        return result;
    }

}
