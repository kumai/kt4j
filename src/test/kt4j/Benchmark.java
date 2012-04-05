package kt4j;

import static org.junit.Assert.*;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import kt4j.binary.KyotoTycoonBinaryClient;
import kt4j.tsvrpc.KyotoTycoonTsvRpcClient;

import org.junit.Test;

public class Benchmark {
    
    @Test
    public void benchmark() throws Exception {
        final String host = "127.0.0.1";
        final int port = 1978;
        final int numOperations = 20000;
        final int numThreads = 100;

        final KyotoTycoonBinaryClient binaryClient = new KyotoTycoonBinaryClient(host, port);
        final KyotoTycoonTsvRpcClient tsvRpcClient = new KyotoTycoonTsvRpcClient(host, port);
        

        Benchmark.execute(binaryClient, numOperations, numThreads);
        Benchmark.execute(tsvRpcClient, numOperations, numThreads);
    }
    
    public static void execute(final KyotoTycoonClient client, final int numOperations, final int numThreads)
            throws Exception {
        final CountDownLatch latch = new CountDownLatch(numOperations);
        
        client.start();
        client.clear();

        long start = System.currentTimeMillis();
        
        final AtomicInteger errorCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        Random r = new Random();
        for (int i = 0; i < numOperations; ++i) {
            final String key = "key" + i;
            final String value = String.valueOf(Math.abs(r.nextInt()));
            executor.submit(new Runnable() {
                @Override public void run() {
                    try {
                        client.set(key, value);
                        assertEquals(value, client.get(key));
                        //assertTrue(client.remove(key));
                    } catch (RuntimeException e) {
                        e.printStackTrace();
                        fail();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        executor.shutdown();
        latch.await();
        
        if (errorCount.intValue() > 0) {
            fail();
        }
        
        long timeElapsed = System.currentTimeMillis() - start;
        
        int qps = (int) ((numOperations / (float) timeElapsed) * 1000);
        System.out.println(client.getClass().getSimpleName() + " - result: " + timeElapsed + "ms, " + qps + "qps");

        client.clear();
        client.stop();
    }

}
