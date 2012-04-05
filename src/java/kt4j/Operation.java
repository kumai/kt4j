package kt4j;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This represents an operation to Kyoto Tycoon, eg: get, set, remove, etc.
 * 
 * @author kumai
 */
public class Operation {
    private final CountDownLatch latch = new CountDownLatch(1);

    private final Request request;
    private Response response;
    private Throwable exception;
    
    private final long timeoutMillis;
    
    public Operation(Request request) throws NullPointerException {
        this(request, 0L);
    }
    
    public Operation(Request request, long timeoutMillis) throws NullPointerException {
        if (request == null) {
            throw new NullPointerException("request");
        }
        
        this.request = request;
        this.timeoutMillis = timeoutMillis;
    }
    
    public Request getRequest() {
        return this.request;
    }
    
    public Response getResponse() {
        return this.response;
    }
    
    public Throwable getException() {
        return exception;
    }
    
    public Operation await() throws InterruptedException {
        if (timeoutMillis > 0L) {
            latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        } else {
            latch.await();
        }
        return this;
    }
    
    public Operation awaitUninterruptibly() {
        try {
            if (timeoutMillis > 0L) {
                latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
            } else {
                latch.await();
            }
        } catch (InterruptedException ignored) {
        }
        return this;
    }
    
    public void completed(Response response) {
        this.response = response;
        latch.countDown();
    }
    
    public void exceptionCaught(Throwable e) {
        this.exception = e;
        latch.countDown();
    }
    
    /**
     * Tests this operation is completed nomally.
     * 
     * @return Returns {@code true} if the operation was completed without errors.
     */
    public boolean isSucceeded() {
        return (exception == null && response != null && response.isSucceeded());
    }
    
    /**
     * Tests this operation is done.
     * 
     * @return Returns {@code true} if the operation was ended.
     */
    public boolean isDone() {
        return (latch.getCount() == 0);
    }
}
