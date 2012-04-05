package kt4j;

/**
 * Expiration time of each records.
 * 
 * @author kumai
 */
public class ExpirationTime {
    public final long value;
    
    private ExpirationTime(long value) {
        this.value = value;
    }
    
    /**
     * Returns an expiration time in seconds.
     * 
     * @param epochTimeInSec the epoch time to expire.
     * @return the expiration time
     * @throws IllegalArgumentException if the specified value is negative
     */
    public static final ExpirationTime at(long epochTimeInSec) throws IllegalArgumentException {
        if (epochTimeInSec < 0) {
            throw new IllegalArgumentException("the expiration time value cannot be negative: " + epochTimeInSec);
        }
        return new ExpirationTime(-epochTimeInSec);
    }
    
    /**
     * Returns an expiration time in seconds.
     * 
     * @param ttlInSec the expiration time from now.
     * @return the expiration time
     * @throws IllegalArgumentException if the specified value is negative
     */
    public static final ExpirationTime after(long ttlInSec) throws IllegalArgumentException {
        if (ttlInSec < 0) {
            throw new IllegalArgumentException("the expiration time value cannot be negative: " + ttlInSec);
        }
        return new ExpirationTime(ttlInSec);
    }
    
    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
