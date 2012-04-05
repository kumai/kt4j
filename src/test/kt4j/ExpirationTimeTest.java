package kt4j;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * 
 * @author Liu Kumai
 *
 */
public class ExpirationTimeTest {

    @Test
    public void testAfter() {
        long ttl = 100L;
        ExpirationTime testee = ExpirationTime.after(ttl);
        assertEquals(ttl, testee.value);
    }

    @Test
    public void testAt() {
        long epoch = System.currentTimeMillis() / 1000;
        ExpirationTime testee = ExpirationTime.at(epoch);
        assertEquals(-epoch, testee.value);
    }

}
