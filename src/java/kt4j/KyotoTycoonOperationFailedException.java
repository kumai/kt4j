package kt4j;

/**
 * 
 * @author kumai
 *
 */
public class KyotoTycoonOperationFailedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public KyotoTycoonOperationFailedException() {
        super();
    }

    public KyotoTycoonOperationFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public KyotoTycoonOperationFailedException(String message) {
        super(message);
    }

    public KyotoTycoonOperationFailedException(Throwable cause) {
        super(cause);
    }
}
