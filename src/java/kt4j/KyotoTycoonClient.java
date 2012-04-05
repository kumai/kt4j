package kt4j;

import java.util.List;
import java.util.Map;

/**
 * Performs Kyoto Tycoon operations as a client. 
 * 
 * @author kumai
 */
public interface KyotoTycoonClient {

    /**
     * Start the client and connect to Kyoto Tycoon server.
     */
    void start() throws KyotoTycoonOperationFailedException;
    
    /**
     * Stop the client.
     */
    void stop();

    /**
     * Set the value of a record.
     * 
     * @param key
     *      the key of the record.
     * @param value
     *      the value of the record.
     */
    void set(byte[] key, byte[] value) throws KyotoTycoonOperationFailedException;
    
    /**
     * Set the value of a record.
     * <p>
     * The key and the value are encoded with UTF-8.
     * </p>
     * 
     * @param key
     *      the key of the record.
     * @param value
     *      the value of the record.
     */
    void set(String key, String value) throws KyotoTycoonOperationFailedException;
    
    /**
     * Set the value of a record.
     * 
     * @param key
     *      the key of the record.
     * @param value
     *      the value of the record.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     */
    void set(byte[] key, byte[] value, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException;
    
    /**
     * Set the value of a record.
     * <p>
     * The <code>key</code> and the <code>value</code> are encoded with UTF-8.
     * </p>
     * 
     * @param key
     *      the key of the record.
     * @param value
     *      the value of the record.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     */
    void set(String key, String value, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException;
    
    /**
     * Stores records at once.
     * 
     * @param keyValuePairs
     *      key-value pairs to store.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     * @param atomic
     *          true to perform all operations atomically, or false for non-atomic operations.
     */
    void setBulk(Map<byte[], byte[]> keyValuePairs, ExpirationTime xt, boolean atomic)
            throws KyotoTycoonOperationFailedException;

    /**
     * Stores records at once.
     * <p>
     * An invocation of this method of the form <code>client.setBulkdString(keyValuePairs)</code>
     * behaves in exactly the same way as the invocation
     * <blockquote>
     * <code>client.setBulkString(keyValuePairs, null)</code>
     * </blockquote>
     * </p>
     * <p>
     * The keys and the values are encoded with UTF-8.
     * </p>
     *  
     * @param keyValuePairs
     *      key-value pairs to store.
     */
    void setBulkString(Map<String, String> keyValuePairs) throws KyotoTycoonOperationFailedException;
    
    /**
     * Stores records at once.
     * <p>
     * An invocation of this method of the form <code>client.setBulkdString(keyValuePairs, xt)</code>
     * behaves in exactly the same way as the invocation
     * <blockquote>
     * <code>client.setBulkString(keyValuePairs, xt, false)</code>
     * </blockquote>
     * </p>
     * <p>
     * The keys and the values are encoded with UTF-8.
     * </p>
     * 
     * @param keyValuePairs
     *      key-value pairs to store.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     */
    void setBulkString(Map<String, String> keyValuePairs, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException;
    
    /**
     * Stores records at once.
     * <p>
     * The keys and the values are encoded with UTF-8.
     * </p>
     * 
     * @param keyValuePairs
     *      key-value pairs to store.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     * @param atomic
     *          true to perform all operations atomically, or false for non-atomic operations.
     */
    void setBulkString(Map<String, String> keyValuePairs, ExpirationTime xt, boolean atomic)
            throws KyotoTycoonOperationFailedException;

    /**
     * Retrieve a record.
     * 
     * @param key the key.
     * @return the value of the corresponding record, or null if not exists.
     * @throws KyotoTycoonOperationFailedException
     */
    byte[] get(byte[] key) throws KyotoTycoonOperationFailedException;
    
    /**
     * Retrieve a record.
     * <p>
     * The <code>key</code> is encoded with UTF-8. 
     * </p>
     * 
     * @param key the key.
     * @return the value of the corresponding record, or null if not exists.
     */
    String get(String key) throws KyotoTycoonOperationFailedException;
    
    /**
     * Retrieves records at onece.
     * <p>
     * This method is the same as <code>getBulkString(keys, false).</code>
     * </p>
     * 
     * @param keys
     *          the keys of the records to retrieve.
     * @return A map of retreived records. Returns empty map if no record found. The map is unmodifiable.
     */
    Map<String, String> getBulkString(List<String> keys) throws KyotoTycoonOperationFailedException;
    
    /**
     * Retrieves records at onece.
     * <p>
     * The <code>keys</code> and the values are encoded with UTF-8. 
     * </p>
     * 
     * @param keys
     *          the keys of the records to retrieve.
     * @param atomic 
     *          true to perform all operations atomically, or false for non-atomic operations.
     * @return A map of retreived records. Returns empty map if no record found. The map is unmodifiable.
     */
    Map<String, String> getBulkString(List<String> keys, boolean atomic)
            throws KyotoTycoonOperationFailedException;
    
    /**
     * Retrieve records at onece.
     * 
     * @param keys the keys of the records to retrieve.
     * @return A map of retreived records. Returns empty map if no record found. The map is unmodifiable.
     */
    Map<byte[], byte[]> getBulk(List<byte[]> keys) throws KyotoTycoonOperationFailedException;
    
    /**
     * Retrieve records at onece.
     * 
     * @param keys
     *          the keys of the records to retrieve.
     * @param atomic 
     *          true to perform all operations atomically, or false for non-atomic operations.
     * @return A map of retreived records. Returns empty map if no record found. The map is unmodifiable.
     */
    Map<byte[], byte[]> getBulk(List<byte[]> keys, boolean atomic) throws KyotoTycoonOperationFailedException;

    /**
     * Retrieve the value of a record and remove it atomically.
     * <p>
     * The <code>key</code> and the value are encoded with UTF-8. 
     * </p>
     * 
     * @param key the key of the record.
     * @return the value of the record.
     */
    String seize(String key) throws KyotoTycoonOperationFailedException;

    /**
     * Retrieve the value of a record and remove it atomically.
     * 
     * @param key the key of the record.
     * @return the value of the record.
     */
    byte[] seize(byte[] key) throws KyotoTycoonOperationFailedException;

    /**
     * Remove a record.
     * 
     * @param key the key
     * @return true on success, or false if no record corresponds to the key.
     */
    boolean remove(String key) throws KyotoTycoonOperationFailedException;
    
    /**
     * Remove a record.
     * 
     * @param key the key
     * @return true on success, or false if no record corresponds to the key.
     */
    boolean remove(byte[] key) throws KyotoTycoonOperationFailedException;
    
    /**
     * Remove records at once.
     * 
     * @param keys 
     *          the keys of the records to remove.
     * @return the number of remeved records
     */
    long removeBulk(List<byte[]> keys) throws KyotoTycoonOperationFailedException;
    
    /**
     * Remove records at once.
     * 
     * @param keys 
     *          the keys of the records to remove.
     * @param atomic
     *          true to perform all operations atomically, or false for non-atomic operations.
     * @return the number of remeved records
     */
    long removeBulk(List<byte[]> keys, boolean atomic) throws KyotoTycoonOperationFailedException;
    
    /**
     * Remove records at once.
     * 
     * @param keys 
     *          the keys of the records to remove.
     * @return the number of remeved records
     */
    long removeBulkString(List<String> keys) throws KyotoTycoonOperationFailedException;
    
    /**
     * Remove records at once.
     * 
     * @param keys 
     *          the keys of the records to remove.
     * @param atomic
     *          true to perform all operations atomically, or false for non-atomic operations.
     * @return the number of remeved records
     */
    long removeBulkString(List<String> keys, boolean atomic) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a number to the numeric integer value of a record.
     * The additional number is 1, the origin number is 0, and the expiration time is not specified.
     * <p>
     * The <code>key</code> is encoded with UTF-8. 
     * </p>
     * 
     * @param key
     *      the key of the record.
     * @return the result value.
     */
    long increment(String key) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a number to the numeric integer value of a record.
     * The additional number is 1, the origin number is 0, and the expiration time is not specified.
     * 
     * @param key
     *      the key of the record.
     * @return the result value.
     */
    long increment(byte[] key) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a number to the numeric integer value of a record.
     * The additional number is 1, and the origin number is 0.
     * <p>
     * The <code>key</code> is encoded with UTF-8. 
     * </p>
     * 
     * @param key
     *      the key of the record.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     * @return the result value.
     */
    long increment(String key, ExpirationTime xt) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a number to the numeric integer value of a record.
     * The additional number is 1, and the origin number is 0.
     * 
     * @param key
     *      the key of the record.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     * @return the result value.
     */
    long increment(byte[] key, ExpirationTime xt) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a number to the numeric integer value of a record.
     * The origin number is 0.
     * <p>
     * The <code>key</code> is encoded with UTF-8. 
     * </p>
     * 
     * @param key
     *      the key of the record.
     * @param num
     *      the additional number.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     * @return the result value.
     */
    long increment(String key, long num, ExpirationTime xt) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a number to the numeric integer value of a record.
     * The origin number is 0.
     * 
     * @param key
     *      the key of the record.
     * @param num
     *      the additional number.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     * @return the result value.
     */
    long increment(byte[] key, long num, ExpirationTime xt) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a number to the numeric integer value of a record.
     * <p>
     * The <code>key</code> is encoded with UTF-8. 
     * </p>
     * 
     * @param key
     *      the key of the record.
     * @param num
     *      the additional number.
     * @param origin
     *      the origin number.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     * @return the result value.
     */
    long increment(String key, long num, long origin, ExpirationTime xt) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a number to the numeric integer value of a record.
     * 
     * @param key
     *      the key of the record.
     * @param num
     *      the additional number.
     * @param origin
     *      the origin number.
     * @param xt
     *      the expiration time of the record. If null is specified, no expiration time is specified.
     * @return the result value.
     */
    long increment(byte[] key, long num, long origin, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException;

    /**
     * Add a number to the numeric double value of a record.
     * The origin number is 0, and the expiration time of the record is not specified.
     * <p>
     * The <code>key</code> is encoded with UTF-8. 
     * </p>
     * 
     * @param key the key of the record.
     * @param num the additional number.
     * @return the result value.
     */
    double incrementDouble(String key, double num) throws KyotoTycoonOperationFailedException;

    /**
     * Add a number to the numeric double value of a record.
     * The origin number is 0.
     * <p>
     * The <code>key</code> is are encoded with UTF-8. 
     * </p>
     * 
     * @param key the key of the record.
     * @param num the additional number.
     * @param xt the expiration time of the record. If null is specified, no expiration time is specified.
     * @return the result value.
     */
    double incrementDouble(String key, double num, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException;

    /**
     * Add a number to the numeric double value of a record.
     * <p>
     * The <code>key</code> is encoded with UTF-8. 
     * </p>
     * 
     * @param key the key of the record.
     * @param num the additional number.
     * @param origin the origin number.
     * @param xt the expiration time of the record. If null is specified, no expiration time is specified.
     * @return the result value.
     */
    double incrementDouble(String key, double num, double origin, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException;

    /**
     * Add a number to the numeric double value of a record.
     * The origin number is 0, and the expiration time of the record is not specified.
     * 
     * @param key the key of the record.
     * @param num the additional number.
     * @return the result value.
     */
    double incrementDouble(byte[] key, double num) throws KyotoTycoonOperationFailedException;

    /**
     * Add a number to the numeric double value of a record.
     * The origin number is 0.
     * 
     * @param key the key of the record.
     * @param num the additional number.
     * @param xt the expiration time of the record. If null is specified, no expiration time is specified.
     * @return the result value.
     */
    double incrementDouble(byte[] key, double num, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException;

    /**
     * Add a number to the numeric double value of a record.
     * 
     * @param key the key of the record.
     * @param num the additional number.
     * @param origin the origin number.
     * @param xt the expiration time of the record. If null is specified, no expiration time is specified.
     * @return the result value.
     */
    double incrementDouble(byte[] key, double num, double origin, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException;

    /**
     * Perform compare-and-swap without expiration time.
     * <p>
     * Atomically set the value to the given updated value if the current value == the expected value. 
     * </p>
     * <p>
     * The <code>key</code>, <code>expect</code> and <code>update</code> are encoded with UTF-8. 
     * </p>
     * 
     * @param key
     *          the key of the record
     * @param expect
     *          the expected value. {@code null} means that no record corresponds.
     * @param update
     *          the new value. 
     * @return {@code true} if successful.
     *          {@code false} return indicates that the actual value was not equal to the expected value.
     */
    boolean cas(String key, String expect, String update) throws KyotoTycoonOperationFailedException;

    /**
     * Perform compare-and-swap.
     * <p>
     * Atomically set the value to the given updated value if the current value == the expected value. 
     * </p>
     * <p>
     * The <code>key</code>, <code>expect</code> and <code>update</code> are encoded with UTF-8. 
     * </p>
     * 
     * @param key
     *          the key of the record
     * @param expect
     *          the expected value. {@code null} means that no record corresponds.
     * @param update
     *          the new value. 
     * @param xt
     *          the expiration time of the record. If null is specified, no expiration time is specified.
     * @return {@code true} if successful.
     *          {@code false} return indicates that the actual value was not equal to the expected value.
     */
    boolean cas(String key, String expect, String update, ExpirationTime xt) throws KyotoTycoonOperationFailedException;
    
    /**
     * Perform compare-and-swap without expiration time.
     * <p>
     * Atomically set the value to the given updated value if the current value == the expected value. 
     * </p>
     * 
     * @param key
     *          the key of the record
     * @param expect
     *          the expected value. {@code null} means that no record corresponds.
     * @param update
     *          the new value. 
     * @return {@code true} if successful.
     *          {@code false} return indicates that the actual value was not equal to the expected value.
     */
    boolean cas(byte[] key, byte[] expect, byte[] update) throws KyotoTycoonOperationFailedException;

    /**
     * Perform compare-and-swap.
     * <p>
     * Atomically set the value to the given updated value if the current value == the expected value. 
     * </p>
     * 
     * @param key
     *          the key of the record
     * @param expect
     *          the expected value. {@code null} means that no record corresponds.
     * @param update
     *          the new value. 
     * @param xt
     *          the expiration time of the record. If null is specified, no expiration time is specified.
     * @return {@code true} if successful.
     *          {@code false} return indicates that the actual value was not equal to the expected value.
     */
    boolean cas(byte[] key, byte[] expect, byte[] update, ExpirationTime xt)
            throws KyotoTycoonOperationFailedException;
    
    /**
     * Replace the value of a record.
     * The key and the value will be encoded into bytes using UTF-8.
     * <p>
     * The <code>key</code> and the <code>value</code> are encoded with UTF-8. 
     * </p>
     * 
     * @param key the key of the record.
     * @param value the value of the record.
     * @return true on success, or false if no record corresponds to the key.
     */
    boolean replace(String key, String value) throws KyotoTycoonOperationFailedException;
    
    /**
     * Replace the value of a record.
     * 
     * @param key the key of the record.
     * @param value the value of the record.
     * @return true on success, or false if no record corresponds to the key.
     */
    boolean replace(byte[] key, byte[] value) throws KyotoTycoonOperationFailedException;
    
    /**
     * Replace the value of a record.
     * The key and the value will be encoded into bytes using UTF-8.
     * <p>
     * The <code>key</code> and the <code>value</code> are encoded with UTF-8. 
     * </p>
     * 
     * @param key the key of the record.
     * @param value the value of the record.
     * @param xt the expiration time of the record. If null is specified, no expiration time is specified.
     * @return true on success, or false if no record corresponds to the key.
     */
    boolean replace(String key, String value, ExpirationTime xt) throws KyotoTycoonOperationFailedException;
    
    /**
     * Replace the value of a record.
     * 
     * @param key the key of the record.
     * @param value the value of the record.
     * @param xt the expiration time of the record. If null is specified, no expiration time is specified.
     * @return true on success, or false if no record corresponds to the key.
     */
    boolean replace(byte[] key, byte[] value, ExpirationTime xt) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a record.
     * The key and the value will be encoded into bytes using UTF-8.
     * <p>
     * The <code>key</code> and the <code>value</code> are encoded with UTF-8. 
     * </p>
     * 
     * @param key the key of the record.
     * @param value the value of the record.
     * @return true on success, or false if existing record is detected.
     */
    boolean add(String key, String value) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a record.
     * 
     * @param key the key of the record.
     * @param value the value of the record.
     * @return true on success, or false if existing record is detected.
     */
    boolean add(byte[] key, byte[] value) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a record.
     * The key and the value will be encoded into bytes using UTF-8.
     * <p>
     * The <code>key</code> and the <code>value</code> are encoded with UTF-8. 
     * </p>
     * 
     * @param key the key of the record.
     * @param value the value of the record.
     * @param xt the expiration time of the record. If null is specified, no expiration time is specified.
     * @return true on success, or false if existing record is detected.
     */
    boolean add(String key, String value, ExpirationTime xt) throws KyotoTycoonOperationFailedException;
    
    /**
     * Add a record.
     * 
     * @param key the key of the record.
     * @param value the value of the record.
     * @param xt the expiration time of the record. If null is specified, no expiration time is specified.
     * @return true on success, or false if existing record is detected.
     */
    boolean add(byte[] key, byte[] value, ExpirationTime xt) throws KyotoTycoonOperationFailedException;
    
    /**
     * Remove all records in a database.
     */
    void clear() throws KyotoTycoonOperationFailedException;
    
    /**
     * Get keys matching a regular expression string.
     * The regular expression string will be encoded into bytes using UTF-8.
     * <p>
     * The <code>regex</code> is encoded with UTF-8. 
     * </p>
     * 
     * @param regex The regular expression string.
     * @return Keys matching the specified regular expression.
     */
    List<String> matchRegex(String regex) throws KyotoTycoonOperationFailedException;

    /**
     * Get keys matching a regular expression string.
     * The regular expression string will be encoded into bytes using UTF-8.
     * <p>
     * The <code>regex</code> is encoded with UTF-8. 
     * </p>
     * 
     * @param regex The regular expression string.
     * @param max The maximum number to retreive. If it is negative, no limit is specified. 
     * @return Keys matching the specified regular expression.
     */
    List<String> matchRegex(String regex, long max) throws KyotoTycoonOperationFailedException;

    /**
     * Get keys matching a regular expression string.
     * 
     * @param regex The regular expression string.
     * @return Keys matching the specified regular expression.
     */
    List<byte[]> matchRegex(byte[] regex) throws KyotoTycoonOperationFailedException;
    
    /**
     * Get keys matching a regular expression string.
     * 
     * @param regex The regular expression string.
     * @param max The maximum number to retreive. If it is negative, no limit is specified. 
     * @return Keys matching the specified regular expression.
     */
    List<byte[]> matchRegex(byte[] regex, long max) throws KyotoTycoonOperationFailedException;

    /**
     * Get keys matching a prefix string.
     * <p>
     * The <code>prefix</code> is encoded with UTF-8. 
     * </p>
     * 
     * @param prefix The prefix string.
     * @return Keys matching the specified prefix.
     */
    List<String> matchPrefix(String prefix) throws KyotoTycoonOperationFailedException;

    /**
     * Get keys matching a prefix string.
     * 
     * @param prefix The prefix string.
     * @param max The maximum number to retreive. If it is negative, no limit is specified. 
     * @return Keys matching the specified prefix.
     */
    List<String> matchPrefix(String prefix, long max) throws KyotoTycoonOperationFailedException;
    
    /**
     * Get keys matching a prefix string.
     * 
     * @param prefix The prefix string.
     * @return Keys matching the specified prefix.
     */
    List<byte[]> matchPrefix(byte[] prefix) throws KyotoTycoonOperationFailedException;
    
    /**
     * Get keys matching a prefix string.
     * 
     * @param prefix The prefix string.
     * @param max The maximum number to retreive. If it is negative, no limit is specified. 
     * @return Keys matching the specified prefix.
     */
    List<byte[]> matchPrefix(byte[] prefix, long max) throws KyotoTycoonOperationFailedException;
    
    /**
     * Call a procedure of the script language extension.
     * 
     * @param procedureName the name of the procedure to call.
     * @param input arbitrary records to pass to the script. 
     * @return the output of the procedure.
     */
    Map<byte[], byte[]> playScript(String procedureName, Map<byte[], byte[]> input)
            throws KyotoTycoonOperationFailedException;

    /**
     * Call a procedure of the script language extension.
     * 
     * @param procedureName the name of the procedure to call.
     * @param input arbitrary records to pass to the script. 
     * @return the output of the procedure.
     */
    Map<String, String> playScriptString(String procedureName, Map<String, String> input)
            throws KyotoTycoonOperationFailedException;
    
    /**
     * Call `void' procedure. This is just for testing.
     */
    void ping() throws KyotoTycoonOperationFailedException;
    
    /**
     * Synchronizes updated contents with the file and the device.
     * 
     * @param hard
     *      for physical synchronization with the device.
     * @param command
     *      the command name to process the database file.
     */
    void synchronize(boolean hard, String command) throws KyotoTycoonOperationFailedException;
    
    /**
     * Scans the database and eliminates regions of expired records.
     * 
     * @param step
     *      The number of steps. If it is not more than 0, the whole region is scanned.
     */
    void vacuum(int step) throws KyotoTycoonOperationFailedException;
    
    /**
     * Gets the miscellaneous status information of a database.
     * 
     * @return status informations of database.
     */
    Map<String, String> getStatus() throws KyotoTycoonOperationFailedException;
    
    /**
     * Gets the report of the server information.
     * 
     * @return the server information
     */
    Map<String, String> getReport() throws KyotoTycoonOperationFailedException;
    
    /**
     * Echo back the input data as the output data, just for testing.
     * 
     * @param input
     *      arbitrary records
     * @return corresponding records to the input data.
     */
    Map<byte[], byte[]> echo(Map<byte[], byte[]> input)
            throws KyotoTycoonOperationFailedException;
    
    /**
     * Echo back the input data as the output data, just for testing.
     * 
     * @param input
     *      arbitrary records
     * @return corresponding records to the input data.
     */
    Map<String, String> echoString(Map<String, String> input)
            throws KyotoTycoonOperationFailedException;
}
