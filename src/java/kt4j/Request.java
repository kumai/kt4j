package kt4j;

/**
 * A request to the Kyoto Tycoon.
 * 
 * @author kumai
 */
public abstract class Request {
    public final Command command;

    protected Request(Command command) {
        this.command = command;
    }
    
    public enum Command {
        GET("get"),
        GET_BULK("get_bulk", (byte) 0xBA),
        SET("set"),
        SET_BULK("set_bulk", (byte) 0xB8),
        REMOVE("remove"),
        REMOVE_BULK("remove_bulk", (byte) 0xB9),
        INCREMENT("increment"),
        INCREMENT_DOUBLE("increment_double"),
        CAS("cas"),
        CLEAR("clear"),
        SEIZE("seize"),
        REPLACE("replace"),
        ADD("add"),
        MATCH_PREFIX("match_prefix"),
        MATCH_REGEX("match_regex"),
        PLAY_SCRIPT("play_script", (byte) 0xB4),
        VOID("void"),
        SYNCHRONIZE("synchronize"),
        VACUUM("vacuum"),
        STATUS("status"),
        REPORT("report"),
        ECHO("echo"),
        ;
        
        public final byte magic;
        public final String procedureName;
        
        private Command(String procedureName) {
            this(procedureName, (byte) 0x00);
        }
        
        private Command(String procedureName, byte magic) {
            this.procedureName = procedureName;
            this.magic = magic;
        }
        
        public boolean isBinarySupported() {
            return (magic != 0);
        }
    }

}
