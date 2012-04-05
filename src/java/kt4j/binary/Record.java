package kt4j.binary;

import java.util.Arrays;

/**
 * 
 * @author kumai
 *
 */
class Record {
    static final int DBIDX_LENGTH = 2;
    static final int KSIZ_LENGTH = 4;
    static final int VSIZ_LENGTH = 4;
    static final int XT_LENGTH = 8;
    static final int HEADER_LENGTH = DBIDX_LENGTH + KSIZ_LENGTH + VSIZ_LENGTH + XT_LENGTH;
    
    final int dbidx;
    final long xt;
    final byte[] key;
    final byte[] value;
   
    Record(int dbidx, byte[] key, byte[] value, long xt) {
        this.dbidx = dbidx;
        this.xt = xt;
        this.key = key;
        this.value = value;
    }

    int length() {
        return HEADER_LENGTH + key.length + value.length;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + dbidx;
        result = prime * result + Arrays.hashCode(key);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Record other = (Record) obj;
        if (dbidx != other.dbidx)
            return false;
        if (!Arrays.equals(key, other.key))
            return false;
        return true;
    }
    
}
