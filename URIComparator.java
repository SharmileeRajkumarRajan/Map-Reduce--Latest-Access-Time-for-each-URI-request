import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A comparator for the the Key-Pair class. This comparator compares only by URI.
 * This comparator is used for grouping the key pairs.
 */
public class URIComparator extends WritableComparator{
	
	public URIComparator() {
		super(KeyPair.class, true);
		
	}
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		return key1.getURI().compareTo(key2.getURI());
	}
}
