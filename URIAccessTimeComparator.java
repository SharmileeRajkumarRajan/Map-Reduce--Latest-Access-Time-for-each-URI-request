import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * A comparator for the KeyPair class that compares by URI and then Access Time Desc
 * This comparator is used for sorting the keypair by the mapreduce framework
 */
public class URIAccessTimeComparator extends WritableComparator {
	
	public URIAccessTimeComparator() {
		super(KeyPair.class, true);	
	}
	
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		SimpleDateFormat formatter=new SimpleDateFormat("dd/MMM/YYYY:HH:mm:ss");
		Date key1_date=null;
		Date key2_date=null;
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		 try {
			 key1_date=formatter.parse(key1.getaccesstime().toString());
			 key2_date=formatter.parse(key2.getaccesstime().toString());
		} 
		 catch (Exception e) {
			 System.out.println("Exception is"+e );	
		}
		int result = key1.getURI().compareTo(key2.getURI());
		if (result == 0)
			return -key1_date.compareTo(key2_date);
		else
			return result;
	}
}
