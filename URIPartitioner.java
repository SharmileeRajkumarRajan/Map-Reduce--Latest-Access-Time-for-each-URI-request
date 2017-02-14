import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A custom partitioner class that partitions the keys only by URI
 * This ensures that all keys with the same URI go to the same reducer
 */
public class URIPartitioner extends Partitioner<KeyPair, Text>{

	@Override
	public int getPartition(KeyPair key, Text value, int numReducers) {
		// TODO Auto-generated method stub
		return key.getURI().hashCode()% numReducers;
	
	}
	

}
