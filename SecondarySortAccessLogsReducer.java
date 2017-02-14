import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reducer for the LatestAccessTime for each URI problem. 
 * The reducer receives a URI as a key and all the access times associated with it in a DESC order
 * The reducer simply emits the URI and the access time (latest Access time) in the values
 */
public class SecondarySortAccessLogsReducer extends Reducer<KeyPair, Text, Text, Text>{
	public void reduce(KeyPair key, Iterable<Text> values, Context context)
			  throws IOException, InterruptedException {
		
		//Getting the latest access time(first value) for each URI
		for(Text accessTime:values)
		{
			context.write(key.getURI(),accessTime);
			break;
		}
	}
}
