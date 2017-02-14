import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortAccessLogsMapper 
	extends Mapper<LongWritable, Text, KeyPair, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   
   //Extracting the fields
	 String valueForOutput=null;
	 String line=value.toString().trim();
	 //Checking whether the line is Empty
	 if(!line.isEmpty() && line.contains("GET")&& line.contains("HTTP"))
	{
		 if(line.substring(line.indexOf("["), line.indexOf("]")).isEmpty()==false)
		 {
		   if(line.substring(line.indexOf("GET")+3,line.indexOf("HTTP")).isEmpty()==false)
		   {
		   valueForOutput=line.substring(line.indexOf("["), line.indexOf("]")).trim();
		   String URI=line.substring(line.indexOf("GET")+3, line.indexOf("HTTP")).trim();
		   String accessTime=valueForOutput.substring(1,valueForOutput.length()-5).trim();
		   context.write(new KeyPair(URI,accessTime),new Text(valueForOutput +"]"));
		   }
		 }
	 } 
 }
}

  


 