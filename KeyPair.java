import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class KeyPair implements WritableComparable<KeyPair>{
 
	//the key pair holds the URI and accesstime
	private Text local_URI;
	private Text local_accesstime;
	SimpleDateFormat format=new SimpleDateFormat("dd/MMM/YYYY:HH:mm:ss");
	Date date_CurrenctKeyPair=null;
	Date date_OtherPair=null;
	//The default constructor
	public KeyPair()
	{
		local_URI = new Text();
		local_accesstime= new Text();
	}
	
	//constructor, initializing the URI and accesstime
	public KeyPair(String uri, String accesstime)
	{
		local_URI = new Text(uri);
		local_accesstime= new Text(accesstime);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		local_URI.readFields(in);
		local_accesstime.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		local_URI.write(out);
		local_accesstime.write(out);
	}

	@Override
	public int compareTo(KeyPair otherPair) {
		// TODO Auto-generated method stub
		int result= local_URI.compareTo(otherPair.local_URI);
		try {
			date_CurrenctKeyPair=format.parse(local_URI.toString());
			date_OtherPair=format.parse(otherPair.local_accesstime.toString());
			
		} 
		catch (Exception e) {
			System.out.println("Exception is"+e );
		}
		if (result!=0)
			return result;
		else
			return date_CurrenctKeyPair.compareTo(otherPair.date_OtherPair);
		
	}
	
	//the Getter and setter methods
	public Text getURI() {
		return local_URI;
	}

	public void setURI(Text URI) {
		this.local_URI = URI;
	}

	public Text getaccesstime() {
		return local_accesstime;
	}

	public void setaccesstime(Text accesstime) {
		this.local_accesstime = accesstime;
	}

}
