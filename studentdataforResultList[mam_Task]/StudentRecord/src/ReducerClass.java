import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class ReduceClass extends Reducer<Text,Text,NullWritable,Text>

{
	 public Text outputkey=new Text();
	 public Text outputvalue =new Text();
public void Reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{

	String status=null;
	for(Text val:values)
	{
		String record[]=val.toString().split(","); 	
		String name=record[0];
		status=record[1];
		outputvalue.set(name);
	}
		
		System.out.println("Name of"+status+"students are:");
	context.write(null, outputvalue);
	}
}