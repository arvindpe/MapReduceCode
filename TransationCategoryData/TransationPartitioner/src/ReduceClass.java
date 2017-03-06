import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


 class ReduceClass extends Reducer<Text,Text,Text,Text>

{
	  public Text outputkey=new Text(); 
	  public Text outputvalue =new Text();
public void Reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
	for(Text val:values)
	{
		String record[]=val.toString().split(","); 	
		String custid=record[0];
		String amt=record[1];
		String category=record[2];	
		outputkey.set(custid);
		outputvalue.set(amt+" "+category);
	}
	context.write(outputkey,outputvalue);
	}
}
