import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


 class ReduceClass extends Reducer<Text,Text,Text,IntWritable>

{
	 public Text outputkey=new Text();
	
public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
	int count=0;
	String total_sold_quantity="quantity sold for state";
	for(Text val:values)
	{
		String record[]=val.toString().split(","); 	
		int quantity=Integer.parseInt(record[1]);
		count=count+quantity;
		
	}
		
		outputkey.set(total_sold_quantity);
		System.out.println("Total quantity sold for this state are");
	
		context.write(outputkey,new IntWritable(count));
	}
}
