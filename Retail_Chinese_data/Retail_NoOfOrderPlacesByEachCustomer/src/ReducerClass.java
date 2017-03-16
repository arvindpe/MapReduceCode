import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerClass extends Reducer<Text,LongWritable,Text,Text>
	   {
		 
		    
		    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		      long sum = 0;
				long count=0;
				
		         for (LongWritable val : values)
		         {       	
		        	sum += val.get();   
		        	count++;
		         }
		         String mycount=Long.toString(count);
		         String mysum=Long.toString(sum);
		    		      
		      context.write(key, new Text(mycount+","+mysum));
		      
		    }
	   }