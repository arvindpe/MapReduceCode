import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
	   {
		   public Text customer=new Text();
		    int max=0;
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      int sum = 0;
				
		         for (IntWritable val : values)
		         {       	
		        	sum += val.get();      
		         }
		         if(sum>max)
			       {
			    	   max=sum;
			    	   customer.set(key);
			       }
		         
		    }
		    
		    protected void cleanup(Context context) throws IOException, InterruptedException 
	    	{
	    	
	    	context.write(customer, new IntWritable(max));
	    	
               
            }
	   }