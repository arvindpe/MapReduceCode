import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceClass extends Reducer<Text,Text,NullWritable, Text>
	 {
		
		   private TreeMap<Double, Text> topMap = new TreeMap<Double, Text>();
		   
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		    {
		      long sum = 0;
		      long quantity=0;
		      double avg=0;
				String myvalue="";
				String myavg="";
		         for (Text val : values)
		         {       	
		        	   String str[]=val.toString().split(",");
		        	   long sale=Long.parseLong(str[1]);
		        	   sum+=sale;
		        	   long qty=Long.parseLong(str[0]);
		        	   quantity+=qty;
		        	   
		        	   avg=sum/quantity;
		        	   
		         }
		         myvalue=key.toString();
		         myavg=Double.toString(avg);
		         myvalue=myvalue+","+myavg;
		         
		         topMap.put(new Double(avg),new Text(myvalue));
		        
		    }
	      
		    protected void cleanup(Context context) throws IOException, InterruptedException 
	    	{
	    	
		    	for (Text t : topMap.values())
		    	{
		    		context.write(NullWritable.get(), t);
	    	
            
		    	}
	    	} 
			
		}