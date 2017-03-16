import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class ReduceClass extends Reducer<Text,Text,NullWritable, Text>
	 {
		
		   private TreeMap<Long, Text> topMap = new TreeMap<Long, Text>();
		   
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		    {
		      long sum = 0;
				String myvalue="";
				String mysum="";
				String zip="";
		         for (Text val : values)
		         {       	
		        	 String str[]=val.toString().split(",");
		        	sum += Long.parseLong(str[1]); 
		        	
		        	 zip=str[0];
		         }
		         myvalue=key.toString();
		         mysum=String.format("%d",sum);
		         myvalue=myvalue+","+mysum+","+zip;
		         
		         topMap.put(new Long(sum),new Text(myvalue));
		         if (topMap.size() > 1)
		         {
						topMap.remove(topMap.firstKey());
				}
		    }
	      
		    protected void cleanup(Context context) throws IOException, InterruptedException 
	    	{
	    	
		    	for (Text t : topMap.descendingMap().values())
		    	{
		    		context.write(NullWritable.get(), t);
	    	
            
		    	}
	    	} 
			
		}