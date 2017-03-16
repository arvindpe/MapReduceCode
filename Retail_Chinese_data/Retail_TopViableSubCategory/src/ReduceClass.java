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
		     
		      long total_sale=0;
		      long total_cost=0;
		      long profit=0;
				String myvalue="";
				String myprofit="";
		         for (Text val : values)
		         {       	
		        	 String str[]=val.toString().split(",");
		        	total_sale += Long.parseLong(str[1]);  
		        	total_cost += Long.parseLong(str[2]);
		        	
		        	if(total_sale >total_cost)
		        	{
		        		
		        		profit=total_sale-total_cost;
		        		
		        	}
		         }
		         myvalue=key.toString();
		         myprofit=String.format("%d",profit);
		         myvalue=myvalue+","+myprofit;
		         
		         topMap.put(new Long(profit),new Text(myvalue));
		         if (topMap.size() > 5)
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