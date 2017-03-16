import java.io.IOException;
import java.text.DecimalFormat;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceClass extends Reducer<Text,Text,NullWritable, Text>
	 {
		
		
		   
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		    {
		      long sum = 0;
		      long quantity=0;
		      double avg=0.00;
		      long cost=0;
				String myvalue="";
				String myavg="";
				//DecimalFormat df=new DecimalFormat();
		         for (Text val : values)
		         {       	
		        	   String str[]=val.toString().split(",");
		        	    cost=Long.parseLong(str[1]);
		        	   sum+=cost;
		        	   long qty=Long.parseLong(str[0]);
		        	   quantity+=qty;
		        	   
		        	   avg=(double)sum/(double)quantity;
		        	   
		        	
		        	   
		         }
		         String id=key.toString();
		       //  myavg=df.format(avg);
		       myavg=String.format("2f",avg);
		         String mycost=Long.toString(cost);
		         String myqty=Long.toString(quantity);
		         myvalue=id+","+myqty+","+mycost+","+myavg;
		         
		     context.write(NullWritable.get(), new Text(myvalue));
		        
		    }
	      
		   
	    	 
			
		}