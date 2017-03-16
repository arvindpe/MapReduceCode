import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

class ReduceClass extends Reducer<Text,Text,NullWritable, Text>
	 {
		
		   private TreeMap<Double, Text> topMap = new TreeMap<Double, Text>();
		   
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		    {
		      long cost_price = 0;
		      long sale_price=0;
		      long profit=0;
		      int quantity=0;
		      double margine=0.00;
				String myvalue="";
				String mymargine="";
				String myquantity="";
				 String myprofit="";
		         for (Text val : values)
		         {       
		        	 String str[]=val.toString().split(",");
		        	 long cost=Long.parseLong(str[0]);
		        	 long sale=Long.parseLong(str[1]);
		        	 int qty=Integer.parseInt(str[2]);
		        	cost_price += cost; 
		        	sale_price += sale; 
		        	quantity +=qty; 
		        	
		        	profit=sale_price-cost_price;
		        	if(cost_price!=0)
		        	{
		        	
		        	margine=((sale_price-cost_price)*100)/cost_price;
		        	}
		        	
		         }
		         myvalue=key.toString();
		         mymargine=Double.toString(margine);
		         myprofit=String.format("%d",profit);
		         myquantity=String.format("%d",quantity);
		         myvalue=myvalue+","+myquantity+","+myprofit+","+mymargine;
		         
		         topMap.put(new Double(margine),new Text(myvalue));
		         
		    }
	      
		    protected void cleanup(Context context) throws IOException, InterruptedException 
	    	{
	    	
		    	for (Text t : topMap.descendingMap().values())
		    	{
		    		context.write(NullWritable.get(), t);
	    	
            
		    	}
	    	}
	 }
			