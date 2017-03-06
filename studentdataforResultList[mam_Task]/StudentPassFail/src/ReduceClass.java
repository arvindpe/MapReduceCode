import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


 public class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
	   {
		
		    public Text output=new Text();
		    
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		    {
		    	String status=null;
		         for (Text val : values)
		         {       	
		        	String str[]=val.toString().split(",");  
		        	String name=str[0];
		        	 status=str[1];
		        	output.set(name);
		        	
		         }
		         
		      	    System.out.println("List of"+status+" students:");  
		      context.write(NullWritable.get(), output);
		      
		    }
	   }