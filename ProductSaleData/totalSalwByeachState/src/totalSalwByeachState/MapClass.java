package totalSalwByeachState;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
{
 public void map(LongWritable key,Text value,Context context)
 {
	 try{
         String[] str = value.toString().split(",");	
         String state=str[4];
         int quantity=Integer.parseInt(str[2]);
         int price=Integer.parseInt(str[3]);
         context.write(new Text(state), new IntWritable(quantity*price));
     
      }
      catch(Exception e)
      {
         System.out.println(e.getMessage());
      } 
	 
	 
 }
}
