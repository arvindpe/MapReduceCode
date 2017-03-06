

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapClass extends Mapper<LongWritable,Text,Text,Text>
{
 public void map(LongWritable key,Text value,Context context)
 {
	 try{
         String[] str = value.toString().split(",");	
         String custid=str[2];
         String amt=str[3];
         String category=str[4];
         context.write(new Text(custid), new Text(custid+","+amt+","+category));
     
      }
      catch(Exception e)
      {
         System.out.println(e.getMessage());
      } 
	 
	 
 }
}
