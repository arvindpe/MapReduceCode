

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapClass extends Mapper<LongWritable,Text,Text,Text>
{
 public void Map(LongWritable key,Text value,Context context)
 {
	 try{
         String[] str = value.toString().split(",");	
         String Itm_id=str[1];
         String qty=str[2];
         String state=str[4];
         
         String row=state+","+qty;
         context.write(new Text(Itm_id), new Text(row));
     
      }
      catch(Exception e)
      {
         System.out.println(e.getMessage());
      } 
	 
	 
 }
}
