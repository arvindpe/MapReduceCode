import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class PartitionClass extends Partitioner<Text, Text>
{
	 public int getPartition(Text key, Text value, int numReduceTasks)
     {
        String[] str = value.toString().split(",");
        String status =str[1];


        if(status.equals("pass"))
        {
           return 0;
        }
        else 
        {
           return 1 ;
        }
     }
}
