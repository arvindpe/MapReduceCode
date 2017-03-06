import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


 class PartitionerClass extends Partitioner <Text,Text>
 {

	public int getPartition(Text key, Text value, int numReduceTasks)
    {
       String[] str = value.toString().split(",");
       String state= str[0];
       
       if(state.equals("MAH"))
       {
    	   return 0;
       }
       
       else 
       {
    	   return 1;
    	   
       }
       
    }

}
