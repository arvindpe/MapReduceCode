import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartition extends Partitioner<Text, Text>
{
		@Override
	public int getPartition(Text key, Text value, int numReduceTasks)
	{
		String record[]=value.toString().split(",");
		
		String ageGroup=record[0].trim();
		
		if(ageGroup.equals("A"))
		{
			return 0;	
		}
		if(ageGroup.equals("B"))
		{
			return 1;	
		}
		if(ageGroup.equals("C"))
		{
			return 2;	
		}
		if(ageGroup.equals("D"))
		{
			return 3;	
		}
		if(ageGroup.equals("E"))
		{
			return 4;	
		}
		if(ageGroup.equals("F"))
		{
			return 5;	
		}
		if(ageGroup.equals("G"))
		{
			return 6;	
		}
		if(ageGroup.equals("H"))
		{
			return 7;
		}
		if(ageGroup.equals("I"))
		{
			return 8;
		}
		else 
		{
			return 9;
		}
		
		
	}		
		
}


