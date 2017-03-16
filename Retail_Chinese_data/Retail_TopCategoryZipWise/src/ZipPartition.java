import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class ZipPartition extends Partitioner<Text, Text>
{
		@Override
	public int getPartition(Text key, Text value, int numReduceTasks)
	{
		String record[]=value.toString().split(",");
		
		String zip=record[0].trim();
		
		if(zip.equals("A"))
		{
			return 0;	
		}
		else if(zip.equals("B"))
		{
			return 1;
		}
		else if(zip.equals("C"))
		{
			return 2;
		}
		else if(zip.equals("D"))
		{
			
			return 3;
		}
		else if(zip.equals("E"))
		{
			
			return 4;
		}
		else if(zip.equals("F"))
		{
			return 5;
		}
		else if(zip.equals("G"))
		{
			
			return 6;
		}
		else
		{
			return 7;
		}
		
	}		
		
}


