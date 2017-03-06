import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;


public class LogPartition extends Configured implements Tool
{

	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text> 
	{
		public Text key=
		
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
