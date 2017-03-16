import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapClass extends
	Mapper<LongWritable, Text,Text, Text>
	{
		

		public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException 
		{
			String record = value.toString();
			String[] parts = record.split(";");
			String product_id=parts[5];
			String sale=parts[8];
			String zip=parts[3];
		
			context.write(new Text(product_id),new Text(zip+","+sale));
		
		}
	}