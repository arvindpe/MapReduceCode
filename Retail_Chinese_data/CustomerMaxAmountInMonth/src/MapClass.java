import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	{	
		IntWritable outvalue=new IntWritable();
		Text outkey=new Text();
		public void map(LongWritable key,Text value,Context context)
		{
			try
			{
				String [] record=value.toString().split(";");
				String cust_id=record[1];
				int sale=Integer.parseInt(record[8]);
				outkey.set(cust_id);
				outvalue.set(sale);
				context.write(outkey,outvalue);
			}
			catch(Exception ex)
			{
				System.out.println(ex.getMessage());
			}
			
			
		}
	}