import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MonthProducts
{

	public static class NovMapper extends
			Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(";");
			context.write(new Text(parts[5]), new Text("nov\t" +parts[8]));
		}
	}

	public static class DecMapper extends
			Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(";");
			context.write(new Text(parts[5]), new Text("dec\t" +parts[8]));
		}
	}
	public static class JanMapper extends
	Mapper<LongWritable, Text, Text, Text> 
	{
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException 
		{
	String record = value.toString();
	String[] parts = record.split(";");
	context.write(new Text(parts[5]), new Text("jan\t" + parts[8]));
		}
	}
	
	public static class FebMapper extends
	Mapper<LongWritable, Text, Text, Text>
	{
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException 
		{
	String record = value.toString();
	String[] parts = record.split(";");
	context.write(new Text(parts[5]), new Text("feb\t" + parts[8]));
		}
	}

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String pro_id =key.toString();
			long novtotal=0;
			long dectotal=0;
			long jantotal=0;
			long febtotal=0;
			for (Text t : values) {
				String parts[] = t.toString().split("\t");
				if (parts[0].equals("nov"))
				{
					novtotal += Long.parseLong(parts[1]);
					
				} 
				else if (parts[0].equals("dec"))
				{
					dectotal += Long.parseLong(parts[1]);
					
				} 
				else if (parts[0].equals("jan"))
				{
					jantotal += Long.parseLong(parts[1]);
					
				} 
				else if (parts[0].equals("feb"))
				{
					febtotal += Long.parseLong(parts[1]);
					
				} 
				
			}
			String nov = String.format("%d", novtotal);
			String dec = String.format("%d", dectotal);
			String jan = String.format("%d", jantotal);
			String feb = String.format("%d", febtotal);
			String row=nov+"\t"+dec+"\t"+jan+"\t"+feb;
			context.write(new Text(pro_id), new Text(row));
		}
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
	    job.setJarByClass(MonthProducts.class);
	    job.setJobName("comprehensiv analysis of a products totalsale on month basis");
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, NovMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, DecMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]),TextInputFormat.class, JanMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]),TextInputFormat.class, FebMapper.class);
		
		
		Path outputPath = new Path(args[4]);
		FileOutputFormat.setOutputPath(job, outputPath);
		//outputPath.getFileSystem(conf).delete(outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}