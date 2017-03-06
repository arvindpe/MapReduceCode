import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import javax.lang.model.SourceVersion;
import javax.tools.Tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class DriverClass extends Configured implements Tool, org.apache.hadoop.util.Tool
{
	 public int  run(String[] arg) throws Exception
	  {
		  
		    Configuration conf = new Configuration();
		    Job job = new Job(conf, "Partion pass fail wise");
		    job.setJarByClass(DriverClass.class);
		    job.setJobName("list of pass and fail student ");
		    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    job.setMapperClass(MapClass.class);
		    job.setPartitionerClass(PartitionClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(2);
		    
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(arg[0]));
		    FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		    return 0;
		  }
	 
	 public static void main(String args[]) throws Exception
		{
			ToolRunner.run(new Configuration(),new DriverClass(), args);
			System.exit(0);
		}
	@Override
	public Set<SourceVersion> getSourceVersions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int run(InputStream arg0, OutputStream arg1, OutputStream arg2,
			String... arg3) {
		// TODO Auto-generated method stub
		return 0;
	}
	

}