import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxAmount {

	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		   
		    Job job = new Job(conf, "customer Who spent maximum amount in a month and in all 4 months");
		    job.setJarByClass(MaxAmount.class);
		    job.setMapperClass(MapClass.class);
		    job.setReducerClass(ReduceClass.class);
		   
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

}
