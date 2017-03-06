import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MapClass extends Mapper<LongWritable,Text, Text, Text> 
{
	private Map<String, String> abMap = new HashMap<String, String>();
	private Text outputKey = new Text();
	private Text outputValue = new Text();

	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		
		super.setup(context);
		 URI[] files =DistributedCache.getCacheFiles(context.getConfiguration());
       // URI[] files = context.getCacheFiles(); // getCacheFiles returns null

	    Path p = new Path(files[0]);
	   
	
		if (p.getName().equals("results.dat")) {
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
				String line = reader.readLine();
				while(line != null) 
				{
					String[] tokens = line.split("\\|\\|");
					String id = tokens[0];
					String status=tokens[1];
					abMap.put(id,status);
					line = reader.readLine();
				}
				reader.close();
		}
	
		
		if (abMap.isEmpty()) 
		{
			throw new IOException("MyError:Unable to load result.dat");
		}

	}

	
    protected void map(LongWritable key, Text value, Context context)
        throws java.io.IOException, InterruptedException {
    	
    	
    	String row = value.toString();
    	String[] tokens = row.split("\\|\\|");
    	String id = tokens[1];
    	String name=tokens[0];
    	String status = abMap.get(id); 
    	
    	String outvalue=name+","+status;
    	outputKey.set(id);
    	outputValue.set(outvalue);
  	  	context.write(outputKey,outputValue);
    }  
}

