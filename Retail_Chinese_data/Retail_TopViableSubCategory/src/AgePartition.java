import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class AgePartition extends Partitioner<Text, Text>
{
	 public int getPartition(Text key, Text value, int numReduceTasks)
	 {
		 String record[]=value.toString().split(",");

		 String ageGroup=record[0].trim();

		 if(ageGroup.equals("A"))
		 {
		 	return 0;	
		 }
		 else if(ageGroup.equals("B"))
		 {
		 	return 1;
		 }
		 else if(ageGroup.equals("C"))
		 {
		 	return 2;
		 }
		 else if(ageGroup.equals("D"))
		 {
		 	
		 	return 3;
		 }
		 else if(ageGroup.equals("E"))
		 {
		 	
		 	return 4;
		 }
		 else if(ageGroup.equals("F"))
		 {
		 	return 5;
		 }
		 else if(ageGroup.equals("G"))
		 {
		 	
		 	return 6;
		 }
		 else if(ageGroup.equals("H"))
		 {
		 	return 7;
		 }
		 else if(ageGroup.equals("I"))
		 {
		 	return 8;
		 	
		 }
		 else
		 {
		 	return 9;
		 }
		 
		 
	 }
}


