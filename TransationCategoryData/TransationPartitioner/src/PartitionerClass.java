import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class PartitionerClass extends Partitioner <Text,Text>{

	public int getPartition(Text key, Text value, int numReduceTasks)
    {
       String[] str = value.toString().split(",");
       String category= str[2];


       if(category.equals("Racquet Sports"))
       {
          return 0;
       }
       else if(category.equals("Games"))
       {
          return 1 ;
       }
       else if(category.equals("Outdoor Recreation"))
       {
          return 2;
       }
       else if(category.equals("Exercise & Fitness"))
       {
    	   return 3;
       }
       else if(category.equals("Jumping"))
       {
    	   return 4;
       }
       else if(category.equals("Team Sports"))
       {
    	   return 5;
       }
       else if(category.equals("Outdoor Play Equipment"))
       {
    	   return 6;
       }
       else if(category.equals("Indoor Games"))
       {
    	   return 7;
       }
       else if(category.equals("Water Sports"))
       {
    	   return 8;
       }
       else if(category.equals("Gymnastics"))
       {
    	   return 9;
       }
       else if(category.equals("Combat Sports"))
       {
    	   return 10;
       }
       else if(category.equals("Winter Sports"))
       {
    	   return 11;
       }
       else if(category.equals("Air Sports"))
       {
    	   return 12;
       }
       else if(category.equals("Puzzles"))
       {
    	   return 13;
       }
       else
       {
    	   return 14;
       }
    }

}
