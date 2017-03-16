
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public  class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
		
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try
	         {
	            String[] parts= value.toString().split(";");	 
	           
	            String now=parts[0];
	            String day=getDay(now);
	            long sale=Long.parseLong(parts[8]);
	          
	        	   
	        	 
	        	  
	           
	            context.write(new Text(day),new LongWritable(sale));
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	      
	      private String getDay(String date) throws ParseException
	      {
	    	//create the date format in which you will parse the date
	    	  java.util.Date datefrm=null;
	    	  SimpleDateFormat df = new SimpleDateFormat( "dd/MM/yy" );
	    	  //parse in the date
	    	  datefrm = df.parse( date);
	    	  //change the pattern to output the day of week
	    	  df.applyPattern( "EEE" );
	    	  //print the formatted date out
	    		return df.format( date ) ;
	    	  
		 
	      }
	   }