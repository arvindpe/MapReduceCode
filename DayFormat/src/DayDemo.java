import java.text.SimpleDateFormat;
import java.util.Date;

public class DayDemo
{
  public static void main(String[] args) {
    
    String strDateFormat = "EEEE";
    SimpleDateFormat sdf = new SimpleDateFormat(strDateFormat);
    System.out.println("Current day of week in EEEE format : " + sdf.format(new Date("2001-01-01 00:00:00")));

  }
}