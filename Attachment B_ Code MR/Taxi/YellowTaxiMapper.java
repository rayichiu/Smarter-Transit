import java.io.IOException;

import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.SimpleDateFormat;  
import java.util.Date;  
import java.util.concurrent.TimeUnit;

public class YellowTaxiMapper
    extends Mapper<LongWritable, Text, NullWritable, Text> {

  private final static SimpleDateFormat fmt = new
      SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private final static SimpleDateFormat df = new SimpleDateFormat("yyyy");

  @Override
  public void map(LongWritable key, Text value, Context context) 
      throws IOException, InterruptedException {
    String[] words = value.toString().split(",");
    String pickup_date_time_str = words[1];
    String dropoff_date_time_str = words[2];
    Boolean check_year = false;
    Boolean check_pick_drop = false;
    Boolean check_distance = false;
    
    try{
      Date pickup_date_time_obj = fmt.parse(pickup_date_time_str);
      Date dropoff_date_time_obj = fmt.parse(dropoff_date_time_str);

      // check 2021
      int pick_up_year = Integer.parseInt(words[1].substring(0,4));
      int drop_off_year = Integer.parseInt(words[2].substring(0,4));
      if ((pick_up_year >= 2018 && pick_up_year <= 2021) &&
          (drop_off_year >= 2018 && drop_off_year <= 2021) ){
        check_year = true;
      }
      // pickup > dropoff and duration < 24 hr
      long diffInMillies = Math.abs(dropoff_date_time_obj.getTime() - pickup_date_time_obj.getTime());
      long diffInHours = TimeUnit.MILLISECONDS.toHours(diffInMillies);
      if (dropoff_date_time_obj.after(pickup_date_time_obj) && diffInHours < 24){
        check_pick_drop = true;
      }
      // check distance
      try{ 
        float f=Float.parseFloat(words[4]);
        if (f > 0){
	  check_distance = true;
        }
      } catch (Exception e){
        check_distance = false;
      }

      // correct data
      if (check_year && check_pick_drop && check_distance){
        String new_output = words[1]+","+words[4]+","+words[7];
        context.write(NullWritable.get(), new Text(new_output));

        // count day of the week
        int day = pickup_date_time_obj.getDay();
        context.getCounter("DayOfTheWeek", Integer.toString(day)).increment(1);
        // count different time of the day
        int time = pickup_date_time_obj.getHours();
        context.getCounter("HourOfTheDay", Integer.toString(time)).increment(1);
      }
    } catch (Exception e){
      System.out.println("Failed with exception: " + e);
    }
    
  } 
}
