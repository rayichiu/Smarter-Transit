import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WeatherNYCMapper extends Mapper <LongWritable, Text, NullWritable, Text> {
    private static final int DAYS = 31;
    private static final int ENDOFFSET = 269;

    @Override
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String ID = line.substring(0, 11);
      if (ID.matches("USW00014732") || ID.matches("USW00094728") || ID.matches("USW00094789")) {
        String[] ele_list = {"PRCP", "SNOW", "SNWD", "TMAX", "TMIN","PSUN", "TSUN",
                             "WT01", "WT02", "WT03","WT04","WT05","WT06","WT07",
                             "WT08","WT09","WT10","WT11","WT12","WT13","WT14","WT15",
                             "WT16","WT17","WT18","WT19","WT021","WT22","WV10", "WV03",
                             "WV07", "WV18", "WV20"};
        String year = line.substring(11, 15);
        String month = line.substring(15, 17);
        String ele = line.substring(17, 21);
        if (Arrays.asList(ele_list).contains(ele)) {
          int day = 0;
          String final_value;
          String ele_value;
          for (int i = 21; i < ENDOFFSET; i = i + 8) {
            ele_value = (line.substring(i ,i+5));
            day += 1;
            String date = String.format("%02d",day);
            final_value = ID + "," + ele + ","+ year + "-" + month + "-" + date + "," + ele_value;
            context.write(NullWritable.get(), new Text(final_value));
          }
        }
      }
    }
}



  