import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.text.*;
import org.apache.commons.lang3.ArrayUtils;
import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeHistoricMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {

        String line = value.toString();
        int strLen = line.length();
        String[] info = new String[37];
        String val = "";
        boolean skip = false;
        for (int i = 0, j = 0; i < strLen; i++) {
            if (line.charAt(i) == ',' && skip == false) {
                if (line.charAt(i-1) == '"') continue;
                info[j++] =val;
                val = "";
            }
            else if (line.charAt(i) == '"') {
                if (skip == false) {
                    skip = true;
                    val += line.charAt(i);
                }
                else if (skip == true) {
                    skip = false;
                    val += line.charAt(i);
                    info[j++] = val;
                    val = "";
                }
            }
            else {
                val += line.charAt(i);
                if (i == strLen - 1) info[j++] = val;
            }
        }

        int[] indArr =  {0, 1, 2, 3, 4, 8, 10, 12, 13, 14, 15, 18, 21, 22, 26, 27, 28, 30, 31};
        String row = "";

        ArrayList<String> cols = new ArrayList<>();
        for (int i = 0; i < info.length; i++) {
            if (ArrayUtils.contains(indArr, i)) {
                if ((i == 1 || i == 3) && info[i] != "") {
                    info[i] = info[i].replace('/', '-'); 
                    info[i] = info[i].substring(6) + "-" + info[i].substring(0, 5);
                }
                if (info[i].contains("\"")) info[i] = info[i].replace('"', '\u0000');
                cols.add(info[i]);
                row += info[i] + ';';
            }
        }

        //check the vadility of time
        if (!cols.get(1).isEmpty()) {
            if (cols.get(3).isEmpty()) {
                context.write(NullWritable.get(), new Text(row));
            }
            else {
                SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String fromString = cols.get(1) + " " + cols.get(2);
                String toString = cols.get(3) + " " + cols.get(4);
                try {
                    Date fromDate = fmt.parse(fromString);
                    Date toDate = fmt.parse(toString);

                    if (fromDate.compareTo(toDate) <= 0) {
                        context.write(NullWritable.get(), new Text(row));
                    } 
                } catch (ParseException e) {
                    //TODO: handle exception
                    e.printStackTrace();
                }
            }
        }
    }
}
