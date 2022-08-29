import java.io.IOException;
import org.apache.commons.lang3.ArrayUtils;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeProfilingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {

        String line = value.toString();
        int strLen = line.length();
        String[] info = new String[19];
        String val = "";
        boolean skip = false;
        for (int i = 0, j = 0; i < strLen; i++) {
            if (line.charAt(i) == ';' && skip == false) {
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

        String borough = info[8];
        String premises = info[10].toUpperCase();

        //sum the number of incidents based on borough
        if (!borough.isEmpty()) {
            context.write(new Text(borough), new IntWritable(1));
        }

        //sum the number of incidents occuring around transit
        if (premises.contains("TRANSIT")) {
            context.write(new Text("TRANSIT"), new IntWritable(1));
        }

    }
}
