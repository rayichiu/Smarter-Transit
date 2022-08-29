import java.io.IOException;
import java.rmi.dgc.VMID;
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

public class CrimeCurrentMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {

        String line = value.toString();
        int strLen = line.length();
        String[] info = new String[39];
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

        int[] indArr =  {0, 2, 3, 4, 5, 6, 13, 14, 15, 16, 17, 19, 20, 22, 26, 30, 31, 32, 33};

        ArrayList<String> temp = new ArrayList<>();
        for (int i = 0; i < info.length; i++) {
            if (ArrayUtils.contains(indArr, i)) {
                if ((i == 3 || i == 5) && info[i] != "") {
                    info[i] = info[i].replace('/', '-'); 
                    info[i] = info[i].substring(6) + "-" + info[i].substring(0, 5);
                }
                if (info[i].contains("\"")) info[i] = info[i].replace('"', '\u0000');
                temp.add(info[i]);
                // row += info[i] + ';';
            }
        }

        //reorder the columns
        String row = "";
        ArrayList<String> cols = new ArrayList<>(temp.size());
        for (int j = 0; j < 19; j++) {
            String v = "";
            //cmplnt_id
            if (j == 0) v = temp.get(0);
            //cmplnt_fr_dt
            else if (j == 1) v = temp.get(2);
            //cmplnt_fr_tm
            else if (j == 2) v = temp.get(3);
            //cmplnt_to_dt
            else if (j == 3) v = temp.get(4);
            //cmplnt_to_tm
            else if (j == 4) v = temp.get(5);
            //ofns_desc
            else if (j == 5) v = temp.get(8);
            //pd_desc
            else if (j == 6) v = temp.get(11);
            //law_cat_cd
            else if (j == 7) v = temp.get(6);
            //boro_nm
            else if (j == 8) v = temp.get(1);
            //loc_of_occur_desc
            else if (j == 9) v = temp.get(7);
            //prem_typ_desc
            else if (j == 10) v = temp.get(12);
            //parks_nm
            else if (j == 11) v = temp.get(9);
            //x_coord
            else if (j == 12) v = temp.get(15);
            //y_coord
            else if (j == 13) v = temp.get(16);
            //transit_disrict
            else if (j == 14) v = temp.get(14);
            //latitude
            else if (j == 15) v = temp.get(17);
            //longitude
            else if (j == 16) v = temp.get(18);
            //partol_boro
            else if (j == 17) v = temp.get(10);
            //station_nm
            else if (j == 18) v = temp.get(13);
            
            row += v + ';';
            cols.add(j, v);
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
