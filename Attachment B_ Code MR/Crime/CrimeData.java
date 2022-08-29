import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;

public class CrimeData {
    
    public static void main (String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: CrimeData <input1 path> <input2 path> <input3 path> <output path>");
            System.exit(-1);
        }

        //First Job - Cleaning: filter selected columms 
        Job job1 = Job.getInstance();
        job1.setJarByClass(CrimeData.class);
        job1.setJobName("Crime Cleaning");

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, CrimeHistoricMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, CrimeCurrentMapper.class);
        job1.setNumReduceTasks(0);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        job1.waitForCompletion(true);


        //Second Job - Profiling: total number of crime incidents in a given borough name
        Job job2 = Job.getInstance();
        job2.setJarByClass(CrimeData.class);
        job2.setJobName("Crime Profiling");

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapperClass(CrimeProfilingMapper.class);
        job2.setCombinerClass(CrimeProfilingReducer.class);
        job2.setNumReduceTasks(1);
        job2.setReducerClass(CrimeProfilingReducer.class);

        FileInputFormat.setInputPaths(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}