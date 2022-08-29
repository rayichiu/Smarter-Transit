import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YellowTaxi {

  public static void main(String[] args) throws Exception { 
    if (args.length != 2) {
      System.err.println("Usage: YellowTaxi <input path> <output path>");
      System.exit(-1);
    }

    Job job = Job.getInstance();
    job.setJarByClass(YellowTaxi.class); 
    job.setJobName("YellowTaxi");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(YellowTaxiMapper.class);
    job.setNumReduceTasks(0);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1); 
  }
}
