import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// Mapper:
// stopId;;;tripId;;;dayId: timestamp, predict_arrival_time, subway_line
// stopId;;;tripId;;;dayId: timestamp, predict_arrival_time, subway_line
// stopId;;;tripId;;;dayId: timestamp, predict_arrival_time, subway_line
// stopId;;;tripId;;;dayId: real_arrival_time     # at most one such row

// Reducer:
// # save `(timestamp, predict_arrival_time)`s to a list
// # save stopId, subway_line, real_arrival_time to variables
// # iterate thru list to get rows like these (will be sorted by station tho):
// timestamp, predict_arrival_time, real_arrival_time, stopId, subway_line

public class GenSubwayDataset {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GenSubwayDataset <input file> <output dir>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(GenSubwayDataset.class);
        job.setJobName("GenSubwayDataset");
        // job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(GenSubwayDatasetMapper.class);
        job.setReducerClass(GenSubwayDatasetReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}