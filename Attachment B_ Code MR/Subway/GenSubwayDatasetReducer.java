import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.List;


public class GenSubwayDatasetReducer extends Reducer<Text, Text, NullWritable, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String stopId = key.toString().split(";;;")[0];
        String subwayLine = null;
        String actualArrivalTime = null;

        List<String> predictions = new ArrayList<String>(); // "timestamp, predict_arrival_time"
        for (Text value_text : values) {
            String value = value_text.toString();

            if (value.contains(", ")) { // prediction
                String timestamp = value.split(", ")[0];
                String predictedArrivalTime = value.split(", ")[1];
                predictions.add(timestamp + ", " + predictedArrivalTime);

                subwayLine = value.split(", ")[2];
                // actualArrivalTime = max(predictedArrivalTime)
                if (actualArrivalTime == null || predictedArrivalTime.compareTo(actualArrivalTime) > 0) {
                    actualArrivalTime = predictedArrivalTime;
                }
            } else { // actual arrival time
                actualArrivalTime = value;
            }
        }

        for (String s : predictions) {
            String timestamp = s.split(", ")[0];
            String predictedArrivalTime = s.split(", ")[1];
            context.write(NullWritable.get(), new Text(
                timestamp + "," + predictedArrivalTime + "," + actualArrivalTime + "," + stopId + "," + subwayLine
                ));
        }
    }
}