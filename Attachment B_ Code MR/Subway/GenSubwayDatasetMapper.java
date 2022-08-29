import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import java.util.Map;
import java.util.List;
import java.io.*;


public class GenSubwayDatasetMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        Gson gson = new Gson();
        Map data = gson.fromJson(gson.fromJson(line, String.class), Map.class);
        Map header = gson.fromJson(gson.toJson(data.get("header")), Map.class);

        String timestamp = header.get("timestamp").toString();

        List entities = gson.fromJson(gson.toJson(data.get("entity")), List.class);
        for (Object entityObj : entities) {
            Map entity = gson.fromJson(gson.toJson(entityObj), Map.class);
            if (!entity.containsKey("tripUpdate")) {
                continue;
            }

            Map trip = gson.fromJson(gson.toJson(
                gson.fromJson(gson.toJson(
                    entity.get("tripUpdate")), Map.class)
                .get("trip")), Map.class);
            String tripId = trip.get("tripId").toString();
            String subwayLine = trip.get("routeId").toString();

            if (!gson.fromJson(gson.toJson(entity.get("tripUpdate")), Map.class).containsKey("stopTimeUpdate")) {
                continue;
            }

            List updates = gson.fromJson(gson.toJson(
                gson.fromJson(gson.toJson(
                    entity.get("tripUpdate")), Map.class)
                .get("stopTimeUpdate")), List.class);
            for (Object updateObj : updates) {
                Map update = gson.fromJson(gson.toJson(updateObj), Map.class);
                if (!update.containsKey("arrival")) {
                    continue;
                }

                String stopId = update.get("stopId").toString();
                String arrivalTime = gson.fromJson(gson.toJson(update.get("arrival")), Map.class).get("time").toString();
                int dayId = Integer.parseInt(arrivalTime) / 24; // tripId is only unique within a single day

                if (arrivalTime.compareTo(timestamp) <= 0) { // actual arrival time
                    context.write(
                        new Text(stopId + ";;;" + tripId + ";;;" + dayId),
                        new Text(arrivalTime)
                    );
                } else { // predicted arrival time
                    context.write(
                        new Text(stopId + ";;;" + tripId + ";;;" + dayId),
                        new Text(timestamp + ", " + arrivalTime + ", " + subwayLine)
                    );
                }
            }
        }
    }
}