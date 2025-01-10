#Reducer Code

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LocationReducer extends ReducerText, IntWritable, Text, IntWritable {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, IterableIntWritable values, Context context) throws IOException, InterruptedException {
        int totalTrips = 0;
        for (IntWritable val  values) {
            totalTrips += val.get();
        }
        result.set(totalTrips);
        context.write(key, result);
    }
}