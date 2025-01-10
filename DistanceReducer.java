#Reducer Code

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DistanceReducer extends ReducerIntWritable, DoubleWritable, IntWritable, DoubleWritable {

    private DoubleWritable result = new DoubleWritable();

    @Override
    public void reduce(IntWritable key, IterableDoubleWritable values, Context context) throws IOException, InterruptedException {
        double totalDistance = 0.0;
        int count = 0;
        
        for (DoubleWritable val  values) {
            totalDistance += val.get();
            count++;
        }
        
        result.set(totalDistance  count);  Calculate average distance
        context.write(key, result);
    }
}