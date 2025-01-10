#Reducer Code
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RevenueReducer extends ReducerText, DoubleWritable, Text, DoubleWritable {
    
    private DoubleWritable result = new DoubleWritable();
    
    @Override
    public void reduce(Text key, IterableDoubleWritable values, Context context) throws IOException, InterruptedException {
        double totalRevenue = 0.0;
        for (DoubleWritable val  values) {
            totalRevenue += val.get();
        }
        result.set(totalRevenue);
        context.write(key, result);
    }
}