1.2 Top 10 PickupDrop-off Locations - MapReduce Program

This job identifies the most popular pickup and drop-off locations.


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LocationMapper extends MapperObject, Text, Text, IntWritable {

    private Text location = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         Assuming CSV format
        String[] fields = value.toString().split(,);
        
        try {
            String pickupLocation = fields[5];  Pickup location ID
            String dropoffLocation = fields[6];  Dropoff location ID

             Emit both pickup and dropoff locations
            location.set(pickupLocation);
            context.write(location, one);
            
            location.set(dropoffLocation);
            context.write(location, one);
        } catch (Exception e) {
             Handle any errors in the data
        }
    }
}
