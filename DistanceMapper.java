1.3 Average Trip Distance by Passenger Count - MapReduce Program

This program computes the average trip distance for each passenger count.

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class DistanceMapper extends MapperObject, Text, IntWritable, DoubleWritable {

    private IntWritable passengerCount = new IntWritable();
    private DoubleWritable tripDistance = new DoubleWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         Assuming CSV format
        String[] fields = value.toString().split(,);
        
        try {
            int passengers = Integer.parseInt(fields[3]);  Passenger count
            double distance = Double.parseDouble(fields[4]);  Trip distance
            
            passengerCount.set(passengers);
            tripDistance.set(distance);
            context.write(passengerCount, tripDistance);
        } catch (Exception e) {
             Handle errors in the data
        }
    }
}



