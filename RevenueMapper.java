1.1 Total Revenue Per Year - MapReduce Program

This program calculates the total revenue per year by extracting the year from the pickup date and summing the fare amounts.



import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RevenueMapper extends MapperObject, Text, Text, DoubleWritable {
    
    private Text year = new Text();
    private DoubleWritable fareAmount = new DoubleWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         Assuming CSV format TripID,PickupDateTime,DropoffDateTime,PassengerCount,TripDistance,PickupLocationID,DropoffLocationID,FareAmount
        String[] fields = value.toString().split(,);
        
        try {
            String pickupDate = fields[1];
            double fare = Double.parseDouble(fields[7]);
             Extract year from the pickup date (assuming format YYYY-MM-DD HHMMSS)
            String yearStr = pickupDate.split(-)[0];
            year.set(yearStr);
            fareAmount.set(fare);
            context.write(year, fareAmount);
        } catch (Exception e) {
             Handle any errors in the data (e.g., missing or malformed values)
        }
    }
}
