import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();

		/*File contains temperature information for Sumner County Tennessee for the year 2010.

		Date - Position 18 for 8 
		Year - Position 18 for 4.
		Month - Position 22 for 2.
		Day - Position 24 for 2.

		Daily Average Temp - Position 95 for 3.  Values need to be multiplied by 10, ie  456 is 45.6 degrees.*/
		
		String month = line.substring(22,24);
		int avgTemp;
		avgTemp = Integer.parseInt(line.substring(95,98));
		context.write(new Text(month), new IntWritable(avgTemp));
	}
}
