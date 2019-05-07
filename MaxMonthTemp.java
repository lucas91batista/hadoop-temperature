import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxMonthTemp{
	
	public static void main (String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		//The output directory must not exist or the applicatin will fail;
		String programArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (programArgs.length !=2){
			System.err.println("Error: Usage MaxTemp <in> <out>");
			System.exit(2);
		}
		//Note: Combiner runs in Mapper phase, but uses Reducer class (Combiner is not used here)
		Job job = Job.getInstance(conf, "Monthly Max Temp");
		job.setJarByClass(MaxMonthTemp.class);
		job.setMapperClass(MaxTempMapper.class);
		job.setReducerClass(MaxTempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(programArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(programArgs[1]));
		
		//Submit the job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
