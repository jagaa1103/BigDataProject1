import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PairApproach {
	
	public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {
		public Logger logger = Logger.getLogger("Map.class");
		private final static IntWritable one = new IntWritable(1);
				
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			logger.info("==== Mapper output ====");
			String line = value.toString();
			String[] strArray = line.split(" ");
			for(int i = 0; i < strArray.length; i++) {
				for(int j = i + 1; j < strArray.length; j++) {
					if(!strArray[i].equals(strArray[j])) {
						logger.info("(" + strArray[i] + ", " + strArray[j] + "), " + one.toString());
						logger.info("(" + strArray[i] + ", * ), " + one.toString());
						context.write(new Pair(strArray[i], strArray[j]), one);
						context.write(new Pair(strArray[i], "*"), one);
					}
					else break;
				}
			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
		
		
	}

	public static class Reduce extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {
		public Logger logger = Logger.getLogger("Reduce.class");
		private double sum; 
		
		@Override
		protected void setup(Reducer<Pair, IntWritable, Pair, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			sum = 0;
			logger.info("==== Reducer output ====");
			super.setup(context);
		}

		public void reduce(Pair pair, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int s = 0;
			for (IntWritable val : values) {
				s += val.get();
			} 
			if(pair.key2.toString().equals("*")) 
				sum = s;
			else {
				DoubleWritable value = new DoubleWritable(s/sum);
				logger.info("(" + pair.toString() + ", " + value.toString() + ")");
				context.write(pair, value);
			}
				
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "crystal");
		job.setJarByClass(PairApproach.class);

		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
