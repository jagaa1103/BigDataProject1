

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PairAndStripeApproach {

	public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {
		public Logger logger = Logger.getLogger("Map.class");
		private final static IntWritable one = new IntWritable(1);
		
		
				
		@Override
		protected void setup(Mapper<LongWritable, Text, Pair, IntWritable>.Context context)
				throws IOException, InterruptedException {
			logger.info("==== Mapper output ====");
			super.setup(context);
		}



		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] strArray = line.split(" ");
			for(int i = 0; i < strArray.length; i++) {
				for(int j = i + 1; j < strArray.length; j++) {
					if(!strArray[i].equals(strArray[j])) {
						Pair p = new Pair(strArray[i], strArray[j]);
						logger.info("(" + p.toString() + ", " + one.toString() + ")");
						context.write(p, one);
						Pair p1 = new Pair(strArray[i], "*");
						logger.info("(" + p1.toString() + ", " + one.toString() + ")");
						context.write(p1, one);
					}
					else break;
				}
			}
		}
		
	}
	
	public static class Reduce extends Reducer<Pair, IntWritable, Text, MapWritable> {
		public Logger logger = Logger.getLogger("Reduce.class");
		HashMap<String, HashMap<String, Double>> mapW;
		double sum; 
		
		@Override
		protected void setup(Reducer<Pair, IntWritable, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {
			mapW = new HashMap<String, HashMap<String, Double>>();
			sum = 0.0;
			logger.info("==== Reducer ouput ====");
			super.setup(context);
		}

		public void reduce(Pair pair, Iterable<IntWritable> values, Context context)		
				throws IOException, InterruptedException {
			if(!mapW.containsKey(pair.key1.toString())) {
				HashMap<String, Double> m = new HashMap<String, Double>();
				mapW.put(pair.key1.toString(), m);
				sum = 0.0;
			}
			int s = sum(values);
			if(pair.key2.toString().equals("*")) {
				sum = s;
			}else {
				HashMap<String, Double> valueMap = mapW.get(pair.key1.toString());
				valueMap.put(pair.key2.toString(), s/sum);
				mapW.put(pair.key1.toString(), valueMap);
			}
		}
		
		@Override
		protected void cleanup(Reducer<Pair, IntWritable, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {
			for(HashMap.Entry<String, HashMap<String, Double>> entry : mapW.entrySet()) {
				Text key = new Text(entry.getKey());
				MapWritable value = new MapWritable();
				for(HashMap.Entry<String, Double> e : entry.getValue().entrySet()) {
					value.put(new Text(e.getKey()), new DoubleWritable(e.getValue()));
				}
				print(key, value);
				context.write(key, value);
			}
			super.cleanup(context);
		}
		
		private int sum(Iterable<IntWritable> values) {
			int sum = 0;
			for (IntWritable val : values)
				sum += val.get();
			return sum;
		}
		
		private void print(Text key, MapWritable map) {
			String res = key.toString();
			for(Entry<Writable, Writable> entry : map.entrySet()) {
				res += ", " + entry.getKey().toString() + " : " + entry.getValue().toString();
			}
			logger.info(res);
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "crystal");
		job.setJarByClass(PairAndStripeApproach.class);

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
