package part3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StripeApproach {

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {

		private MapWritable mapW;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {
			mapW = new MapWritable();
			super.setup(context);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] strArray = line.split(" ");
			for (int i = 0; i < strArray.length; i++) {
				for (int j = i + 1; j < strArray.length; j++) {
					if (!strArray[i].equals(strArray[j])) {
						Text k = new Text(strArray[j]);
						if (!mapW.containsKey(key))
							mapW.put(k, new IntWritable(1));
						else {
							IntWritable val = (IntWritable) mapW.get(k);
							mapW.put(k, new IntWritable(val.get() + 1));
						}
							
					} else
						break;
				}
				context.write(new Text(strArray[i]), mapW);
				mapW = new MapWritable();
			}
		}
	}

	public static class Reduce extends Reducer<Text, MapWritable, Text, MapWritable> {

		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			MapWritable sum = new MapWritable();
			for (MapWritable mapW : values) {
				for (Entry<Writable, Writable> entry : mapW.entrySet()) {
					Text k = (Text)entry.getKey();
					IntWritable countInSum = (IntWritable)sum.get(k); 
					IntWritable nCount = (IntWritable) entry.getValue();
					if (sum.containsKey(k)) 
						sum.put(k, new IntWritable(countInSum.get() + nCount.get()));
					else
						sum.put(k, entry.getValue());
				}
			}
			context.write(key, sum);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "crystal");
		job.setJarByClass(StripeApproach.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
