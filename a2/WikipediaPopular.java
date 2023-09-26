import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import java.util.regex.Pattern;

public class WikipediaPopular extends Configured implements Tool {

	public static class WikipediaPopularMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		private final LongWritable result = new LongWritable();
		private Text word = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			String [] s = line.split(" "); 
			String date_time = s[0];
			String language = s[1];
			String title = s[2];
			long views = Long.parseLong(s[3]);

			if (language.toLowerCase().equals("en") && (!title.equals("Main_Page")) && (!title.startsWith("Special:"))){
					result.set(views);
					word.set(date_time);
					context.write(word,result);
			}

		}
	}

	public static class WikipediaPopularReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long most_views = 0;
			for(LongWritable val : values){
				
				if(val.get() > most_views)
					most_views = val.get();
				
			}
			result.set(most_views);
			context.write(key, result);
		}
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "WikipediaPopular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(WikipediaPopularMapper.class);
		job.setCombinerClass(WikipediaPopularReducer.class);
		job.setReducerClass(WikipediaPopularReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
