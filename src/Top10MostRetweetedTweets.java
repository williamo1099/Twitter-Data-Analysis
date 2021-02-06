
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONObject;

/**
 * JobChaining - Top10MostRetweetedTweets
 * A class to find 10 most retweeted tweets.
 *
 * @author William Oktavianus (williamo1099)
 */
public class Top10MostRetweetedTweets {

    public static class RetweetCounterMapper extends Mapper<Object, Text, Text, LongWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject tweetJSON = new JSONObject(value.toString());
            if (tweetJSON.has("id_str") && !tweetJSON.isNull("id_str") &&
                    tweetJSON.has("created_at") && !tweetJSON.isNull("created_at") &&
                    tweetJSON.has("text") && !tweetJSON.isNull("text") &&
                    tweetJSON.has("retweet_count") && !tweetJSON.isNull("retweet_count")) {
                String tweet = tweetJSON.getString("id_str") + "," + tweetJSON.getString("created_at") + "," +
                                tweetJSON.getString("text").replace("\n", " ");
                long retweetCount = Long.parseLong(tweetJSON.get("retweet_count").toString());
                context.write(new Text(tweet), new LongWritable(retweetCount));
            }
        }

    }

    public static class RetweetCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }

    }

    public static class TweetFilterMapper extends Mapper<Object, Text, NullWritable, Text> {

        private TreeMap<Text, Text> map = new TreeMap<>(new Comparator<Text>() {

            @Override
            public int compare(Text o1, Text o2) {
                Integer int1 = new Integer(o1.toString().split(",")[0]);
                Integer int2 = new Integer(o2.toString().split(",")[0]);
                int compare = int1.compareTo(int2);
                if (compare == 0) {
                    return o1.toString().split(",")[1].compareTo(o2.toString().split(",")[1]);
                }
                return compare;
            }
        });

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text mapKey : this.map.keySet()) {
                context.write(NullWritable.get(), this.map.get(mapKey));
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().split("\t").length == 2) {
                String tweet = value.toString().split("\t")[0];
                long retweetCount = Long.parseLong(value.toString().split("\t")[1]);
                this.map.put(new Text(retweetCount + "," + tweet), new Text(value.toString()));
                if (this.map.size() > 10) {
                    this.map.remove(this.map.firstKey());
                }
            }
        }

    }

    public static class TweetFilterReducer extends Reducer<NullWritable, Text, Text, NullWritable> {

        private TreeMap<Text, Text> map = new TreeMap<>(new Comparator<Text>() {

            @Override
            public int compare(Text o1, Text o2) {
                Integer int1 = new Integer(o1.toString().split(",")[0]);
                Integer int2 = new Integer(o2.toString().split(",")[0]);
                int compare = int1.compareTo(int2);
                if (compare == 0) {
                    return o1.toString().split(",")[1].compareTo(o2.toString().split(",")[1]);
                }
                return compare;
            }
        });

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (value.toString().split("\t").length == 2) {
                    String tweet = value.toString().split("\t")[0];
                    long retweetCount = Long.parseLong(value.toString().split("\t")[1]);
                    this.map.put(new Text(retweetCount + "," + tweet), new Text(tweet));
                    if (this.map.size() > 10) {
                        this.map.remove(this.map.firstKey());
                    }
                }
            }
            for (Text mapKey : this.map.descendingKeySet()) {
                context.write(mapKey, NullWritable.get());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        // Set input and output path.
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path intOutputPath = new Path(args[1] + "_int");
        Path outputPath = new Path(args[1]);

        // Initialize counting job.
        Job countingJob = new Job(conf, "JobChaining - Retweet Counter");
        countingJob.setJarByClass(Top10MostRetweetedTweets.class);
        
        // Set mapper and reducer class for counting job.
        countingJob.setMapperClass(Top10MostRetweetedTweets.RetweetCounterMapper.class);
        countingJob.setCombinerClass(Top10MostRetweetedTweets.RetweetCounterReducer.class);
        countingJob.setReducerClass(Top10MostRetweetedTweets.RetweetCounterReducer.class);
        
        // Set output key and value class for counting job.
        countingJob.setOutputKeyClass(Text.class);
        countingJob.setOutputValueClass(LongWritable.class);

        // Set input and output path for counting job.
        countingJob.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(countingJob, inputPath);
        countingJob.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(countingJob, intOutputPath);

        int code = 1;
        if (countingJob.waitForCompletion(true)) {
            // Initialize filtering job.
            Job filteringJob = new Job(conf, "JobChaining - Tweet Filter");
            filteringJob.setJarByClass(Top10MostRetweetedTweets.class);
            
            // Set mapper and reducer class for filtering job.
            filteringJob.setMapperClass(Top10MostRetweetedTweets.TweetFilterMapper.class);
            filteringJob.setReducerClass(Top10MostRetweetedTweets.TweetFilterReducer.class);
            filteringJob.setNumReduceTasks(1);
            
            // Set output key and value class for filtering job.
            filteringJob.setMapOutputKeyClass(NullWritable.class);
            filteringJob.setMapOutputValueClass(Text.class);
            filteringJob.setOutputKeyClass(Text.class);
            filteringJob.setOutputValueClass(NullWritable.class);

            // Set input and output path for filtering job.
            filteringJob.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(filteringJob, intOutputPath);
            filteringJob.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(filteringJob, outputPath);

            code = filteringJob.waitForCompletion(true) ? 0 : 1;
        }

        FileSystem.get(conf).delete(intOutputPath, true);
        System.out.println("All Done");
        System.exit(code);
    }
}
