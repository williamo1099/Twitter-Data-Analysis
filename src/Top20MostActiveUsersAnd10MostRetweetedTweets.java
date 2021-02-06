
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONObject;

/**
 * JobMerging - Top10MostRetweetedTweets
 * A class to find 10 most retweeted tweets and 20 most active users in Twitter.
 * 
 * @author William Oktavianus (williamo1099)
 */
public class Top20MostActiveUsersAnd10MostRetweetedTweets {
    
    public static final String MULTIPLE_OUTPUTS_USERS = "users";
    public static final String MULTIPLE_OUTPUTS_RETWEETS = "retweets";
    
    public static class TweetRetweetCounterMapper extends Mapper<Object, Text, Text, LongWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            this.tweetCounterMap(key, value, context);
            this.retweetCounterMap(key, value, context);
        }
        
        private void tweetCounterMap(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject tweetJSON = new JSONObject(value.toString());
            if (tweetJSON.has("user") && !tweetJSON.isNull("user")) {
                JSONObject userJSON = (JSONObject) tweetJSON.get("user");
                
                if (userJSON.has("id_str") && !userJSON.isNull("id_str") && 
                        userJSON.has("name") && !userJSON.isNull("name") &&
                        userJSON.has("location") && !userJSON.isNull("location") &&
                        userJSON.has("verified") && !userJSON.isNull("verified") &&
                        userJSON.has("followers_count") && !userJSON.isNull("followers_count") &&
                        userJSON.has("friends_count") && !userJSON.isNull("friends_count")) {
                    String user = userJSON.getString("id_str") + "," + userJSON.getString("name") + "," +
                                        userJSON.getString("location") + "," + userJSON.getBoolean("verified") + "," +
                                        userJSON.getInt("followers_count") + "," + userJSON.getInt("friends_count");
                    context.write(new Text(user + "=1"), new LongWritable(1));
                }
            }
        }
        
        private void retweetCounterMap(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject tweetJSON = new JSONObject(value.toString());
            if (tweetJSON.has("id_str") && !tweetJSON.isNull("id_str") &&
                    tweetJSON.has("created_at") && !tweetJSON.isNull("created_at") &&
                    tweetJSON.has("text") && !tweetJSON.isNull("text") &&
                    tweetJSON.has("retweet_count") && !tweetJSON.isNull("retweet_count")) {
                String tweet = tweetJSON.getString("id_str") + "," + tweetJSON.getString("created_at") + "," +
                                tweetJSON.getString("text").replace("\n", " ");
                long retweetCount = Long.parseLong(tweetJSON.get("retweet_count").toString());
                context.write(new Text(tweet + "=2"), new LongWritable(retweetCount));
            }
        }
        
    }
    
    public static class TweetRetweetCounterCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
        
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            context.write(new Text(key.toString()), new LongWritable(count));
        }

    }
    
    public static class TweetRetweetCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private MultipleOutputs<Text, LongWritable> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.mos = new MultipleOutputs<>(context);
        }
        
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            
            if (key.toString().split("=")[1].equals("1")) {
                this.mos.write(MULTIPLE_OUTPUTS_USERS, new Text(key.toString().split("=")[0]), new LongWritable(count));
            } else {
                this.mos.write(MULTIPLE_OUTPUTS_RETWEETS, new Text(key.toString().split("=")[0]), new LongWritable(count));
            }
        }

    }
    
    public static void main(String[] args) throws Exception {
        // Set path.
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // Initialize counting job.
        Configuration conf = new Configuration();
        Job countingJob = new Job(conf, "JobMerging - Tweet and Retweet Counter");
        countingJob.setJarByClass(Top20MostActiveUsersAnd10MostRetweetedTweets.class);
        
        // Set mapper and reducer class.
        countingJob.setMapperClass(Top20MostActiveUsersAnd10MostRetweetedTweets.TweetRetweetCounterMapper.class);
        countingJob.setCombinerClass(Top20MostActiveUsersAnd10MostRetweetedTweets.TweetRetweetCounterCombiner.class);
        countingJob.setReducerClass(Top20MostActiveUsersAnd10MostRetweetedTweets.TweetRetweetCounterReducer.class);
        
        // Set output key and value class.
        countingJob.setOutputKeyClass(Text.class);
        countingJob.setOutputValueClass(LongWritable.class);

        // Set input and output path.
        countingJob.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(countingJob, inputPath);
        countingJob.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(countingJob, outputPath);
        
        MultipleOutputs.addNamedOutput(countingJob, MULTIPLE_OUTPUTS_USERS, TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(countingJob, MULTIPLE_OUTPUTS_RETWEETS, TextOutputFormat.class, Text.class, LongWritable.class);

        System.exit(countingJob.waitForCompletion(true) ? 0 : 2);
    }
}
