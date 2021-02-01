
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONObject;

/**
 * JobChaining - Top20MostActiveUsers
 * A class to find 20 most active users in Twitter, based on how many times a user tweets.
 *
 * @author William Oktavianus (williamo1099)
 */
public class Top20MostActiveUsers {

    public static class TweetCounterMapper extends Mapper<Object, Text, Text, LongWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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
                    context.write(new Text(user), new LongWritable(1));
                }
            }
        }

    }

    public static class TweetCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

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
                String userId = value.toString().split("\t")[0];
                long tweetCount = Long.parseLong(value.toString().split("\t")[1]);
                this.map.put(new Text(tweetCount + "," + userId), new Text(value.toString()));
                if (this.map.size() > 20) {
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
                    String userId = value.toString().split("\t")[0];
                    long tweetCount = Long.parseLong(value.toString().split("\t")[1]);
                    this.map.put(new Text(tweetCount + "," + userId), new Text(userId));
                    if (this.map.size() > 20) {
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
        // set path
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path intOutputPath = new Path(args[1] + "_int");
        Path outputPath = new Path(args[1]);

        Job countingJob = new Job(conf, "JobChaining - Tweet Counter");
        countingJob.setJarByClass(Top20MostActiveUsers.class);
        // set mapper and reducer class
        countingJob.setMapperClass(Top20MostActiveUsers.TweetCounterMapper.class);
        countingJob.setCombinerClass(Top20MostActiveUsers.TweetCounterReducer.class);
        countingJob.setReducerClass(Top20MostActiveUsers.TweetCounterReducer.class);
        // set output key and value class
        countingJob.setOutputKeyClass(Text.class);
        countingJob.setOutputValueClass(LongWritable.class);

        countingJob.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(countingJob, inputPath);
        countingJob.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(countingJob, intOutputPath);

        int code = 1;
        if (countingJob.waitForCompletion(true)) {
            Job filteringJob = new Job(conf, "JobChaining - Tweet Filter");
            filteringJob.setJarByClass(Top20MostActiveUsers.class);
            // set mapper and reducer class
            filteringJob.setMapperClass(Top20MostActiveUsers.TweetFilterMapper.class);
            filteringJob.setReducerClass(Top20MostActiveUsers.TweetFilterReducer.class);
            filteringJob.setNumReduceTasks(1);
            // set output key and value class
            filteringJob.setMapOutputKeyClass(NullWritable.class);
            filteringJob.setMapOutputValueClass(Text.class);
            filteringJob.setOutputKeyClass(Text.class);
            filteringJob.setOutputValueClass(NullWritable.class);

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