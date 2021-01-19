
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.json.JSONObject;

/**
 * Data Organization - TweetPartition
 * A class to partition data into four categories : tweet (0), reply (1), retweet (2) and quote retweet (3).
 *
 * @author williamo1099
 */
public class TweetPartition {

    public static class TweetMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject tweetJSON = new JSONObject(value.toString());
            IntWritable category = new IntWritable();
            if (tweetJSON.has("id") && !tweetJSON.isNull("id") &&
                    tweetJSON.has("text") && !tweetJSON.isNull("text")) {
                String tweetId = tweetJSON.get("id").toString();
                String tweetText = tweetJSON.get("text").toString();
                if (tweetJSON.has("in_reply_to_status_id") && !tweetJSON.isNull("in_reply_to_status_id")) {
                    // is a reply
                    category.set(1);
                } else if (tweetJSON.has("retweeted_status") && !tweetJSON.isNull("retweeted_status")) {
                    if (tweetJSON.has("quoted_status_id") && !tweetJSON.isNull("quoted_status_id")) {
                        // is a quote retweet
                        category.set(3);
                    } else {
                        // is a retweet
                        category.set(2);
                    }
                } else {
                    // is a tweet
                    category.set(0);
                }
                context.write(category, new Text(tweetId + "," + tweetText));
            }
        }

    }

    public static class TweetPartitioner extends Partitioner<IntWritable, Text> implements Configurable {

        private static final String MIN_CATEGORY = "min.category";
        private Configuration conf = null;
        private int minCategory = 0;

        @Override
        public int getPartition(IntWritable key, Text value, int i) {
            return key.get() - this.minCategory;
        }

        @Override
        public Configuration getConf() {
            return this.conf;
        }

        @Override
        public void setConf(Configuration c) {
            this.conf = c;
        }

        public static void setMinCategory(Job job, int minCategory) {
            job.getConfiguration().setInt(MIN_CATEGORY, minCategory);
        }
    }
    
    public static class TweetReducer extends Reducer<IntWritable, Text, NullWritable, NullWritable> {
        
        private static Connection connection;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                DBConfiguration dbConf = new DBConfiguration(context.getConfiguration());
                connection = dbConf.getConnection();
            } catch (Exception ex) { }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String id = value.toString().split(",")[0];
                String text = value.toString().split(",")[1];
                
                String[] columnNames = new String[]{"Id", "text"};
                String tableName;
                int category = key.get();
                if (category == 0) {
                    tableName = "tweet_data";
                } else if (category == 1) {
                    tableName = "reply_data";
                } else if (category == 2) {
                    tableName = "retweet_data";
                } else {
                    tableName = "quote_data";
                }
                
                try {
                    PreparedStatement statement = null;
                    statement = connection.prepareStatement(constructQuery(tableName, columnNames));
                    statement.setString(1, id);
                    statement.setString(2, text);
                    statement.execute();
                } catch (Exception ex) { }
            }
        }
        
        public String constructQuery(String table, String[] fieldNames) {
            if (fieldNames == null) {
                throw new IllegalArgumentException("Field names may not be null");
            }

            StringBuilder query = new StringBuilder();
            query.append("INSERT INTO ").append(table);

            if (fieldNames.length > 0 && fieldNames[0] != null) {
                query.append(" (");
                for (int i = 0; i < fieldNames.length; i++) {
                    query.append(fieldNames[i]);
                    if (i != fieldNames.length - 1) {
                        query.append(",");
                    }
                }
                query.append(")");
            }
            query.append(" VALUES (");

            for (int i = 0; i < fieldNames.length; i++) {
                query.append("?");
                if (i != fieldNames.length - 1) {
                    query.append(",");
                }
            }
            query.append(");");

            return query.toString();
        }
    }

    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf,
            "com.mysql.jdbc.Driver", // driver class
            "jdbc:mysql://127.0.0.1:3306/twitter_analysis", // db url
            "root", // user name
            ""); //password
        Job partitioningJob = Job.getInstance(conf, "Data Organization - Tweet Partition");
        partitioningJob.setJarByClass(TweetPartition.class);
        
        partitioningJob.setMapperClass(TweetMapper.class);
        partitioningJob.setPartitionerClass(TweetPartitioner.class);
        TweetPartitioner.setMinCategory(partitioningJob, 0);
        partitioningJob.setReducerClass(TweetReducer.class);
        partitioningJob.setNumReduceTasks(4);

        partitioningJob.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(partitioningJob, new Path(args[0]));
        
        partitioningJob.setOutputFormatClass(DBOutputFormat.class);
        partitioningJob.setMapOutputKeyClass(IntWritable.class);
        partitioningJob.setMapOutputValueClass(Text.class);
        partitioningJob.setOutputKeyClass(NullWritable.class);
        partitioningJob.setOutputValueClass(NullWritable.class);
        
        boolean success = partitioningJob.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}