
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONObject;

/**
 * JobMerging - AverageTweetLength
 * A class to find average tweet length in one hour and one day per month.
 *
 * @author William Oktavianus (williamo1099)
 */
public class AverageTweetLength {

    public static final String MULTIPLE_OUTPUTS_HOUR = "hour";
    public static final String MULTIPLE_OUTPUTS_DAY = "day";
    
    public static class AverageTweetCounterMapper extends Mapper<Object, Text, Text, AverageTuple> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject tweetJSON = new JSONObject(value.toString());
            if (tweetJSON.has("created_at") && !tweetJSON.isNull("created_at") &&
                    tweetJSON.has("text") && !tweetJSON.isNull("text")) {
                double tweetLength = tweetJSON.get("text").toString().length();
                SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
                try {
                    Date date = dateFormat.parse(tweetJSON.get("created_at").toString());
                    String[] month = new String[]{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
                    String[] day = new String[]{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
                    this.averageHourMapper(new Text(month[date.getMonth()] + " - " + date.getHours() + "=0"), tweetLength, context);
                    this.averageDayMapper(new Text(month[date.getMonth()] + " - " + day[date.getDay()] + "=1"), tweetLength, context);
                } catch (ParseException ex) {}
            }
        }
        
        private void averageHourMapper(Text key, double value, Context context) throws IOException, InterruptedException {
            AverageTuple average = new AverageTuple();
            average.setAverage(value);
            average.setCount(1);
            context.write(key, average);
        }
        
        private void averageDayMapper(Text key, double value, Context context) throws IOException, InterruptedException {
            AverageTuple average = new AverageTuple();
            average.setAverage(value);
            average.setCount(1);
            context.write(key, average);
        }
        
    }
    
    public static class AverageTweetCounterCombiner extends Reducer<Text, AverageTuple, Text, AverageTuple> {
        
        @Override
        protected void reduce(Text key, Iterable<AverageTuple> values, Context context) throws IOException, InterruptedException {
            AverageTuple average = new AverageTuple();
            double sum = 0;
            double count = 0;
            for (AverageTuple value : values) {
                sum += value.getAverage() * value.getCount();
                count += value.getCount();
            }
            average.setAverage(sum / count);
            average.setCount(count);
            context.write(new Text(key.toString()), average);
        }
        
    }
    
    public static class AverageTweetCounterReducer extends Reducer<Text, AverageTuple, Text, AverageTuple> {
        
        private MultipleOutputs<Text, AverageTuple> mos;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.mos = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<AverageTuple> values, Context context) throws IOException, InterruptedException {
            AverageTuple average = new AverageTuple();
            double sum = 0;
            double count = 0;
            for (AverageTuple value : values) {
                sum += value.getAverage() * value.getCount();
                count += value.getCount();
            }
            average.setAverage(sum / count);
            average.setCount(count);
            
            if (key.toString().split("=")[1].equals("0")) {
                this.mos.write(MULTIPLE_OUTPUTS_HOUR, new Text(key.toString().split("=")[0]), average, MULTIPLE_OUTPUTS_HOUR + "/part");
            } else {
                this.mos.write(MULTIPLE_OUTPUTS_DAY, new Text(key.toString().split("=")[0]), average, MULTIPLE_OUTPUTS_DAY + "/part");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.mos.close();
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        // Initialize counting job.
        Configuration conf = new Configuration();
        Job countingJob = new Job(conf, "JobMerging - Average Tweet Length");
        countingJob.setJarByClass(AverageTweetLength.class);
        
        // Set mapper and reducer class.
        countingJob.setMapperClass(AverageTweetLength.AverageTweetCounterMapper.class);
        countingJob.setCombinerClass(AverageTweetLength.AverageTweetCounterCombiner.class);
        countingJob.setReducerClass(AverageTweetLength.AverageTweetCounterReducer.class);
        
        // Set output key and value class.
        countingJob.setOutputKeyClass(Text.class);
        countingJob.setOutputValueClass(AverageTuple.class);

        // Set input and output path.
        countingJob.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(countingJob, new Path(args[0]));
        countingJob.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(countingJob, new Path(args[1]));
        
        MultipleOutputs.addNamedOutput(countingJob, MULTIPLE_OUTPUTS_HOUR, TextOutputFormat.class, Text.class, AverageTuple.class);
        MultipleOutputs.addNamedOutput(countingJob, MULTIPLE_OUTPUTS_DAY, TextOutputFormat.class, Text.class, AverageTuple.class);

        System.exit(countingJob.waitForCompletion(true) ? 0 : 2);
    }

}

class AverageTuple implements Writable {

    private double count = 0;
    private double average;

    public void readFields(DataInput in) throws IOException {
        this.count = in.readDouble();
        this.average = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.count);
        out.writeDouble(this.average);
    }

    public double getCount() {
        return this.count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    public double getAverage() {
        return this.average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    @Override
    public String toString() {
        return this.average + "";
    }
    
}