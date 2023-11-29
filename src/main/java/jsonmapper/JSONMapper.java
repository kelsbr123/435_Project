package jsonmapper;
import com.sun.source.tree.Tree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class JSONMapper extends Configured implements Tool {

    public static class JsonMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final int PRIME = 31;

        private static String idConverter(String ascii){
            return Integer.toString(ascii.hashCode() * PRIME);
        }


        public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
            String line = value.toString();
            JSONObject jsonObject = new JSONObject(line);
            String reviewerID = jsonObject.get("reviewerID");
            String asin = idConverter(jsonObject.get("asin"));
            String score = jsonObject.get("overall");
            String name = jsonObject.get("reviewerName");

            context.write(new Text(reviewerID+"/"+name), new Text(asin + ":" + score));
        }
    }

    public static class JSONReducer extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> history = new ArrayList<>();
            for (Text value : values) {
                if(!history.contains(value.toString())) history.add(value.toString());
            }
            if (history.size() > 4) {
                String rows = CSVBuilder.buildRows(key.toString(), history);
                context.write(new Text(rows), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JSONMapper(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "JSON Mapper");
        job.setJarByClass(JSONMapper.class);
        job.setMapperClass(JSONMapper.JsonMapper.class);
        job.setReducerClass(JSONMapper.JSONReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.getConfiguration().set("mapreduce.output.basename", "CSV");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
