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
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class JSONMapper extends Configured implements Tool {

    public static class JsonMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
            String line = value.toString();
            JSONObject jsonObject = new JSONObject(line);
            String reviewerID = jsonObject.getReviewerID();
            String asin = jsonObject.get("asin");
            String score = jsonObject.get("overall");
            context.write(new Text(reviewerID), new Text(asin + ":" + score));
        }
    }

    public static class JsonReducer extends Reducer<Text, Text, Text, MapWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            MapWritable writable = new MapWritable();
            for (Text value : values) {
                String[] tuple = value.toString().split(":");
                writable.putIfAbsent(new Text(tuple[0]), new Text(tuple[1]));
            }
            if(writable.size() > 2)  context.write(key, writable);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JSONMapper(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Test");
        job.setJarByClass(JSONMapper.class);
        DistributedCache.addFileToClassPath(new Path("~/435_project/src/main/resources/json-20231013.jar"), conf);
        job.setMapperClass(JSONMapper.JsonMapper.class);
        job.setReducerClass(JSONMapper.JsonReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
