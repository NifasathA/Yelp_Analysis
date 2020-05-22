package bigdata.bigdata;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigdata.bigdata.sort_utilities;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class top10_yelpbuss {

    public static class TitanicMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String x[] = value.toString().split("\\^");
            DoubleWritable one = new DoubleWritable(1.0);
            Pattern pattern = Pattern.compile("\\b\\d{5}");
            Matcher matcher = pattern.matcher(x[1]);
            if(matcher.find()){
            	int len = x[1].length();
            	String zipcode = x[1].substring(len-5,len); 
            	zipcode = zipcode.trim();
            	Text word = new Text(zipcode);
                context.write(word, one);
            }
        }
    }

    public static class TitanicReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();
        private Map<String,Double> countMap = new HashMap<String,Double>();
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum = sum + (double)val.get();
            }
            result.set(sum);
            countMap.put(key.toString(), sum);
            //context.write(key, result);
        }
        
        protected void cleanup (Context context) throws IOException, InterruptedException {
          HashMap<String,Double> sortedMap = sort_utilities.sortByValues(countMap);

            int counter = 0;
            for (Map.Entry<String, Double> x: sortedMap.entrySet()) {
            	counter++;
                if (counter == 10) {
                    break;
                }
                context.write(new Text(x.getKey()), new DoubleWritable(x.getValue()));
            }
      }
    }
    
    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");

        job.setJarByClass(average_gender.class);
        job.setMapperClass(TitanicMapper.class);
        job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(DoubleWritable.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(TitanicReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}