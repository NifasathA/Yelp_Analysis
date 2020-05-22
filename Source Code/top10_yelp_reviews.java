package bigdata.bigdata;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigdata.bigdata.sort_utilities;


public class top10_yelp_reviews {

    public static class TitanicMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String x[] = value.toString().split("\\^");
            DoubleWritable one = new DoubleWritable(1.0);
            Text word; 
            if( x[2].equals("") || x[2].equals(" ") || x[2] == null || x[2].equals("?") || x[2].length()<1){
            	word = new Text("null");
            }
            else{
                word = new Text(x[2]);
            }
            if( x[3].equals("") || x[3].equals(" ") || x[3] == null || x[3].equals("?") || x[3].length()<1){
            	one = new DoubleWritable(0.0);
            }
            else{
                one = new DoubleWritable(Double.parseDouble(x[3]));
            }
                context.write(word, one);
        }
    }

    public static class TitanicReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    	 private DoubleWritable result = new DoubleWritable();
         private Map<String,Double> countMap = new HashMap<String,Double>();
         public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                 throws IOException, InterruptedException {
             double sum = 0;
             double average = 0;
             int count = 0;
             for (DoubleWritable val : values) {
                 sum = sum + (double)val.get();
                 count++;
             }
             average = sum / count;
             result.set(average);
             countMap.put(key.toString(), average);
            //context.write(key, result);
         }
        
        protected void cleanup (Context context) throws IOException, InterruptedException {
          HashMap<String,Double> sortedMap = sort_utilities.sortByValuesDesc(countMap);

            int counter = 0;
            for (Map.Entry<String, Double> x: sortedMap.entrySet()) {
            	counter++;
                if (counter == 100) {
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