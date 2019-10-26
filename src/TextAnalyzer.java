import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {

    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Implementation of you mapper function

            // Map all permutations of word-pairs and write them to 'context'
            String line = value.toString().toLowerCase().replaceAll("[^a-z0-9]", " ");
            String[] words = line.split("\\s");
            for (int i = 0; i < words.length; ++i) {
               String word1 = words[i].trim();
               if (word1.isEmpty()) {
                   continue;
               }
               for (int j = 0; j < words.length; ++j) {
                   if (i != j) {
                       String word2 = words[j].trim();
                       if (word2.isEmpty()) {
                           continue;
                       }
                       context.write(new Text(word1 + " " + word2), new IntWritable(1));
                   }
               }
            }

            // Map all permutations of word-pairs and write them to 'context'
            //  String input = value.toString();
            //  String[] lines = input.split("\\n");
            //  for (int lineNum = 0; lineNum < lines.length; ++lineNum) {
            //      String[] outerResult = lines[lineNum].split("\\s");
            //      for (int i = 0; i < outerResult.length; ++i) {
            //          String word1 = outerResult[i].replaceAll("[^a-zA-Z0-9]", " ").toLowerCase().trim();
            //          if (word1.isEmpty()) {
            //              continue;
            //          }
            //          String[] innerResult = lines[lineNum].split("\\s");
            //          for (int j = 0; j < innerResult.length; ++j) {
            //              if (i != j) {
            //                  String word2 = innerResult[j].replaceAll("[^a-zA-Z0-9]", " ").toLowerCase().trim();
            //                  if (word2.isEmpty()) {
            //                      continue;
            //                  }
            //                  context.write(new Text(word1 + " " + word2), new IntWritable(1));
            //              }
            //          }
            //      }
            //
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    public static class TextCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Implementation of you combiner function
            int sum = 0;
            for (IntWritable val : values) {
               sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<Text, IntWritable, Text, Text> {
        private final static Text emptyText = new Text("");

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Implementation of you reducer function
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new Text(Integer.toString(sum)));

            //  // Write out the results; you may change the following example
            //  // code to fit with your reducer function.
            //  // Write out each edge and its weight
	        //  Text value = new Text();
            //  for(String neighbor: map.keySet()){
            //      String weight = map.get(neighbor).toString();
            //      value.set(" " + neighbor + " " + weight);
            //      context.write(key, value);
            //  }
            // Empty line for ending the current context key
            //  context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Uncomment to split by sentence
        conf.set("textinputformat.record.delimiter", ".");

        // Create job
        Job job =  Job.getInstance(conf, "EID1_cts2458");  //  Deprecated: new Job(conf, "EID1_cts2458"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
	    // set local combiner class
        job.setCombinerClass(TextCombiner.class);

        // set reducer class
	    job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        //job.setMapOutputKeyClass(?.class);
        //job.setMapOutputValueClass(?.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }

    // You may define sub-classes here. Example:
    // public static class MyClass {
    //
    // }
}



