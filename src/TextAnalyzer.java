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
import java.text.BreakIterator;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Locale;
import java.util.*;

import java.io.DataInput;
import java.io.DataOutput;

// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {

    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, Edge> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Implementation of you mapper function
            //  BreakIterator iterator = BreakIterator.getSentenceInstance(Locale.US);
            //  String source = value.toString();
            //  iterator.setText(source);
            //  int start = iterator.first();
            //  for (int end = iterator.next(); end != BreakIterator.DONE; start = end, end = iterator.next()) {
                // Map all permutations of word-pairs and write them to 'context'
                //  String sentence = source.substring(start,end).toLowerCase().replaceAll("[^a-z0-9]", " ");

            List<String> sentences = Arrays.asList(value.toString().split("\\n"));
            for(String sentence : sentences) {
                sentence = sentence.toLowerCase().replaceAll("[^a-z0-9]", " ");
                Set<String> items = new HashSet<String>(Arrays.asList(sentence.split("\\s+")));
                for (String entry : items) {
                    String word1 = entry.trim();
                    if (word1.isEmpty()) {
                        continue;
                    }
                    for (String entry2 : items) {
                        String word2 = entry2.trim();
                        if (word2.isEmpty()) {
                            continue;
                        }
                        context.write(new Text(word1), new Edge(word2, 1));
                    }
                 }
            }
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    public static class TextCombiner extends Reducer<Text, Edge, Text, Edge> {
        public void reduce(Text vertex, Iterable<Edge> edges, Context context) throws IOException, InterruptedException {
            // Implementation of you combiner function
            HashMap<String, Integer> map = new HashMap<>();
            for (Edge edge : edges) {
                int weight = edge.weight.get();
                if (map.containsKey(edge.dest.toString())) {
                    weight += (int)map.get(edge.dest.toString());
                }
                map.put(edge.dest.toString(), weight);
            }

            for (String neighbor : map.keySet()) {
                context.write(new Text(vertex), new Edge(neighbor, (int)map.get(neighbor)));
            }
        }
    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<Text, Edge, Text, Text> {
        private final static Text emptyText = new Text("");

        public void reduce(Text vertex, Iterable<Edge> edges, Context context) throws IOException, InterruptedException {
            // Implementation of you reducer function
            HashMap<String, Integer> map = new HashMap<>();
            for (Edge edge : edges) {
                int weight = edge.weight.get();
                if (map.containsKey(edge.dest.toString())) {
                    weight += map.get(edge.dest.toString());
                }
                map.put(edge.dest.toString(), weight);
            }

            for (String neighbor : map.keySet()) {
                context.write(new Text(vertex + " " + neighbor), new Text(map.get(neighbor).toString()));
            }
            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Uncomment to split by sentence
        //  conf.set("textinputformat.record.delimiter", ".");

        // Create job
        Job job =  Job.getInstance(conf, "EID1_cts2458");  //  Deprecated: new Job(conf, "EID1_cts2458"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Edge.class);

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
    //  public static class Edge {
    //      String dest;
    //      int weight;

    //      Edge(String dest, int weight) {
    //          this.dest = dest;
    //          this.weight = weight;
    //      }
    //  }

    public static class Edge implements Writable {
        Text dest;
        IntWritable weight;
    
        public Edge() {
            this.dest = new Text();
            this.weight = new IntWritable(0);
        }
    
        public Edge(Text dest, IntWritable weight) {
            this.dest = dest;
            this.weight = weight;
        }
        
        public Edge(String dest, Integer weight) {
            this.dest = new Text(dest);
            this.weight = new IntWritable(weight);
        }
    
        public Text getDest() {
            return this.dest;
        }
    
        public IntWritable getWeight() {
            return this.weight;
        }
    
        public void setDest(Text dest) {
            this.dest = dest;
        }
    
        public void setCount(IntWritable weight) {
            this.weight = weight;
        }
    
        public void readFields(DataInput in) throws IOException {
            this.dest.readFields(in);
            this.weight.readFields(in);
        }
    
        public void write(DataOutput out) throws IOException {
            this.dest.write(out);
            this.weight.write(out);
        }
    
        @Override
        public String toString() {
            return this.dest.toString() + "\t" + this.weight.toString();
        }
    }

}



