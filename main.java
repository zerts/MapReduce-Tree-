package ru.mipt.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by instructor on 06.11.16.
 */
public class WordCount extends Configured implements Tool{

    /* FIRST MAPPER*/

    public static class FirstMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private IntWritable firstVertex = new IntWritable();
        private IntWritable secondVertex = new IntWritable();

        public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] lines =  line.toString().split("\\s+");
            firstVertex.set(Integer.parseInt(lines[0]));
            Integer second = Integer.parseInt(lines[1]);
            if (!second.equals(0)) {
                secondVertex.set(second);
                context.write(firstVertex, secondVertex);
            }
        }
    }

    public static class FirstReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        private IntWritable count = new IntWritable();

        @Override
        public void reduce(IntWritable word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                IntWritable sum = new IntWritable();
                sum.set(value.get());
                context.write(word, sum);
            }
        }
    }



    /* POW MAPPER */

    public static class PowMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private IntWritable firstVertex = new IntWritable();
        private IntWritable secondVertex = new IntWritable();

        public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] lines =  line.toString().split("\\s+");
            firstVertex.set(Integer.parseInt(lines[0]));
            secondVertex.set(Integer.parseInt(lines[1]));
            context.write(firstVertex, secondVertex);
            firstVertex.set((-1) * Integer.parseInt(lines[0]));
            context.write(secondVertex, firstVertex);
        }
    }

    public static class PowReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        private IntWritable count = new IntWritable();

        @Override
        public void reduce(IntWritable word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> in = new ArrayList<>();
            List<Integer> out = new ArrayList<>();
            for (IntWritable value : values) {
                Integer newItem = value.get();
                if (newItem > 0) {
                    in.add(newItem);
                } else {
                    out.add((-1) * newItem);
                }
            }
            for (Integer inner : in) {
                for (Integer outer : out) {
                    IntWritable first = new IntWritable();
                    IntWritable second = new IntWritable();
                    first.set(outer);
                    second.set(inner);
                    context.write(first, second);
                }
            }
        }
    }


    /* PREFOLD MAPPER */

    public static class PrefoldMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private IntWritable firstVertex = new IntWritable();
        private IntWritable secondVertex = new IntWritable();

        public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] lines =  line.toString().split("\\s+");
            firstVertex.set(Integer.parseInt(lines[0]));
            secondVertex.set(Integer.parseInt(lines[1]));
            context.write(firstVertex, secondVertex);
        }
    }

    public static class PrefoldReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        private IntWritable count = new IntWritable();

        Integer currLength;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            currLength = Integer.parseInt(conf.get("currLength"));
        }

        @Override
        public void reduce(IntWritable word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable dist = new IntWritable((-1) * currLength);
            for (IntWritable value : values) {
                IntWritable second = new IntWritable();
                second.set(value.get());
                context.write(second, dist);
            }
        }
    }


    /* FOLD MAPPER*/

    public static class FoldMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private IntWritable firstVertex = new IntWritable();
        private IntWritable secondVertex = new IntWritable();

        public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] lines =  line.toString().split("\\s+");
            firstVertex.set(Integer.parseInt(lines[0]));
            secondVertex.set(Integer.parseInt(lines[1]));
            context.write(firstVertex, secondVertex);
        }
    }

    public static class FoldReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

        private Integer currLength, countedLength = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            currLength = Integer.parseInt(conf.get("currLength"));
        }

        private IntWritable count = new IntWritable();

        @Override
        public void reduce(IntWritable word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            countedLength = 0;
            Boolean isMinusOne = false;
            Boolean isNotMinusOne = false;
            //Integer nextVertex = word.get();
            List<Integer> nextVertexes = new ArrayList<>();
            //nextVertexes.add(word.get());
            for (IntWritable value : values) {
                Integer currValue = value.get();
                if (currValue < countedLength) {
                    isMinusOne = true;
                    countedLength = currValue;
                }
                if (currValue > 0) {
                    isNotMinusOne = true;
                    nextVertexes.add(currValue);
                }
            }
            if (isMinusOne) {
                if (isNotMinusOne) {
                    IntWritable newCountedLength = new IntWritable(countedLength - currLength);
                    for (Integer nextVertex : nextVertexes) {
                        IntWritable next = new IntWritable(nextVertex);
                        context.write(next, newCountedLength);
                    }
                }
                else {
                    IntWritable newCountedLength = new IntWritable(countedLength);
                    context.write(word, newCountedLength);
                }
            }
            else {
                IntWritable newCountedLength = new IntWritable((-1) * currLength);
                for (Integer nextVertex : nextVertexes) {
                    IntWritable next = new IntWritable(nextVertex);
                    context.write(next, newCountedLength);
                }
            }
        }
    }

    private static void deleteFolder(FileSystem fs, Path... paths) throws IOException {
        for (Path path: paths) {
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
        }
    }


    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = new Configuration();
        System.out.println(String.valueOf(conf));

        System.out.println("Hello\n");
        Integer numberOfVertex = Integer.parseInt(strings[1]);
        Path outputPath = new Path("output_hdfs_folder/pow/1");

        Integer currLength = 1;

        // настройка Job'ы
        //FileSystem fs = Fi1leSystem.get(conf);

        Job job1 = Job.getInstance();
        job1.setJarByClass(WordCount.class);

        job1.setMapperClass(FirstMapper.class);
        job1.setReducerClass(FirstReducer.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setNumReduceTasks(8); // по умолчанию задаётся 1 reducer

        TextInputFormat.addInputPath(job1, new Path(strings[0]));
        //deleteFolder(fs, outputPath);
        TextOutputFormat.setOutputPath(job1, outputPath);

        if (job1.waitForCompletion(true)) {

            while(2 * currLength < numberOfVertex) {

                outputPath = new Path("output_hdfs_folder/pow/" + String.valueOf(2 * currLength));

                Job jobPow = Job.getInstance();
                jobPow.setJarByClass(WordCount.class);

                jobPow.setMapperClass(PowMapper.class);
                jobPow.setReducerClass(PowReducer.class);

                jobPow.setOutputKeyClass(IntWritable.class);
                jobPow.setOutputValueClass(IntWritable.class);

                jobPow.setInputFormatClass(TextInputFormat.class);
                jobPow.setOutputFormatClass(TextOutputFormat.class);

                jobPow.setMapOutputKeyClass(IntWritable.class);
                jobPow.setMapOutputValueClass(IntWritable.class);

                jobPow.setNumReduceTasks(8); // по умолчанию задаётся 1 reducer

                TextInputFormat.addInputPath(jobPow, new Path("output_hdfs_folder/pow/" + String.valueOf(currLength)));
                //deleteFolder(fs, outputPath);
                TextOutputFormat.setOutputPath(jobPow, outputPath);

                currLength *= 2;
                System.out.println("output_hdfs_folder/pow/" + String.valueOf(currLength));

                if (!jobPow.waitForCompletion(true)) {
                    return 0;
                }
            }

            outputPath = new Path("output_hdfs_folder/fold/" + String.valueOf(currLength));

            System.out.println("conf of conf" + String.valueOf(currLength));

            conf.set("currLength", String.valueOf(currLength));

            System.out.println("preconf");
            Job preFoldJob = Job.getInstance(conf);
            System.out.println("configured");
            preFoldJob.setJarByClass(WordCount.class);

            preFoldJob.setMapperClass(PrefoldMapper.class);
            preFoldJob.setReducerClass(PrefoldReducer.class);

            preFoldJob.setOutputKeyClass(IntWritable.class);
            preFoldJob.setOutputValueClass(IntWritable.class);

            preFoldJob.setInputFormatClass(TextInputFormat.class);
            preFoldJob.setOutputFormatClass(TextOutputFormat.class);

            preFoldJob.setMapOutputKeyClass(IntWritable.class);
            preFoldJob.setMapOutputValueClass(IntWritable.class);

            preFoldJob.setNumReduceTasks(8); // по умолчанию задаётся 1 reducer

            TextInputFormat.addInputPath(preFoldJob, new Path("output_hdfs_folder/pow/" + String.valueOf(currLength)));
            //deleteFolder(fs, outputPath);
            TextOutputFormat.setOutputPath(preFoldJob, outputPath);

            if (!preFoldJob.waitForCompletion(true)) {
                return 0;
            }


            while (currLength > 1) {

                currLength /= 2;
                conf.set("currLength", String.valueOf(currLength));
                outputPath = new Path("output_hdfs_folder/fold/" + String.valueOf(currLength));

                Job foldJob = Job.getInstance(conf);
                foldJob.setJarByClass(WordCount.class);

                foldJob.setMapperClass(FoldMapper.class);
                foldJob.setReducerClass(FoldReducer.class);

                foldJob.setOutputKeyClass(IntWritable.class);
                foldJob.setOutputValueClass(IntWritable.class);

                foldJob.setInputFormatClass(TextInputFormat.class);
                foldJob.setOutputFormatClass(TextOutputFormat.class);

                foldJob.setMapOutputKeyClass(IntWritable.class);
                foldJob.setMapOutputValueClass(IntWritable.class);

                foldJob.setNumReduceTasks(8); // по умолчанию задаётся 1 reducer

                TextInputFormat.addInputPath(foldJob, new Path("output_hdfs_folder/fold/" + String.valueOf(currLength * 2)));
                TextInputFormat.addInputPath(foldJob, new Path("output_hdfs_folder/pow/" + String.valueOf(currLength)));
                //deleteFolder(fs, outputPath);
                TextOutputFormat.setOutputPath(foldJob, outputPath);

                if (!foldJob.waitForCompletion(true)) {
                    return 0;
                }
            }
            return 1;

        } else {
            return 0;
        }


    }


    public static void main(String[] args) throws Exception {
        new WordCount().run(args);
    }
}
