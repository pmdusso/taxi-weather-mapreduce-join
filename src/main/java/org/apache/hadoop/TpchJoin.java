package org.apache.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;


public class TpchJoin {

    public static final String TAXI = "T";
    public static final String WEATHER = "W";
    public static final String TAXI_TAG = TAXI + "~";
    public static final String WEATHER_TAG = WEATHER + "~";

    /**
     * A WritableComparator optimized for Text keys.
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(Text.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            final int n1 = WritableUtils.decodeVIntSize(b1[s1]);
            final int n2 = WritableUtils.decodeVIntSize(b2[s2]);
            return compareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
        }
    }

    public static class JoinReducer extends Reducer<LongWritable, Text, Text, Text> {

        String record, rainVolume;

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (final Text val : values) {
                String valueSplitted[] = val.toString().split("~");

                if (valueSplitted[0].equals(TAXI)) {
                    record = valueSplitted[1].trim();
                } else if (valueSplitted[0].equals(WEATHER)) {
                    rainVolume = valueSplitted[1].trim();
                }
            }

            // pump final output to file
            if (record != null && rainVolume != null) {
                context.write(new Text(record.toString()), new Text("," + Double.valueOf(rainVolume) * 10));
            }
            /*else if (rainVolume == null) {
                context.write(new Text(record.toString()), new Text("," + 0.0));
            }*/
        }

        public static class TaxiMapper extends Mapper<Object, Text, LongWritable, Text> {

            private String fileTag = TAXI_TAG;

            private final LongWritable pickupDatetime = new LongWritable();
            private final Text record = new Text();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //2013-01-12 01:40:03

            @Override
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] split = value.toString().split(",");
                try {
                    pickupDatetime.set(df.parse(split[5]).getTime());
                } catch (ParseException pe) {
                    System.out.println(pe.getMessage());
                }
                record.set(fileTag + value);
                context.write(pickupDatetime, record);
            }
        }

        public static class WeatherMapper extends Mapper<Object, Text, LongWritable, Text> {

            private String fileTag = WEATHER_TAG;
            private final LongWritable rainDate = new LongWritable();
            private final Text rainVolume = new Text();
            DateFormat df = new SimpleDateFormat("dd-MM-yyyy HH:mm zzz"); //1-6-2013 5:51 GMT,

            @Override
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

                String[] split = value.toString().replace("\"", "").split(",");
                try {
                    rainDate.set(df.parse(split[0]).getTime());
                } catch (ParseException pe) {
                    System.out.println(pe.getMessage());
                }
                if (split.length == 2) {
                    rainVolume.set(fileTag + split[1]);
                } else if (split.length == 3) {
                    rainVolume.set(fileTag + split[1] + "." + split[2]);
                } else
                    rainVolume.set(fileTag + "0");

                context.write(rainDate, rainVolume);
            }
        }

        public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
            PropertyConfigurator.configure("log4j.properties");
            final Configuration conf = new Configuration();
            final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            if (otherArgs.length != 3) {
                System.out.println("Usage: tpchjoin <taxi> <weather> <out>");
                System.exit(2);
            }

            final Job job = new Job(conf, "Taxi Join");
            job.setJarByClass(TpchJoin.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            job.setReducerClass(JoinReducer.class);
            MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TaxiMapper.class);
            MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, WeatherMapper.class);

            final FileSystem hdfs = FileSystem.get(conf);
            if (hdfs.exists(new Path(otherArgs[2]))) {
                otherArgs[2] += "_" + System.currentTimeMillis();
            }
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

            final long start = System.nanoTime();
            final int r = job.waitForCompletion(true) ? 0 : 1;
            final long span = System.nanoTime() - start;
            System.out.println(TimeUnit.SECONDS.convert(span, TimeUnit.NANOSECONDS));
            System.out.println("======================================================");

        }

    }
}