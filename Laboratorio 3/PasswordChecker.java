import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PasswordChecker {

    public static class PasswordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Map<String, Integer> passwordDatabase = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path databasePath = new Path(conf.get("databasePath"));
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(databasePath)));
            String line;
            while ((line = br.readLine()) != null) {
                passwordDatabase.put(line, 0);
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String password = value.toString();

            try {
                MessageDigest md = MessageDigest.getInstance("SHA-1");
                md.update(password.getBytes());
                byte[] digest = md.digest();
                String hash = bytesToHex(digest);

                if (passwordDatabase.containsKey(hash)) {
                    context.write(new Text(password), new IntWritable(passwordDatabase.get(hash)));
                } else {
                    context.write(new Text(password), new IntWritable(0));
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        private String bytesToHex(byte[] bytes) {
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        }
    }

    public static class PasswordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalCount = 0;
            for (IntWritable count : values) {
                totalCount += count.get();
            }
            context.write(key, new IntWritable(totalCount));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: PasswordChecker <databasePath> <inputPath> <outputPath>");
            System.exit(1);
        }

        String databasePath = args[0];
        String inputPath = args[1];
        String outputPath = args[2];

        Configuration conf = new Configuration();
        conf.set("databasePath", databasePath);

        Job job = Job.getInstance(conf, "Password Checker");
        job.setJarByClass(PasswordChecker.class);
        job.setMapperClass(PasswordMapper.class);
        job.setReducerClass(PasswordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setMaxInputSplitSize(job, 16 * 1024 * 1024); // Tamaño máximo de división del archivo de entrada (16MB)
        FileInputFormat.setMinInputSplitSize(job, 1); // Tamaño mínimo de división del archivo de entrada (1 byte)
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.getConfiguration().set("mapreduce.map.memory.mb", "2048"); // Aumentar memoria asignada al map
        job.getConfiguration().set("mapreduce.reduce.memory.mb", "2048"); // Aumentar memoria asignada al reduce
        job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx1536m"); // Ajustar tamaño del Java Heap del map
        job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx1536m"); // Ajustar tamaño del Java Heap del reduce
        job.getConfiguration().set("mapreduce.map.memory.mb", "4096"); // Aumentar memoria asignada al map
	job.getConfiguration().set("mapreduce.reduce.memory.mb", "4096"); // Aumentar memoria asignada al reduce
	job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m"); // Ajustar tamaño del Java Heap del map
	job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m"); // Ajustar tamaño del Java Heap del reduce

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

