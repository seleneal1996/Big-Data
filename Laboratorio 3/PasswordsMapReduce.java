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

public class PasswordsMapReduce {

    public static class PasswordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Map<String, Integer> passwordDatabase = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Cargar la base de datos de contraseñas en una estructura de datos adecuada
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
                // Calcular el hash SHA-1 de la contraseña
                MessageDigest md = MessageDigest.getInstance("SHA-1");
                md.update(password.getBytes());
                byte[] digest = md.digest();
                String hash = bytesToHex(digest);

                // Verificar si el hash de la contraseña está en la base de datos
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
        job.setJarByClass(PasswordsMapReduce.class);
        job.setMapperClass(PasswordMapper.class);
        job.setReducerClass(PasswordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
