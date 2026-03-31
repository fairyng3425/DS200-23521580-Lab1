import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai3 {

    // 1. MAPPER: Đọc Ratings, ghép Giới tính từ Users
    public static class GenderMapper extends Mapper<Object, Text, IntWritable, Text> {
        private Map<Integer, String> userGenderMap = new HashMap<>();
        private IntWritable movieIdKey = new IntWritable();
        private Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String userPathStr = conf.get("userPath");

            if (userPathStr != null) {
                Path path = new Path(userPathStr);
                FileSystem fs = FileSystem.get(conf);

                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String delimiter = line.contains("::") ? "::" : ",";
                        String[] parts = line.split(delimiter);

                        if (parts.length >= 2) {
                            try {
                                int userId = Integer.parseInt(parts[0].trim());
                                String gender = parts[1].trim();
                                userGenderMap.put(userId, gender);
                            } catch (NumberFormatException e) {}
                        }
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String delimiter = line.contains("::") ? "::" : ",";
            String[] fields = line.split(delimiter);

            if (fields.length >= 3) {
                try {
                    int userId = Integer.parseInt(fields[0].trim());
                    int movieId = Integer.parseInt(fields[1].trim());
                    float rating = Float.parseFloat(fields[2].trim());

                    String gender = userGenderMap.get(userId);
                    if (gender != null) {
                        movieIdKey.set(movieId);
                        outValue.set(rating + "|" + gender);
                        context.write(movieIdKey, outValue);
                    }
                } catch (NumberFormatException e) {}
            }
        }
    }


    // 2. REDUCER: Tính điểm riêng cho Nam/Nữ và ghép Tên Phim
    public static class GenderReducer extends Reducer<IntWritable, Text, Text, Text> {
        private Map<Integer, String> movieMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String moviePathStr = conf.get("moviePath");

            if (moviePathStr != null) {
                Path path = new Path(moviePathStr);
                FileSystem fs = FileSystem.get(conf);

                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String delimiter = line.contains("::") ? "::" : ",";
                        String[] parts = line.split(delimiter, 3);

                        if (parts.length >= 2) {
                            try {
                                int movieId = Integer.parseInt(parts[0].trim());
                                String title = parts[1].trim();
                                movieMap.put(movieId, title);
                            } catch (NumberFormatException e) {}
                        }
                    }
                }
            }
        }

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\\|");
                if (parts.length >= 2) {
                    try {
                        float rating = Float.parseFloat(parts[0]);
                        String gender = parts[1];

                        if (gender.equalsIgnoreCase("M")) {
                            maleSum += rating;
                            maleCount++;
                        } else if (gender.equalsIgnoreCase("F")) {
                            femaleSum += rating;
                            femaleCount++;
                        }
                    } catch (NumberFormatException e) {}
                }
            }

            if (maleCount > 0 || femaleCount > 0) {
                float maleAvg = maleCount == 0 ? 0.0f : maleSum / maleCount;
                float femaleAvg = femaleCount == 0 ? 0.0f : femaleSum / femaleCount;

                String title = movieMap.getOrDefault(key.get(), "Unknown Movie");
                

                String outputStr = String.format(Locale.US, "Male: %.2f, Female: %.2f", maleAvg, femaleAvg);

                context.write(new Text(title), new Text(outputStr));
            }
        }
    }


    // 3. DRIVER: Điều phối hệ thống
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Bai3 <movies.txt_path> <users.txt_path> <ratings_dir> <output_dir>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("moviePath", args[0]);
        conf.set("userPath", args[1]);

        Job job = Job.getInstance(conf, "Bai 3: Ratings By Gender");
        job.setJarByClass(Bai3.class);

        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}