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

public class Bai4 {

    // Hàm phân loại nhóm tuổi
    private static String getAgeGroup(int age) {
        if (age <= 18) return "0-18";
        else if (age <= 35) return "18-35";
        else if (age <= 50) return "35-50";
        else return "50+";
    }

    // 1. MAPPER: Đọc Ratings, ghép Nhóm tuổi từ Users
    public static class AgeMapper extends Mapper<Object, Text, IntWritable, Text> {
        private Map<Integer, String> userAgeGroupMap = new HashMap<>();
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

                        if (parts.length >= 3) {
                            try {
                                int userId = Integer.parseInt(parts[0].trim());
                                int age = Integer.parseInt(parts[2].trim()); 
                                String ageGroup = getAgeGroup(age);
                                userAgeGroupMap.put(userId, ageGroup);
                            } catch (NumberFormatException e) {

                            }
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


                    String ageGroup = userAgeGroupMap.get(userId);
                    if (ageGroup != null) {
                        movieIdKey.set(movieId);

                        outValue.set(ageGroup + ":" + rating);
                        context.write(movieIdKey, outValue);
                    }
                } catch (NumberFormatException e) {
                }
            }
        }
    }


    // 2. REDUCER: Tính điểm riêng cho từng Nhóm tuổi và ghép Tên Phim
    public static class AgeReducer extends Reducer<IntWritable, Text, Text, Text> {
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
            float sum0 = 0, sum1 = 0, sum2 = 0, sum3 = 0; // Tương ứng: 0-18, 18-35, 35-50, 50+
            int count0 = 0, count1 = 0, count2 = 0, count3 = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length >= 2) {
                    try {
                        String ageGroup = parts[0];
                        float rating = Float.parseFloat(parts[1]);

                        switch (ageGroup) {
                            case "0-18": sum0 += rating; count0++; break;
                            case "18-35": sum1 += rating; count1++; break;
                            case "35-50": sum2 += rating; count2++; break;
                            case "50+": sum3 += rating; count3++; break;
                        }
                    } catch (NumberFormatException e) {}
                }
            }

            // Tính trung bình
            String res0 = count0 > 0 ? String.format(Locale.US, "%.2f", sum0 / count0) : "NA";
            String res1 = count1 > 0 ? String.format(Locale.US, "%.2f", sum1 / count1) : "NA";
            String res2 = count2 > 0 ? String.format(Locale.US, "%.2f", sum2 / count2) : "NA";
            String res3 = count3 > 0 ? String.format(Locale.US, "%.2f", sum3 / count3) : "NA";

            // Lấy tên phim
            String title = movieMap.getOrDefault(key.get(), "Unknown Movie");

            String outputStr = String.format("0-18: %s\t18-35: %s\t35-50: %s\t50+: %s", res0, res1, res2, res3);

            context.write(new Text(title), new Text(outputStr));
        }
    }


    // 3. DRIVER: Điều phối hệ thống
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Bai4 <movies.txt_path> <users.txt_path> <ratings_dir> <output_dir>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("moviePath", args[0]);
        conf.set("userPath", args[1]);

        Job job = Job.getInstance(conf, "Bai 4: Ratings By Age Group");
        job.setJarByClass(Bai4.class);

        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AgeReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}