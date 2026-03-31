import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2 {

    // 1. MAPPER: Đọc Rating và nhân bản cho từng Thể loại
    public static class GenreMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Map<Integer, String[]> movieGenresMap = new HashMap<>();
        private Text genreKey = new Text();
        private FloatWritable ratingValue = new FloatWritable();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String moviePath = conf.get("moviePath");

            if (moviePath != null) {
                Path path = new Path(moviePath);
                FileSystem fs = FileSystem.get(conf);

                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String delimiter = line.contains("::") ? "::" : ",";
                        String[] parts = line.split(delimiter, 3);

                        if (parts.length >= 3) {
                            try {
                                int movieId = Integer.parseInt(parts[0].trim());
                                // Tách các thể loại bằng dấu |
                                String[] genres = parts[2].trim().split("\\|");
                                movieGenresMap.put(movieId, genres);
                            } catch (NumberFormatException e) {
                                // Bỏ qua nếu gặp dòng tiêu đề chữ
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
                    int movieId = Integer.parseInt(fields[1].trim());
                    float rating = Float.parseFloat(fields[2].trim());

                    String[] genres = movieGenresMap.get(movieId);
                    if (genres != null) {
                        // Phát ra cặp (Thể Loại, Điểm) cho mỗi thể loại của phim đó
                        for (String g : genres) {
                            genreKey.set(g.trim());
                            ratingValue.set(rating);
                            context.write(genreKey, ratingValue);
                        }
                    }
                } catch (NumberFormatException e) {
                    // Bỏ qua dòng lỗi
                }
            }
        }
    }

    // 2. REDUCER: Tính điểm trung bình cho từng Thể loại
    public static class GenreReducer extends Reducer<Text, FloatWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;

            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            float avg = sum / count;
            String outputFormat = String.format("Avg: %.2f, Count: %d", avg, count);
            context.write(key, new Text(outputFormat));
        }
    }

    // 3. DRIVER: Điều phối hệ thống
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Bai2 <movies.txt_path> <ratings_input_dir> <output_dir>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("moviePath", args[0]);

        Job job = Job.getInstance(conf, "Bai 2: Ratings By Genre");
        job.setJarByClass(Bai2.class);

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}