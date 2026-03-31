import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai1 {

    // 1. MAPPER: Đọc từng dòng rating
    public static class RatingsMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {
        private IntWritable movieId = new IntWritable();
        private FloatWritable rating = new FloatWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Tự động nhận diện dấu phân cách là phẩy hoặc ::
            String delimiter = line.contains("::") ? "::" : ",";
            String[] fields = line.split(delimiter);

            if (fields.length >= 3) {
                try {
                    // Try-catch giúp bỏ qua dòng tiêu đề nếu có
                    int mId = Integer.parseInt(fields[1].trim());
                    float r = Float.parseFloat(fields[2].trim());

                    movieId.set(mId);
                    rating.set(r);
                    context.write(movieId, rating);
                } catch (NumberFormatException e) {
                    // Âm thầm bỏ qua các dòng không phải là số
                }
            }
        }
    }

    // 2. REDUCER: Tính toán và ghép tên phim
    public static class RatingsReducer extends Reducer<IntWritable, FloatWritable, Text, Text> {
        private Map<Integer, String> movieMap = new HashMap<>();
        private String maxMovie = "";
        private float maxRating = -1.0f; 

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String moviePathStr = conf.get("moviePath");

            if (moviePathStr != null) {
                Path path = new Path(moviePathStr);
                FileSystem fs = FileSystem.get(conf);

                // Dùng try-with-resources và ép chuẩn UTF-8
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String delimiter = line.contains("::") ? "::" : ",";
                        String[] parts = line.split(delimiter, 3); // Cắt tối đa 3 phần để bảo toàn tên phim có dấu phẩy
                        if (parts.length >= 2) {
                            try {
                                int id = Integer.parseInt(parts[0].trim());
                                String title = parts[1].trim();
                                movieMap.put(id, title);
                            } catch (NumberFormatException e) {
                                // Bỏ qua dòng tiêu đề
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;

            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            float avg = sum / count;
            // Nếu không tìm thấy tên, in ra ID thay vì Unknown trống lốc
            String title = movieMap.getOrDefault(key.get(), "Unknown Movie (ID: " + key.get() + ")");

            // In kết quả từng phim
            String outputInfo = String.format("Average rating: %.2f (Total ratings: %d)", avg, count);
            context.write(new Text(title), new Text(outputInfo));

            // Cập nhật kỷ lục phim cao điểm nhất (điều kiện: >= 5 lượt đánh giá)
            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = title;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("-------------------------------------------------------"), new Text(""));
            if (maxMovie.isEmpty()) {
                context.write(new Text("[RESULT]"), new Text("No movie has at least 5 ratings."));
            } else {
                String result = String.format("is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings.", maxRating);
                context.write(new Text("[WINNER] " + maxMovie), new Text(result));
            }
        }
    }

    // 3. DRIVER: Cấu hình và chạy
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Bai1 <movies.txt_path> <ratings_input_dir> <output_dir>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        
        // Truyền đường dẫn movies.txt vào cấu hình
        conf.set("moviePath", args[0]);

        Job job = Job.getInstance(conf, "Bai 1: Calculate Movie Ratings (Upgraded)");
        job.setJarByClass(Bai1.class);

        job.setMapperClass(RatingsMapper.class);
        job.setReducerClass(RatingsReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}