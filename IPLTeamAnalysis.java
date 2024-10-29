import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IPLTeamAnalysis {

    public static class IPLMatchMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String team1 = fields[5];
            String team2 = fields[6];
            String winningTeam = fields[10];

            // Emit team names and match results
            context.write(new Text(team1), new Text("win"));
            context.write(new Text(team2), new Text("loss"));
            if (!winningTeam.equals("NA")) {
                context.write(new Text(winningTeam), new Text("win"));
            }
        }
    }

    public static class IPLMatchReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int wins = 0;
            int matches = 0;

            for (Text val : values) {
                if (val.toString().equals("win")) {
                    wins++;
                }
                matches++;
            }

            // Calculate winning percentage
            double winningPercentage = (double) wins / matches * 100;
            String cluster = "";
            if (winningPercentage >= 50) {
                cluster = "Top Performer";
            } else if (winningPercentage >= 30) {
                cluster = "Midfield Performers";
            } else {
                cluster = "Lower Performers";
            }

            // Emit team name, winning percentage, and cluster
            context.write(key, new Text(String.format("%.2f%%, %s", winningPercentage, cluster)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IPL Match Analysis");
        job.setJarByClass(IPLTeamAnalysis.class);
        job.setMapperClass(IPLMatchMapper.class);
        job.setReducerClass(IPLMatchReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

