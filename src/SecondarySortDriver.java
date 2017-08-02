/**
 * Created by egamkar on 7/14/2017.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out
                    .printf("Two parameters are required for SecondarySortDriver- <input dir> <output dir>\n");
            return -1;
        }

        Job job = new Job(getConf());
        job.setJobName("Secondary sort example");

        job.setJarByClass(SecondarySortDriver.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SecondarySortMapper.class);
        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setSortComparatorClass(CompositeKeySortComparator.class);
        job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setOutputKeyClass(CompositeKeyWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(8);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
                new SecondarySortDriver(), args);
        System.exit(exitCode);
    }
}
