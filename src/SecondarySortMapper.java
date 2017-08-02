/**
 * Created by egamkar on 7/14/2017.
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends
        Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (value.toString().length() > 0) {
            String arrEmpAttributes[] = value.toString().split(",");

            context.write(
                    new CompositeKeyWritable(arrEmpAttributes[0],Integer.parseInt(arrEmpAttributes[1])), NullWritable.get());
        }

    }
}
