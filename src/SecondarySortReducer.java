/**
 * Created by egamkar on 7/14/2017.
 */
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends
        Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable> {

    @Override
    public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values,
                       Context context) throws IOException, InterruptedException {

        for (NullWritable value : values) {

            context.write(key, NullWritable.get());
        }

    }
}
