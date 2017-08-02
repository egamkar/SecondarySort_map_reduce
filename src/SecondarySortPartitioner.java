/**
 * Created by egamkar on 7/14/2017.
 */
/***************************************************************
 *Partitioner: SecondarySortPartitioner
 ***************************************************************/

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends
        Partitioner<CompositeKeyWritable, NullWritable> {

    @Override
    public int getPartition(CompositeKeyWritable key, NullWritable value,
                            int numReduceTasks) {

        return (key.getempId().hashCode() % numReduceTasks);
    }
}
