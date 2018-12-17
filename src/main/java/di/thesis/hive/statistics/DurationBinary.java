package di.thesis.hive.statistics;

import di.thesis.indexing.types.PointST;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import utils.SerDerUtil;
import utils.checking;

public class DurationBinary extends GenericUDF {

    private BinaryObjectInspector b;
    //private SettableStructObjectInspector structOI;


    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length!=1)
            throw new UDFArgumentLengthException("Duration only takes 1 argument: Trajectory");

        try {

            b = (BinaryObjectInspector) arguments[0];
          //  structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();
/*
            boolean check= checking.point(structOI);

            if(!check){
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }
*/

        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

        return PrimitiveObjectInspectorFactory.writableLongObjectInspector; //oti einai edw gurnaei to evaluate
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] deferredObjects) throws HiveException {

        BytesWritable trajB=b.getPrimitiveWritableObject(deferredObjects[0].get());
        PointST[] trajectory = SerDerUtil.trajectory_deserialize(trajB.getBytes());

        return new LongWritable(trajectory[trajectory.length-1].getTimestamp()-trajectory[0].getTimestamp());
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }
}
