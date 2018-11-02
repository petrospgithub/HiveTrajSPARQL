package di.thesis.hive.statistics;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import utils.checking;

public class Duration extends GenericUDF {

    private ListObjectInspector listOI;
    private SettableStructObjectInspector structOI;


    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length!=1)
            throw new UDFArgumentLengthException("Duration only takes 1 argument: Trajectory");

        try {

            listOI = (ListObjectInspector) arguments[0];
            structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            boolean check= checking.point(structOI);

            if(!check){
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }


        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

        return PrimitiveObjectInspectorFactory.writableLongObjectInspector; //oti einai edw gurnaei to evaluate
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        int last = listOI.getListLength(deferredObjects[0].get())-1;

        long end_timestamp = ((LongWritable) (structOI.getStructFieldData(listOI.getListElement(deferredObjects[0].get(), last), structOI.getStructFieldRef("timestamp")))).get();
        long start_timestamp = ((LongWritable) (structOI.getStructFieldData(listOI.getListElement(deferredObjects[0].get(), 0), structOI.getStructFieldRef("timestamp")))).get();

        return new LongWritable(end_timestamp-start_timestamp);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }
}
