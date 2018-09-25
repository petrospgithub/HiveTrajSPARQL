package di.thesis.hive.extras;

import di.thesis.hive.utils.SpatioTemporalObjectInspector;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import utils.checking;

public class EndPoint extends GenericUDF {

    private ListObjectInspector listOI;
    private SettableStructObjectInspector structOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        if (objectInspectors.length!=1)
            throw new UDFArgumentLengthException("StartPoint only takes 1 argument: Trajectory");

        try {

            listOI = (ListObjectInspector) objectInspectors[0];
            structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            boolean check= checking.point(structOI);

            if(!check){
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }

        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

        return new SpatioTemporalObjectInspector().PointObjectInspector();//oti einai edw gurnaei to evaluate
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        try {
            int last = listOI.getListLength(deferredObjects[0].get())-1;

            long timestamp = (long) (structOI.getStructFieldData(listOI.getListElement(deferredObjects[0].get(), last), structOI.getStructFieldRef("timestamp")));
            double longitude = (double) (structOI.getStructFieldData(listOI.getListElement(deferredObjects[0].get(), last), structOI.getStructFieldRef("longitude")));
            double latitude = (double) (structOI.getStructFieldData(listOI.getListElement(deferredObjects[0].get(), last), structOI.getStructFieldRef("latitude")));

            Object[] ret = new Object[3];
            ret[0]=new LongWritable(timestamp);
            ret[1]=new DoubleWritable(longitude);
            ret[2]=new DoubleWritable(latitude);
            return ret;
        } catch (RuntimeException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }
}
