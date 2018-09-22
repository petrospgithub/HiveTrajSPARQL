package di.thesis.hive.statistics;

import distance.*;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.DoubleWritable;
import utils.checking;

import java.util.Objects;

public class Length extends GenericUDF{

    private ListObjectInspector listOI;
    private SettableStructObjectInspector structOI;
    private StringObjectInspector func_name=null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length!=2) {
            throw new UDFArgumentLengthException("Length only takes 2 arguments: Trajectory, Distance function");
        }

        try {
            listOI = (ListObjectInspector) objectInspectors[0];
            structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            boolean check= checking.point(structOI);

            if(!check){
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }

            func_name=(StringObjectInspector)objectInspectors[1];

        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object traj=deferredObjects[0].get();

        String arg = func_name.getPrimitiveJavaObject(deferredObjects[1].get());
        int trajectory_length=listOI.getListLength(deferredObjects[0].get());
        double distance=0;
        Distance func =null;

        double curr_longitude;
        double curr_latitude;

        double next_longitude;
        double next_latitude;

        if (Objects.equals(arg, "Havershine")) {
            func=Haversine$.MODULE$;
        } else if (Objects.equals(arg, "Manhattan")) {
            func= Euclidean$.MODULE$;
        } else if (Objects.equals(arg, "Euclidean")) {
            func= Manhattan$.MODULE$;
        } else {
            throw new HiveException("No valid function");
        }

        for (int i=0; i<trajectory_length-1; i++) {
            curr_longitude = (double) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("longitude")));
            curr_latitude = (double) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("latitude")));

            next_longitude = (double) (structOI.getStructFieldData(listOI.getListElement(traj, i+1), structOI.getStructFieldRef("longitude")));
            next_latitude = (double) (structOI.getStructFieldData(listOI.getListElement(traj, i+1), structOI.getStructFieldRef("latitude")));

            distance+= func.get(curr_latitude, curr_longitude, next_latitude, next_longitude);
        }

        return new DoubleWritable(distance);

    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }

}
