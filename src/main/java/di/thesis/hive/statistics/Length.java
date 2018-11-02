package di.thesis.hive.statistics;

import di.thesis.hive.test.LoggerUDF2;
import di.thesis.indexing.distance.PointDistance;
import di.thesis.indexing.distance.PointManhattan;
import di.thesis.indexing.distance.Pointeuclidean;
import di.thesis.indexing.distance.Pointhaversine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    private static final Log LOG = LogFactory.getLog(Length.class.getName());


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

        String f = func_name.getPrimitiveJavaObject(deferredObjects[1].get());
        int trajectory_length=listOI.getListLength(deferredObjects[0].get());
        double distance=0;
        PointDistance func ;

        double curr_longitude;
        double curr_latitude;

        double next_longitude;
        double next_latitude;


        if (Objects.equals(f, "Havershine")) {
            func= new Pointhaversine();
        } else if (Objects.equals(f, "Manhattan")) {
            func= new Pointeuclidean();
        } else if (Objects.equals(f, "Euclidean")) {
            func= new PointManhattan();
        } else {
            throw new HiveException("No valid function");
        }

        for (int i=0; i<trajectory_length-1; i++) {
            curr_longitude = ((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("longitude")))).get();
            curr_latitude = ((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("latitude")))).get();

            next_longitude = ((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i+1), structOI.getStructFieldRef("longitude")))).get();
            next_latitude = ((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i+1), structOI.getStructFieldRef("latitude")))).get();

            distance+= func.calculate(curr_latitude, curr_longitude, next_latitude, next_longitude);
/*
            LOG.warn("lon1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "+curr_longitude);
            LOG.warn("lat1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "+curr_latitude);
            LOG.warn("lon2 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "+next_longitude);
            LOG.warn("lat2 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "+next_latitude);
            LOG.warn("DISTANCE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "+distance);
*/
        }

        return new DoubleWritable(distance);

    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }

}
