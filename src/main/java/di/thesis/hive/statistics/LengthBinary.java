package di.thesis.hive.statistics;

import di.thesis.indexing.distance.PointDistance;
import di.thesis.indexing.distance.PointManhattan;
import di.thesis.indexing.distance.Pointeuclidean;
import di.thesis.indexing.distance.Pointhaversine;
import di.thesis.indexing.types.PointST;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import utils.SerDerUtil;

import java.util.Objects;

public class LengthBinary extends GenericUDF {

    private BinaryObjectInspector b;
//    private SettableStructObjectInspector structOI;
    private StringObjectInspector func_name=null;

    private static final Log LOG = LogFactory.getLog(Length.class.getName());


    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length!=2) {
            throw new UDFArgumentLengthException("Length only takes 2 arguments: Trajectory, Distance function");
        }

        try {
            b = (BinaryObjectInspector) objectInspectors[0];
         /*
            structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            boolean check= checking.point(structOI);

            if(!check){
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }
*/
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

        BytesWritable trajB=b.getPrimitiveWritableObject(deferredObjects[0].get());
        PointST[] trajectory = SerDerUtil.trajectory_deserialize(trajB.getBytes());


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

        for (int i=0; i<trajectory.length-1; i++) {
            curr_longitude = trajectory[i].getLongitude();

            curr_latitude = trajectory[i].getLatitude();

            next_longitude = trajectory[i+1].getLongitude();
            next_latitude = trajectory[i+1].getLatitude();

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
