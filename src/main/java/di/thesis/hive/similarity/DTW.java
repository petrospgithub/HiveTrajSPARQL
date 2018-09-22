package di.thesis.hive.similarity;

import distance.Distance;
import distance.Euclidean$;
import distance.Haversine$;
import distance.Manhattan$;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.DoubleWritable;
import utils.checking;

import java.util.Objects;

public class DTW extends GenericUDF {

    private ListObjectInspector trajectoryA_listOI;
    private SettableStructObjectInspector trajectoryA_structOI;

    private ListObjectInspector trajectoryB_listOI;
    private SettableStructObjectInspector trajectoryB_structOI;

    private IntObjectInspector fast;
    private StringObjectInspector func_name;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length!=4)
            throw new UDFArgumentLengthException("ST_Intersects3D only takes 2 arguments!");

        try {
            trajectoryA_listOI = (StandardListObjectInspector) objectInspectors[0];
            trajectoryA_structOI = (SettableStructObjectInspector) trajectoryA_listOI.getListElementObjectInspector();

            trajectoryB_listOI = (StandardListObjectInspector) objectInspectors[1];
            trajectoryB_structOI = (SettableStructObjectInspector) trajectoryB_listOI.getListElementObjectInspector();

            fast = (IntObjectInspector)objectInspectors[2];
            func_name=(StringObjectInspector)objectInspectors[3];


            boolean check= checking.point(trajectoryA_structOI);
            boolean check2= checking.point(trajectoryB_structOI);

            if(!check || !check2){
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }



        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        Object trajA=deferredObjects[0].get();
        Object trajB=deferredObjects[1].get();

        int trajectoryA_length=trajectoryA_listOI.getListLength(deferredObjects[0].get());

        int trajectoryB_length=trajectoryB_listOI.getListLength(deferredObjects[1].get());

        int w=fast.get(deferredObjects[2].get());

        String f=func_name.getPrimitiveJavaObject(deferredObjects[3].get());

        double[][] DTW_distance_matrix=new double[trajectoryA_length][trajectoryB_length];

        double distance;
        Distance func;

        double trajA_longitude;
        double trajA_latitude;

        double trajB_longitude;
        double trajB_latitude;

        if (Objects.equals(f, "Havershine")) {
            func= Haversine$.MODULE$;
        } else if (Objects.equals(f, "Manhattan")) {
            func= Euclidean$.MODULE$;
        } else if (Objects.equals(f, "Euclidean")) {
            func= Manhattan$.MODULE$;
        } else {
            throw new HiveException("No valid function");
        }


        trajA_longitude = (double) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, 0), trajectoryA_structOI.getStructFieldRef("longitude")));
        trajA_latitude = (double) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, 0), trajectoryA_structOI.getStructFieldRef("latitude")));

        trajB_longitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, 0), trajectoryB_structOI.getStructFieldRef("longitude")));
        trajB_latitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, 0), trajectoryB_structOI.getStructFieldRef("latitude")));

        DTW_distance_matrix[0][0]=func.get(trajA_latitude, trajA_longitude, trajB_latitude, trajB_longitude);
        for (int i = 0; i < trajectoryB_length; i++) {
            trajB_longitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, i), trajectoryB_structOI.getStructFieldRef("longitude")));
            trajB_latitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, i), trajectoryB_structOI.getStructFieldRef("latitude")));
            DTW_distance_matrix[0][i] = func.get(trajA_latitude, trajA_longitude, trajB_latitude, trajB_longitude);
            //LOGGER.log(Level.WARNING, String.valueOf(func.distance(trajA_latitude, trajA_longitude, trajB_longitude, trajB_latitude)));
        }

        trajB_longitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, 0), trajectoryB_structOI.getStructFieldRef("longitude")));
        trajB_latitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, 0), trajectoryB_structOI.getStructFieldRef("latitude")));
        for (int i = 0; i < trajectoryA_length; i++) {
            trajA_longitude = (double) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, i), trajectoryA_structOI.getStructFieldRef("longitude")));
            trajA_latitude = (double) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, i), trajectoryA_structOI.getStructFieldRef("latitude")));

            DTW_distance_matrix[i][0] = DTW_distance_matrix[0][0]=func.get(trajA_latitude, trajA_longitude, trajB_latitude, trajB_longitude);
        }

        w = Math.max(w, Math.abs(trajectoryA_length-trajectoryB_length)); // adapt window size (*)
        for (int i = 1; i < trajectoryA_length; i++) {

            trajA_longitude = (double) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, i - 1), trajectoryA_structOI.getStructFieldRef("longitude")));
            trajA_latitude = (double) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, i - 1), trajectoryA_structOI.getStructFieldRef("latitude")));

            for (int j = Math.max(1, i-w); j < Math.min(trajectoryB_length, i+w); j++) {

                trajB_longitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, j - 1), trajectoryB_structOI.getStructFieldRef("longitude")));
                trajB_latitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, j - 1), trajectoryB_structOI.getStructFieldRef("latitude")));

                distance=func.get(trajA_latitude, trajA_longitude, trajB_latitude, trajB_longitude);

                DTW_distance_matrix[i][j]=distance+
                        Math.min(Math.min(DTW_distance_matrix[i-1][j], DTW_distance_matrix[i][j-1]), DTW_distance_matrix[i-1][j-1]);

            }
        }
        return new DoubleWritable(DTW_distance_matrix[trajectoryA_length-1][trajectoryB_length-1]);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }
}
