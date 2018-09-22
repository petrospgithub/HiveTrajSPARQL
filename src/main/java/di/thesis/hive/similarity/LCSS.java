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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.DoubleWritable;
import utils.checking;

import java.util.Objects;

public class LCSS extends GenericUDF {

    private ListObjectInspector trajectoryA_listOI;
    private SettableStructObjectInspector trajectoryA_structOI;

    private ListObjectInspector trajectoryB_listOI;
    private SettableStructObjectInspector trajectoryB_structOI;

    private StringObjectInspector func_name;
    private DoubleObjectInspector eps;
    private IntObjectInspector d;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length!=5)
            throw new UDFArgumentLengthException("ST_Intersects3D only takes 2 arguments!");

        try {
            trajectoryA_listOI = (StandardListObjectInspector) objectInspectors[0];
            trajectoryA_structOI = (SettableStructObjectInspector) trajectoryA_listOI.getListElementObjectInspector();

            trajectoryB_listOI = (StandardListObjectInspector) objectInspectors[1];
            trajectoryB_structOI = (SettableStructObjectInspector) trajectoryB_listOI.getListElementObjectInspector();

            func_name=(StringObjectInspector)objectInspectors[2];

            eps=(DoubleObjectInspector)objectInspectors[3];

            d=(IntObjectInspector)objectInspectors[4];

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

        int trajectoryA_length=trajectoryA_listOI.getListLength(trajA);

        int trajectoryB_length=trajectoryA_listOI.getListLength(trajB);

        String f=func_name.getPrimitiveJavaObject(deferredObjects[2].get());

        double error=eps.get(deferredObjects[3].get());

        int delta=d.get(deferredObjects[4].get());

        int[][] LCS_distance_matrix = new int[trajectoryA_length+1][trajectoryB_length+1];

        Distance func;

        double trajA_longitude;
        double trajA_latitude;
        long trajA_timestamp;

        double trajB_longitude;
        double trajB_latitude;
        long trajB_timestamp;

        double distance;

        if (Objects.equals(f, "Havershine")) {
            func= Haversine$.MODULE$;
        } else if (Objects.equals(f, "Manhattan")) {
            func= Euclidean$.MODULE$;
        } else if (Objects.equals(f, "Euclidean")) {
            func= Manhattan$.MODULE$;
        } else {
            throw new HiveException("No valid function");
        }

        for (int i = 0; i <= trajectoryB_length; i++) {
            LCS_distance_matrix[0][i] = 0;
        }

        for (int i = 0; i <= trajectoryA_length; i++) {
            LCS_distance_matrix[i][0] = 0;
        }

        for (int i = 1; i <= trajectoryA_length; i++) {

            trajA_longitude = (double) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, i-1), trajectoryA_structOI.getStructFieldRef("longitude")));
            trajA_latitude = (double) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, i-1), trajectoryA_structOI.getStructFieldRef("latitude")));
            trajA_timestamp = (long) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, i-1), trajectoryA_structOI.getStructFieldRef("timestamp")));

            for (int j = 1; j <= trajectoryB_length; j++) {

                trajB_longitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, j-1), trajectoryB_structOI.getStructFieldRef("longitude")));
                trajB_latitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, j-1), trajectoryB_structOI.getStructFieldRef("latitude")));
                trajB_timestamp = (long) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajB, i-1), trajectoryA_structOI.getStructFieldRef("timestamp")));


                distance=func.get(trajA_latitude, trajA_longitude, trajB_latitude, trajB_longitude);
                if (distance<=error && Math.abs(trajA_timestamp-trajB_timestamp)<=delta) {
                    LCS_distance_matrix[i][j] = LCS_distance_matrix[i - 1][j - 1] + 1;
                } else {
                    LCS_distance_matrix[i][j] = Math.max(LCS_distance_matrix[i - 1][j], LCS_distance_matrix[i][j - 1]);
                }
            }
        }

        /* bazoume ena alignment ston xrono...  stin DTW metrame mono apostash xwris na logariazoume to xrono*/

        int a = trajectoryA_length;
        int b = trajectoryB_length;

        //ArrayList<PointST> common=new ArrayList<PointST>();
        while (a!=0 && b!=0) {

            trajA_longitude = (double) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, a-1), trajectoryA_structOI.getStructFieldRef("longitude")));
            trajA_latitude = (double) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, a-1), trajectoryA_structOI.getStructFieldRef("latitude")));
            trajA_timestamp = (long) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, a-1), trajectoryA_structOI.getStructFieldRef("timestamp")));

            trajB_longitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, b-1), trajectoryB_structOI.getStructFieldRef("longitude")));
            trajB_latitude = (double) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, b-1), trajectoryB_structOI.getStructFieldRef("latitude")));
            trajB_timestamp = (long) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajB, b-1), trajectoryA_structOI.getStructFieldRef("timestamp")));

            distance=func.get(trajA_latitude, trajA_longitude, trajB_latitude, trajB_longitude);
            if(distance<=error && Math.abs(trajA_timestamp-trajB_timestamp)<=delta) {
                a--;
                b--;
            } else {
                if (LCS_distance_matrix[a-1][b]>= LCS_distance_matrix[a][b-1]) {
                    a--;
                } else {
                    b--;
                }
            }
        }

        return new DoubleWritable(1-((double)LCS_distance_matrix[trajectoryA_length][trajectoryB_length]/(double)Math.min(trajectoryA_length,trajectoryB_length)));
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }
}
