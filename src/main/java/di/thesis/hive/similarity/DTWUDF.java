package di.thesis.hive.similarity;

import di.thesis.indexing.distance.PointDistance;
import di.thesis.indexing.distance.PointManhattan;
import di.thesis.indexing.distance.Pointeuclidean;
import di.thesis.indexing.distance.Pointhaversine;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import utils.checking;

import java.util.Objects;

public class DTWUDF extends GenericUDF {

    private ListObjectInspector trajectoryA_listOI;
    private SettableStructObjectInspector trajectoryA_structOI;

    private ListObjectInspector trajectoryB_listOI;
    private SettableStructObjectInspector trajectoryB_structOI;

    private IntObjectInspector fast;
    private StringObjectInspector func_name;

   // private HiveDecimalObjectInspector accept_dist;

    private IntObjectInspector min_ts_tolerance;
    private IntObjectInspector max_ts_tolerance;


    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length!=6)
            throw new UDFArgumentLengthException("DTWUDF only takes 7 arguments!");

        try {
            trajectoryA_listOI = (StandardListObjectInspector) objectInspectors[0];
            trajectoryA_structOI = (SettableStructObjectInspector) trajectoryA_listOI.getListElementObjectInspector();

            trajectoryB_listOI = (StandardListObjectInspector) objectInspectors[1];
            trajectoryB_structOI = (SettableStructObjectInspector) trajectoryB_listOI.getListElementObjectInspector();

            fast = (IntObjectInspector)objectInspectors[2];
            func_name=(StringObjectInspector)objectInspectors[3];

          //  accept_dist=(WritableConstantHiveDecimalObjectInspector) objectInspectors[4];

            min_ts_tolerance=(IntObjectInspector) objectInspectors[4];

            max_ts_tolerance=(IntObjectInspector) objectInspectors[5];


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

        //double d=accept_dist.getPrimitiveJavaObject(deferredObjects[4].get()).doubleValue();

        int minTSext=min_ts_tolerance.get(deferredObjects[4].get());
        int maxTSext=max_ts_tolerance.get(deferredObjects[5].get());

        PointDistance func;

        if (Objects.equals(f, "Havershine")) {
            func= new Pointhaversine();
        } else if (Objects.equals(f, "Manhattan")) {
            func= new Pointeuclidean();
        } else if (Objects.equals(f, "Euclidean")) {
            func= new PointManhattan();
        } else {
            throw new HiveException("No valid function");
        }

        long min_tsA = ((LongWritable) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, 0), trajectoryA_structOI.getStructFieldRef("timestamp")))).get();
        long max_tsA = ((LongWritable) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, trajectoryA_length-1), trajectoryA_structOI.getStructFieldRef("timestamp")))).get();

        long min_tsB = ((LongWritable) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajA, 0), trajectoryB_structOI.getStructFieldRef("timestamp")))).get();
        long max_tsB = ((LongWritable) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajA, trajectoryA_length-1), trajectoryB_structOI.getStructFieldRef("timestamp")))).get();

        //an einai arnhtika kai ta 2 sigoura DTWUDF
        //an einai 8etika kai ta 2 koitaw an kanoun overlap

        if (minTSext<0 || maxTSext<0) {
            double calc=calculate(trajectoryA_length, trajectoryB_length, trajA, trajB, func, w);

            double traj_dist=(calc / (double)Math.min(trajectoryA_length,trajectoryB_length));

            //if (traj_dist<=d) {
                return new DoubleWritable(traj_dist);
           // } else {
           //     return new DoubleWritable(Double.MAX_VALUE);
           // }

        } else if ( (min_tsA-minTSext)<=max_tsB && min_tsB<=(max_tsA+maxTSext) ) {
            double calc=calculate(trajectoryA_length, trajectoryB_length, trajA, trajB, func, w);

            double traj_dist=(calc / (double)Math.min(trajectoryA_length,trajectoryB_length));

          //  if (traj_dist<=d) {
                return new DoubleWritable(traj_dist);
           // } else {
           //     return null;
            //}
        } else {
            return new DoubleWritable(Double.MAX_VALUE);
        }

    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }


    private double calculate (int trajectoryA_length, int trajectoryB_length, Object trajA, Object trajB, PointDistance func, int w) {

        double[][] DTW_distance_matrix=new double[trajectoryA_length][trajectoryB_length];

        double trajA_longitude;
        double trajA_latitude;

        double trajB_longitude;
        double trajB_latitude;

        double distance;

        trajA_longitude = ((DoubleWritable)  (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, 0), trajectoryA_structOI.getStructFieldRef("longitude")))).get();
        trajA_latitude = ((DoubleWritable)  (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, 0), trajectoryA_structOI.getStructFieldRef("latitude")))).get();

        trajB_longitude = ((DoubleWritable)  (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, 0), trajectoryB_structOI.getStructFieldRef("longitude")))).get();
        trajB_latitude = ((DoubleWritable)  (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, 0), trajectoryB_structOI.getStructFieldRef("latitude")))).get();

        DTW_distance_matrix[0][0]=func.calculate(trajA_latitude, trajA_longitude, trajB_latitude, trajB_longitude);
        for (int i = 0; i < trajectoryB_length; i++) {
            trajB_longitude = ((DoubleWritable)  (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, i), trajectoryB_structOI.getStructFieldRef("longitude")))).get();
            trajB_latitude = ((DoubleWritable)  (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, i), trajectoryB_structOI.getStructFieldRef("latitude")))).get();
            DTW_distance_matrix[0][i] = func.calculate(trajA_latitude, trajA_longitude, trajB_latitude, trajB_longitude);
            //LOGGER.log(Level.WARNING, String.valueOf(func.distance(trajA_latitude, trajA_longitude, trajB_longitude, trajB_latitude)));
        }

        trajB_longitude = ((DoubleWritable)  (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, 0), trajectoryB_structOI.getStructFieldRef("longitude")))).get();
        trajB_latitude = ((DoubleWritable)  (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, 0), trajectoryB_structOI.getStructFieldRef("latitude")))).get();
        for (int i = 0; i < trajectoryA_length; i++) {
            trajA_longitude = ((DoubleWritable)  (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, i), trajectoryA_structOI.getStructFieldRef("longitude")))).get();
            trajA_latitude = ((DoubleWritable)  (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, i), trajectoryA_structOI.getStructFieldRef("latitude")))).get();

            DTW_distance_matrix[i][0] = DTW_distance_matrix[0][0]=func.calculate(trajA_latitude, trajA_longitude, trajB_latitude, trajB_longitude);
        }

        w = Math.max(w, Math.abs(trajectoryA_length-trajectoryB_length)); // adapt window size (*)
        for (int i = 1; i < trajectoryA_length; i++) {

            trajA_longitude = ((DoubleWritable) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, i - 1), trajectoryA_structOI.getStructFieldRef("longitude")))).get();
            trajA_latitude = ((DoubleWritable)  (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, i - 1), trajectoryA_structOI.getStructFieldRef("latitude")))).get();

            for (int j = Math.max(1, i-w); j < Math.min(trajectoryB_length, i+w); j++) {

                trajB_longitude = ((DoubleWritable)  (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, j - 1), trajectoryB_structOI.getStructFieldRef("longitude")))).get();
                trajB_latitude = ((DoubleWritable)  (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajB, j - 1), trajectoryB_structOI.getStructFieldRef("latitude")))).get();

                distance=func.calculate(trajA_latitude, trajA_longitude, trajB_latitude, trajB_longitude);

                DTW_distance_matrix[i][j]=distance+
                        Math.min(Math.min(DTW_distance_matrix[i-1][j], DTW_distance_matrix[i][j-1]), DTW_distance_matrix[i-1][j-1]);

            }
        }

        return DTW_distance_matrix[trajectoryA_length-1][trajectoryB_length-1];
    }

}
