package di.thesis.hive.similarity;

import di.thesis.indexing.distance.DTW;
import di.thesis.indexing.types.PointST;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import utils.SerDerUtil;

public class DTWBinary extends GenericUDF {
/*
    private ListObjectInspector trajectoryA_listOI;
    private SettableStructObjectInspector trajectoryA_structOI;

    private ListObjectInspector trajectoryB_listOI;
    private SettableStructObjectInspector trajectoryB_structOI;
*/

    private BinaryObjectInspector trajA;
    private BinaryObjectInspector trajB;

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
         //   trajectoryA_listOI = (StandardListObjectInspector) objectInspectors[0];
         //   trajectoryA_structOI = (SettableStructObjectInspector) trajectoryA_listOI.getListElementObjectInspector();

           // trajectoryB_listOI = (StandardListObjectInspector) objectInspectors[1];
          //  trajectoryB_structOI = (SettableStructObjectInspector) trajectoryB_listOI.getListElementObjectInspector();

            trajA=(BinaryObjectInspector) objectInspectors[0];
            trajB=(BinaryObjectInspector) objectInspectors[1];

            fast = (IntObjectInspector)objectInspectors[2];
            func_name=(StringObjectInspector)objectInspectors[3];

            //  accept_dist=(WritableConstantHiveDecimalObjectInspector) objectInspectors[4];

            min_ts_tolerance=(IntObjectInspector) objectInspectors[4];

            max_ts_tolerance=(IntObjectInspector) objectInspectors[5];

/*
            boolean check= checking.point(trajectoryA_structOI);
            boolean check2= checking.point(trajectoryB_structOI);

            if(!check || !check2){
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }
*/
        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

     //   Object trajA=deferredObjects[0].get();
     //   Object trajB=deferredObjects[1].get();

        BytesWritable trajBinaryA=trajA.getPrimitiveWritableObject(deferredObjects[0].get());
        PointST[] trajectoryA= SerDerUtil.trajectory_deserialize(trajBinaryA.getBytes());

        BytesWritable trajBinaryB=trajB.getPrimitiveWritableObject(deferredObjects[1].get());
        PointST[] trajectoryB= SerDerUtil.trajectory_deserialize(trajBinaryB.getBytes());


    //    int trajectoryA_length=trajectoryA.length;

    //    int trajectoryB_length=trajectoryB.length;

        int w=fast.get(deferredObjects[2].get());

        String f=func_name.getPrimitiveJavaObject(deferredObjects[3].get());

        //double d=accept_dist.getPrimitiveJavaObject(deferredObjects[4].get()).doubleValue();

        int minTSext=min_ts_tolerance.get(deferredObjects[4].get());
        int maxTSext=max_ts_tolerance.get(deferredObjects[5].get());

      //  PointDistance func;
/*
        if (Objects.equals(f, "Havershine")) {
            func= new Pointhaversine();
        } else if (Objects.equals(f, "Manhattan")) {
            func= new Pointeuclidean();
        } else if (Objects.equals(f, "Euclidean")) {
            func= new PointManhattan();
        } else {
            throw new HiveException("No valid function");
        }
*/
//        long min_tsA = trajectoryA[0].getTimestamp();//((LongWritable) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, 0), trajectoryA_structOI.getStructFieldRef("timestamp")))).get();
   //     long max_tsA = trajectoryA[trajectoryA_length-1].getTimestamp();//((LongWritable) (trajectoryA_structOI.getStructFieldData(trajectoryA_listOI.getListElement(trajA, trajectoryA_length-1), trajectoryA_structOI.getStructFieldRef("timestamp")))).get();

  //      long min_tsB = trajectoryB[0].getTimestamp();//((LongWritable) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajA, 0), trajectoryB_structOI.getStructFieldRef("timestamp")))).get();
   //     long max_tsB = trajectoryB[trajectoryB_length-1].getTimestamp();//((LongWritable) (trajectoryB_structOI.getStructFieldData(trajectoryB_listOI.getListElement(trajA, trajectoryA_length-1), trajectoryB_structOI.getStructFieldRef("timestamp")))).get();

        //an einai arnhtika kai ta 2 sigoura DTWUDF
        //an einai 8etika kai ta 2 koitaw an kanoun overlap
        DTW dtw=new DTW(trajectoryA);

        try {
            return new DoubleWritable(dtw.similarity(trajectoryB,w,f,minTSext,maxTSext));
        } catch (Exception e) {
            throw new HiveException(e);
        }

/*
        if (minTSext<0 || maxTSext<0) {


            dtw.similarity(trajectoryB,w,f,minTSext,maxTSext);

            double calc=calculate(trajectoryA_length, trajectoryB_length, trajA, trajB, func, w);

            double traj_dist=(calc / (double)Math.min(trajectoryA_length,trajectoryB_length));

            //if (traj_dist<=d) {
            // } else {
            //     return new DoubleWritable(Double.MAX_VALUE);
            // }

        } else if ( (min_tsA-minTSext)<=max_tsB && min_tsB<=(max_tsA+maxTSext) ) {
           // double calc=calculate(trajectoryA_length, trajectoryB_length, trajA, trajB, func, w);

         //   double traj_dist=(calc / (double)Math.min(trajectoryA_length,trajectoryB_length));

            //  if (traj_dist<=d) {
            return new DoubleWritable(traj_dist);
            // } else {
            //     return null;
            //}
        } else {
            return new DoubleWritable(Double.MAX_VALUE);
        }
*/
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }

/*
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
*/
}
