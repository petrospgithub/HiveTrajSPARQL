package di.thesis.hive.similarity;

import di.thesis.indexing.distance.LCSS;
import di.thesis.indexing.distance.PointDistance;
import di.thesis.indexing.distance.PointManhattan;
import di.thesis.indexing.distance.Pointeuclidean;
import di.thesis.indexing.distance.Pointhaversine;
import di.thesis.indexing.types.PointST;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import utils.SerDerUtil;
import utils.checking;

import java.util.Objects;

public class LCSSBinary extends GenericUDF {

    private BinaryObjectInspector trajA;
    private BinaryObjectInspector trajB;

    private StringObjectInspector func_name;

    private ObjectInspector epsOI;

    private IntObjectInspector d;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length!=5)
            throw new UDFArgumentLengthException("LCSSUDF only takes 5 arguments!");

        try {

            trajA=(BinaryObjectInspector) objectInspectors[0];
            trajB=(BinaryObjectInspector) objectInspectors[1];

            func_name=(StringObjectInspector)objectInspectors[2];

            epsOI = objectInspectors[3];


            d=(IntObjectInspector)objectInspectors[4];
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

        BytesWritable trajBinaryA = trajA.getPrimitiveWritableObject(deferredObjects[0].get());
        PointST[] trajectoryA = SerDerUtil.trajectory_deserialize(trajBinaryA.getBytes());

        BytesWritable trajBinaryB = trajB.getPrimitiveWritableObject(deferredObjects[1].get());
        PointST[] trajectoryB = SerDerUtil.trajectory_deserialize(trajBinaryB.getBytes());

        int trajectoryA_length = trajectoryA.length;

        int trajectoryB_length = trajectoryB.length;

        String f = func_name.getPrimitiveJavaObject(deferredObjects[2].get());

       // double error = eps.getPrimitiveJavaObject(deferredObjects[3].get()).doubleValue();


        double eps;

        try {
            eps = ((HiveDecimalObjectInspector)epsOI).getPrimitiveJavaObject(deferredObjects[3]).doubleValue();
        } catch (java.lang.ClassCastException e) {
            eps = ((WritableConstantDoubleObjectInspector)epsOI).get(deferredObjects[3]);
        }

        int delta = d.get(deferredObjects[4].get());

       // int[][] LCS_distance_matrix = new int[trajectoryA_length + 1][trajectoryB_length + 1];

        LCSS lcss=new LCSS(trajectoryA);

        try {
            double sim=lcss.similarity(trajectoryB, f, eps, delta);

            return new DoubleWritable(sim);

        } catch (Exception e) {
            //e.printStackTrace();
            throw new HiveException(e);
        }

    }



    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }
}
