package di.thesis.hive.similarity;

import di.thesis.indexing.spatiotemporaljts.STRtree3D;
import di.thesis.indexing.types.PointST;
import di.thesis.indexing.types.Triplet;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import utils.SerDerUtil;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

public class IndeKNNBinaryStoreTraj extends GenericUDTF {

    private BinaryObjectInspector treeOI = null;

    private BinaryObjectInspector queryOI = null;

    private HiveDecimalObjectInspector dist_threshold;
    private IntObjectInspector minT_tolerance;
    private IntObjectInspector maxT_tolerance;
    private WritableLongObjectInspector traj_rowID;

    private StringObjectInspector similarityFuncOI;
    private StringObjectInspector pointFuncOI;

    private IntObjectInspector kOI;


    private IntObjectInspector wOI;

    private HiveDecimalObjectInspector epsOI;
    private IntObjectInspector deltaOI;


   private transient Object[] forwardMapObj = new Object[5];

    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        //  if (objectInspectors.length<6)
        // throw new UDFArgumentLengthException("IndexKNN at least 6 arguments!");

        try {

            //  listOI = (ListObjectInspector) objectInspectors[0];
            // structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            queryOI = (BinaryObjectInspector) objectInspectors[0];

            treeOI = (BinaryObjectInspector) objectInspectors[1];

            // k=(IntObjectInspector) objectInspectors[2];
            dist_threshold = (HiveDecimalObjectInspector) objectInspectors[2];
            minT_tolerance = (IntObjectInspector) objectInspectors[3];
            maxT_tolerance = (IntObjectInspector) objectInspectors[4];

            traj_rowID = (WritableLongObjectInspector) objectInspectors[5];

            similarityFuncOI = (StringObjectInspector) objectInspectors[6];
            pointFuncOI = (StringObjectInspector) objectInspectors[7];

            kOI = (IntObjectInspector) objectInspectors[8];

            wOI = (IntObjectInspector) objectInspectors[9];
            epsOI = (HiveDecimalObjectInspector) objectInspectors[10];
            deltaOI = (IntObjectInspector) objectInspectors[11];


        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

       // forwardMapObj = new Object[(objectInspectors.length - 11) + 2];

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("trajArowid");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        fieldNames.add("distance");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        fieldNames.add("rowid");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        // LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  internalMergeOI" );
        fieldNames.add("traja");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);

        fieldNames.add("trajb");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);

        /*
        for (int i = 12; i < objectInspectors.length; i++) {
            fieldNames.add("col" + i);
            fieldOIs.add(objectInspectors[i]);
        }
        */

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);

    }

    @Override
    public void process(Object[] objects) throws HiveException {
        try {
            BytesWritable tree = treeOI.getPrimitiveWritableObject(objects[1]);

            long rowID = traj_rowID.get(objects[5]);

            ByteArrayInputStream bis = new ByteArrayInputStream(tree.getBytes());
            ObjectInput in = new ObjectInputStream(bis);
            STRtree3D retrievedObject = (STRtree3D) in.readObject();

            //Object traj=objects[0];


            BytesWritable trajBinaryA = queryOI.getPrimitiveWritableObject(objects[0]);
            PointST[] trajectoryA = SerDerUtil.trajectory_deserialize(trajBinaryA.getBytes());


            double threshold = dist_threshold.getPrimitiveJavaObject(objects[2]).doubleValue();
            int minTtolerance = minT_tolerance.get(objects[3]);
            int maxTtolerance = maxT_tolerance.get(objects[4]);


            String SF=similarityFuncOI.getPrimitiveJavaObject(objects[6]);
            String PF=pointFuncOI.getPrimitiveJavaObject(objects[7]);

            int k=kOI.get(objects[8]);
            int w=wOI.get(objects[9]);
            double eps=epsOI.getPrimitiveJavaObject(objects[10]).doubleValue();
            int delta=deltaOI.get(objects[11]);

            List<Triplet> tree_results = retrievedObject.knn(trajectoryA, threshold, SF, k, minTtolerance, maxTtolerance, PF, w, eps, delta);

            for (int i=0; i<tree_results.size(); i++) {
                //Long entry = (Long) tree_results.get(i);
                forwardMapObj[0]= new LongWritable(rowID);
                forwardMapObj[1] = new DoubleWritable(tree_results.get(i).getDistance());
                forwardMapObj[2] = new LongWritable(tree_results.get(i).getId());

                forwardMapObj[3] = trajBinaryA;
                forwardMapObj[4] = new BytesWritable(SerDerUtil.trajectory_serialize(tree_results.get(i).getTrajectory()));


                forward(forwardMapObj);
            }

        } catch (Exception e) {
            throw new HiveException(e);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
