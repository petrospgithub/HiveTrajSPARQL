package di.thesis.hive.similarity;

import di.thesis.indexing.spatiotemporaljts.STRtree3D;
import di.thesis.indexing.types.PointST;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import utils.SerDerUtil;
import utils.checking;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

public class IndexKNNBinary extends GenericUDTF {

    private BinaryObjectInspector treeOI=null;
  //  private ListObjectInspector listOI;
   // private SettableStructObjectInspector structOI;

    private BinaryObjectInspector queryOI=null;

    private HiveDecimalObjectInspector dist_threshold;
    private IntObjectInspector minT_tolerance;
    private IntObjectInspector maxT_tolerance;
    private WritableLongObjectInspector traj_rowID;

    private transient Object[] forwardMapObj = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        if (objectInspectors.length<6)
            throw new UDFArgumentLengthException("IndexKNN at least 6 arguments!");

        try {

          //  listOI = (ListObjectInspector) objectInspectors[0];
           // structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            queryOI=(BinaryObjectInspector) objectInspectors[0];

            treeOI=(BinaryObjectInspector) objectInspectors[1];

            // k=(IntObjectInspector) objectInspectors[2];
            dist_threshold=(HiveDecimalObjectInspector) objectInspectors[2];
            minT_tolerance=(IntObjectInspector) objectInspectors[3];
            maxT_tolerance=(IntObjectInspector) objectInspectors[4];

            traj_rowID=(WritableLongObjectInspector)objectInspectors[5];



        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

        forwardMapObj=new Object[(objectInspectors.length-5)+2];

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("trajectory_id");
        fieldNames.add("rowID");
        fieldNames.add("trajectory");

        fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        fieldOIs.add(objectInspectors[0]);


        for (int i=6; i<objectInspectors.length; i++) {
            fieldNames.add("col"+i);
            fieldOIs.add(objectInspectors[i]);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);

    }

    //private transient final Object[] forwardMapObj = new Object[2];

    @Override
    public void process(Object[] objects) throws HiveException {

        try {
            BytesWritable tree=treeOI.getPrimitiveWritableObject(objects[1]);

            long rowID=traj_rowID.get(objects[5]);

            ByteArrayInputStream bis = new ByteArrayInputStream(tree.getBytes());
            ObjectInput in = new ObjectInputStream(bis);
            STRtree3D retrievedObject = (STRtree3D)in.readObject();

            //Object traj=objects[0];


            BytesWritable trajBinaryA = queryOI.getPrimitiveWritableObject(objects[0]);
            PointST[] trajectoryA = SerDerUtil.trajectory_deserialize(trajBinaryA.getBytes());


            double threshold= dist_threshold.getPrimitiveJavaObject(objects[2]).doubleValue();
            int minTtolerance= minT_tolerance.get(objects[3]);
            int maxTtolerance= maxT_tolerance.get(objects[4]);




            List tree_results=retrievedObject.knn(trajectoryA, threshold, minTtolerance, maxTtolerance);

            for (int i=0; i<tree_results.size(); i++) {
                Long entry = (Long) tree_results.get(i);
                forwardMapObj[0]=new LongWritable(entry);
                forwardMapObj[1] = new LongWritable(rowID);
                forwardMapObj[2] = trajBinaryA;


                for (int j=6; j<objects.length; j++) {
                    forwardMapObj[j-3]=objects[j];
                }

                forward(forwardMapObj);
            }

            //return result;

        } catch (Exception e) {
            throw new HiveException(e);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
