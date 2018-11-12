package di.thesis.hive.similarity;

import di.thesis.indexing.spatiotemporaljts.STRtree3D;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import utils.checking;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

public class IndexKNN extends GenericUDTF {

    private BinaryObjectInspector treeIO=null;
    private ListObjectInspector listOI;
    private SettableStructObjectInspector structOI;

    private HiveDecimalObjectInspector dist_threshold;
    private IntObjectInspector minT_tolerance;
    private IntObjectInspector maxT_tolerance;
    private WritableLongObjectInspector partidOI;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

       // if (objectInspectors.length!=9)
           // throw new UDFArgumentLengthException("ST_IndexIntersects only takes 8 arguments!");

        try {

            listOI = (ListObjectInspector) objectInspectors[0];
            structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            treeIO=(BinaryObjectInspector) objectInspectors[1];

            // k=(IntObjectInspector) objectInspectors[2];
            dist_threshold=(HiveDecimalObjectInspector) objectInspectors[2];
            minT_tolerance=(IntObjectInspector) objectInspectors[3];
            maxT_tolerance=(IntObjectInspector) objectInspectors[4];

            partidOI=(WritableLongObjectInspector)objectInspectors[5];


            boolean check= checking.point(structOI);

            if(!check){
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }

        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        //fieldNames.add("pid");
        fieldNames.add("trajectory_id");
      //  fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);

    }

    private transient final Object[] forwardMapObj = new Object[1];

    @Override
    public void process(Object[] objects) throws HiveException {

        try {
            BytesWritable tree=treeIO.getPrimitiveWritableObject(objects[1]);

            long pid=partidOI.get(objects[5]);

            ByteArrayInputStream bis = new ByteArrayInputStream(tree.getBytes());
            ObjectInput in = new ObjectInputStream(bis);
            STRtree3D retrievedObject = (STRtree3D)in.readObject();

            Object traj=objects[0];

            double threshold= dist_threshold.getPrimitiveJavaObject(objects[2]).doubleValue();
            int minTtolerance= minT_tolerance.get(objects[3]);
            int maxTtolerance= maxT_tolerance.get(objects[4]);

            List tree_results=retrievedObject.knn(traj, listOI, structOI, threshold, minTtolerance, maxTtolerance);

            for (int i=0; i<tree_results.size(); i++) {
                Long entry = (Long) tree_results.get(i);
                //forwardMapObj[0]=new LongWritable(pid);
                forwardMapObj[0] = new LongWritable(entry);
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



/*
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        try {

            listOI = (ListObjectInspector) objectInspectors[0];
            structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            treeIO=(BinaryObjectInspector) objectInspectors[1];

           // k=(IntObjectInspector) objectInspectors[2];
            dist_threshold=(HiveDecimalObjectInspector) objectInspectors[2];
            minT_tolerance=(IntObjectInspector) objectInspectors[3];
            maxT_tolerance=(IntObjectInspector) objectInspectors[4];

            boolean check= checking.point(structOI);

            if(!check){
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }

        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                        .writableLongObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        BytesWritable tree=treeIO.getPrimitiveWritableObject(deferredObjects[1].get());


        ArrayList<LongWritable> result = new ArrayList<>();

        try {

            FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

            STRtree3D retrievedObject = (STRtree3D)conf.asObject(tree.getBytes());
            Object traj=deferredObjects[0].get();

            double threshold= dist_threshold.getPrimitiveJavaObject(deferredObjects[2].get()).doubleValue();
            int minTtolerance= minT_tolerance.get(deferredObjects[3].get());
            int maxTtolerance= maxT_tolerance.get(deferredObjects[4].get());

            List tree_results=retrievedObject.knn(traj, listOI, structOI, threshold, minTtolerance, maxTtolerance);

            for (int i=0; i<tree_results.size(); i++) {

                Long entry=(Long)tree_results.get(i);

                result.add(new LongWritable(entry));
            }

            return result;

        } catch (Exception e) {
            throw new HiveException(e);
        }

       // return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
    */
}
