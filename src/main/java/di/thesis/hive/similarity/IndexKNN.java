package di.thesis.hive.similarity;

import di.thesis.indexing.spatiotemporaljts.STRtree3D;
import di.thesis.indexing.types.EnvelopeST;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import utils.checking;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

public class IndexKNN extends GenericUDF {

    private BinaryObjectInspector treeIO=null;
    private ListObjectInspector listOI;
    private SettableStructObjectInspector structOI;

    private IntObjectInspector k;
    private DoubleObjectInspector dist_threshold;
    private IntObjectInspector minT_tolerance;
    private IntObjectInspector maxT_tolerance;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        try {

            listOI = (ListObjectInspector) objectInspectors[0];
            structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            treeIO=(BinaryObjectInspector) objectInspectors[1];

           // k=(IntObjectInspector) objectInspectors[2];
            dist_threshold=(DoubleObjectInspector) objectInspectors[2];
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

        ByteArrayInputStream bis = new ByteArrayInputStream(tree.getBytes());
        ObjectInput in=null;

        try {


            in = new ObjectInputStream(bis);
            STRtree3D o = (STRtree3D) in.readObject();
            Object traj=deferredObjects[0].get();

            double threshold= dist_threshold.get(deferredObjects[2].get());
            int minTtolerance= minT_tolerance.get(deferredObjects[3].get());
            int maxTtolerance= maxT_tolerance.get(deferredObjects[4].get());

            //int trajectoryA_length=listOI.getListLength(deferredObjects[0].get());

            List tree_results=o.knn(traj, listOI, structOI, threshold, minTtolerance, maxTtolerance);

            for (int i=0; i<tree_results.size(); i++) {

                Integer entry=(Integer)((AbstractMap.SimpleImmutableEntry)tree_results).getKey();

                result.add(new LongWritable(entry));
            }

            return result;


        } catch (IOException | ClassNotFoundException e) {
            throw new HiveException(e);
        }

       // return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
