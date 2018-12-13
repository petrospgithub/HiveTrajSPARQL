package di.thesis.hive.extras;

import di.thesis.indexing.types.EnvelopeST;
import di.thesis.indexing.types.PointST;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import utils.SerDerUtil;

public class JStartPointBinary extends GenericUDF {

    private BinaryObjectInspector box=null;


    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        box = (BinaryObjectInspector)objectInspectors[0];

        //return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        BytesWritable query=box.getPrimitiveWritableObject(deferredObjects[0].get());
        PointST[] mbb= SerDerUtil.trajectory_deserialize(query.getBytes());

        return mbb[0].toString();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
