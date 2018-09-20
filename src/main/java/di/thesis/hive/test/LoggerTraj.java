package di.thesis.hive.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.HashSet;

public class LoggerTraj extends GenericUDF {

    private ListObjectInspector listOI;
    private SettableStructObjectInspector structOI;
    private static final Log LOG = LogFactory.getLog(LoggerUDF2.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length!=1)
            throw new UDFArgumentLengthException("StartPoint only takes 1 argument: Trajectory");

        try {

            listOI = (ListObjectInspector) objectInspectors[0];
            structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            HashSet c=new HashSet();
            c.add("longitude");
            c.add("latitude");

            c.add("timestamp");


            String str = String.valueOf(c.containsAll(structOI.getAllStructFieldRefs()));

            LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ "+str);

        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
