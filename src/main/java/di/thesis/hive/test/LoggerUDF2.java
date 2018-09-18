package di.thesis.hive.test;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LoggerUDF2 extends GenericUDF {

    private StructObjectInspector mbb1;
    private final static Logger LOGGER = Logger.getLogger(LoggerUDF2.class.getName());


    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length!=1)
            throw new UDFArgumentLengthException("TestUDF takes 1 argument!");

        mbb1 = (StandardStructObjectInspector) objectInspectors[0];

        LOGGER.log(Level.INFO, mbb1.getAllStructFieldRefs().toString());

        HashSet c=new HashSet();
        c.add("minx");
        c.add("maxx");

        c.add("miny");
        c.add("maxy");

        c.add("mint");
        c.add("maxt");

        String str = String.valueOf(c.containsAll(mbb1.getAllStructFieldRefs()));

        LOGGER.log(Level.INFO, str);

        //FAILED: RuntimeException cannot find field kati pou den uparxei from [0:id, 1:minx, 2:maxx, 3:miny, 4:maxy, 5:mint, 6:maxt]

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
