package di.thesis.hive.test;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Level;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import utils.checking;

public class LoggerUDF2 extends GenericUDF {

    private SettableStructObjectInspector mbb1;
   // private final static Logger LOGGER = Logger.getLogger(LoggerUDF2.class.getName());
    private static final Log LOG = LogFactory.getLog(LoggerUDF2.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length!=1)
            throw new UDFArgumentLengthException("TestUDF takes 1 argument!");

        try {

            mbb1 = (SettableStructObjectInspector) objectInspectors[0];

            LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ " + mbb1.getAllStructFieldRefs().toString());

            boolean check = checking.mbb(mbb1);

            if (check) {
                LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ " + check);
            } else {
                throw new UDFArgumentException("Wrong box structure (var names)");
            }
        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

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
