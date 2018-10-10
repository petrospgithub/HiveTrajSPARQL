package di.thesis.hive.test;

import com.vividsolutions.jts.geom.Polygon;
import di.thesis.indexing.types.EnvelopeST;
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
import utils.checking;

public class LoggerPolygon extends GenericUDF {

    // private ListObjectInspector listOI;
    private SettableStructObjectInspector mbb1;
    private static final Log LOG = LogFactory.getLog(LoggerPolygon.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1)
            throw new UDFArgumentLengthException("StartPoint only takes 1 argument: Trajectory");

        try {

            mbb1 = (SettableStructObjectInspector) objectInspectors[0];
            boolean check = checking.mbb(mbb1);

            if (check) {
                LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ " + check);
            } else {
                throw new UDFArgumentException("Wrong traj points structure (var names)");
            }

        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        double mbb1_minlon = (double) (mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("minx")));
        double mbb1_maxlon = (double) (mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("maxx")));

        double mbb1_minlat = (double) (mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("miny")));
        double mbb1_maxlat = (double) (mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("maxy")));

        long mbb1_mints = (long) (mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("mint")));
        long mbb1_maxts = (long) (mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("maxt")));


        Polygon poly = new EnvelopeST(mbb1_minlon, mbb1_maxlon, mbb1_minlat, mbb1_maxlat, mbb1_mints, mbb1_maxts).jtsGeom();

        LOG.warn("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ " + poly);
        LOG.warn("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ " + poly.getCoordinates()[0]);

        LOG.warn("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ " + poly.getGeometryN(0));
        LOG.warn("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ " + poly.getGeometryN(1));


        return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}