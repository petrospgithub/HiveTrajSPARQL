package di.thesis.hive.extras;

import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import utils.checking;

public class Trajectory2Linestring extends GenericUDF {

    private ListObjectInspector listOI;
    private SettableStructObjectInspector structOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length!=1)
            throw new UDFArgumentLengthException("StartPoint only takes 1 argument: Trajectory");

        try {

            listOI = (ListObjectInspector) arguments[0];
            structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            boolean check= checking.point(structOI);

            if(!check){
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }


        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector; //oti einai edw gurnaei to evaluate
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        double lon;
        double lat;

        Object traj=deferredObjects[0].get();

        try {
            Polyline linestring = new Polyline();
            lon = (double) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("longitude")));
            lat = (double) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("latitude")));
            linestring.startPath(lon, lat);
            for (int i = 1; i < listOI.getListLength(deferredObjects[0].get()); i++) {
                lon = (double) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("longitude")));
                lat = (double) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("latitude")));

                linestring.lineTo(lon, lat);
            }
            OGCGeometry ogcObj = OGCGeometry.createFromEsriGeometry(linestring, SpatialReference.create(4326));
            return new Text(ogcObj.asGeoJson());
        } catch (RuntimeException e) {
            throw e;
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }
}
