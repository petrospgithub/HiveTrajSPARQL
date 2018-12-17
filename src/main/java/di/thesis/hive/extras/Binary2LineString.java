package di.thesis.hive.extras;

import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import di.thesis.indexing.types.EnvelopeST;
import di.thesis.indexing.types.PointST;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import utils.SerDerUtil;
import utils.checking;

public class Binary2LineString extends GenericUDF {

    private BinaryObjectInspector b;
   // private SettableStructObjectInspector structOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length!=1)
            throw new UDFArgumentLengthException("Trajectory2Linestring only takes 1 argument: Trajectory");

        try {

            b = (BinaryObjectInspector) arguments[0];
           // structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

            /*
            boolean check= checking.point(structOI);

            if(!check){
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }
*/

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
        BytesWritable trajB=b.getPrimitiveWritableObject(deferredObjects[0].get());
        PointST[] trajectory = SerDerUtil.trajectory_deserialize(trajB.getBytes());


        try {
            Polyline linestring = new Polyline();
            lon = trajectory[0].getLongitude();
            lat = trajectory[0].getLatitude();

            linestring.startPath(lon, lat);
            for (int i = 1; i < trajectory.length; i++) {
                lon = trajectory[i].getLongitude();
                lat = trajectory[i].getLatitude();

                linestring.lineTo(lon, lat);
            }
            OGCGeometry ogcObj = OGCGeometry.createFromEsriGeometry(linestring, SpatialReference.create(3857));
            return new Text(ogcObj.asText());
        } catch (RuntimeException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }
}

