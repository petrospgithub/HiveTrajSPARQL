package di.thesis.hive.similarity;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import di.thesis.indexing.distance.BoxLineDist;
import di.thesis.indexing.stOperators.Intersects;
import di.thesis.indexing.types.EnvelopeST;
import di.thesis.indexing.utils.STtoS;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import utils.checking;

public class TrajBoxUDF extends GenericUDF {

    private ListObjectInspector listOI;
    private SettableStructObjectInspector structOI;

    private SettableStructObjectInspector mbb1;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        if (objectInspectors.length!=2)
            throw new UDFArgumentLengthException("TrajBoxUDF only takes 2 arguments!");

        ObjectInspector a = objectInspectors[0];
        ObjectInspector b = objectInspectors[1];
     //   ObjectInspector c= objectInspectors[1];

        try {

           // dist=(DoubleObjectInspector) c;
            if (a instanceof ListObjectInspector && b instanceof SettableStructObjectInspector) {
                listOI = (ListObjectInspector) a;
                structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

                mbb1=(SettableStructObjectInspector)b;

                boolean check = checking.point(structOI);
                boolean check2 = checking.mbb(mbb1);

                if(!check || !check2){
                    throw new UDFArgumentException("Invalid variables structure (var names)");
                }
            } else {
                throw new UDFArgumentException("Invalid data types! Required Trajectory and Box");

            }

        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        double mbb1_minlon=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects[1].get(), mbb1.getStructFieldRef("minx")))).get();
        double mbb1_maxlon=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects[1].get(), mbb1.getStructFieldRef("maxx")))).get();

        double mbb1_minlat=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects[1].get(), mbb1.getStructFieldRef("miny")))).get();
        double mbb1_maxlat=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects[1].get(), mbb1.getStructFieldRef("maxy")))).get();

        long mbb1_mints=  ((LongWritable)(mbb1.getStructFieldData(deferredObjects[1].get(), mbb1.getStructFieldRef("mint")))).get();
        long mbb1_maxts=  ((LongWritable)(mbb1.getStructFieldData(deferredObjects[1].get(), mbb1.getStructFieldRef("maxt")))).get();


        Object traj=deferredObjects[0].get();

      //  double threshold=dist.get(deferredObjects[2].get());

        int trajectory_length=listOI.getListLength(deferredObjects[0].get());
        double trajectory_longitude;
        double trajectory_latitude;
        long trajectory_timestamp;

        double result=Double.MAX_VALUE;


        for (int i=0; i<trajectory_length-1; i++) {

            trajectory_longitude = ((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("longitude")))).get();
            trajectory_latitude = ((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("latitude")))).get();
            trajectory_timestamp = ((LongWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("timestamp")))).get();

            if (
                    Intersects.apply(mbb1_minlon,mbb1_maxlon,mbb1_minlat,mbb1_maxlat,mbb1_mints,mbb1_maxts,
                            trajectory_longitude,trajectory_latitude,trajectory_timestamp)
            ) {
                result= 0;
                break;
            }
        }

        if (result>0) {
            LineString line = STtoS.trajectory_transformation(traj, listOI, structOI);
            Polygon poly = new EnvelopeST(mbb1_minlon, mbb1_maxlon, mbb1_minlat, mbb1_maxlat, mbb1_mints, mbb1_maxts).jtsGeom();
            result= BoxLineDist.minDist(poly,line);
        }

        return new DoubleWritable(result);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
