package di.thesis.hive.mbb;

import di.thesis.hive.utils.SpatioTemporalObjectInspector;
import di.thesis.indexing.types.EnvelopeST;
import di.thesis.indexing.types.PointST;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import utils.SerDerUtil;
import utils.checking;

public class MbbSTUDFBinary extends GenericUDF {

    // private ListObjectInspector listOI;
    //private SettableStructObjectInspector structOI;

    private BinaryObjectInspector traj;

    private HiveDecimalObjectInspector min_longitude = null;
    private HiveDecimalObjectInspector max_longitude = null;

    private HiveDecimalObjectInspector min_latitude = null;
    private HiveDecimalObjectInspector max_latitude = null;

    private LongObjectInspector min_timestamp = null;
    private LongObjectInspector max_timestamp = null;


    private int mode;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length == 1) {
            try {
                traj = (BinaryObjectInspector) objectInspectors[0];
            } catch (RuntimeException e) {
                throw new UDFArgumentException(e);
            }
        } else if (objectInspectors.length == 6) {
            try {

                min_longitude = (WritableConstantHiveDecimalObjectInspector) objectInspectors[0];
                max_longitude = (WritableConstantHiveDecimalObjectInspector) objectInspectors[1];

                min_latitude = (WritableConstantHiveDecimalObjectInspector) objectInspectors[2];
                max_latitude = (WritableConstantHiveDecimalObjectInspector) objectInspectors[3];

                min_timestamp = (WritableConstantLongObjectInspector) objectInspectors[4];
                max_timestamp = (WritableConstantLongObjectInspector) objectInspectors[5];

                mode = 6;
            } catch (RuntimeException e) {
                throw new UDFArgumentException(e);
            }
        } else {
            throw new UDFArgumentLengthException("MbbSTUDF only takes 1 argument or 6 arguments: Trajectory or explicit fields");
        }

        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (mode == 1) {
            return Mbb_definiton(deferredObjects);
        } else if (mode == 6) {


            EnvelopeST envelopeST=new EnvelopeST(min_longitude.getPrimitiveJavaObject(deferredObjects[0].get()).doubleValue(),
                    max_longitude.getPrimitiveJavaObject(deferredObjects[1].get()).doubleValue(),
                    min_latitude.getPrimitiveJavaObject(deferredObjects[2].get()).doubleValue(),
                    max_latitude.getPrimitiveJavaObject(deferredObjects[3].get()).doubleValue(),
                    (long) min_timestamp.getPrimitiveJavaObject(deferredObjects[4].get()),
                    (long) max_timestamp.getPrimitiveJavaObject(deferredObjects[5].get())
                    );

            return SerDerUtil.mbb_serialize(envelopeST);

        } else {
            throw new RuntimeException("Invalid input");
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }

    private Object Mbb_definiton(DeferredObject[] deferredObjects) {
        try {

            BytesWritable query=traj.getPrimitiveWritableObject(deferredObjects[0]);
            PointST[] trajectory=SerDerUtil.trajectory_deserialize(query.getBytes());

            int last = trajectory.length-1;

           // Object traj = deferredObjects[0].get();

            long min_ts = trajectory[0].getTimestamp();//((LongWritable) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("timestamp")))).get();
            long max_ts = trajectory[last].getTimestamp();//((LongWritable) (structOI.getStructFieldData(listOI.getListElement(traj, last), structOI.getStructFieldRef("timestamp")))).get();

            double lon;
            double lat;

            double min_lon = trajectory[0].getLongitude();//((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("longitude")))).get();
            double min_lat = trajectory[0].getLatitude();//((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("latitude")))).get();
            double max_lon = trajectory[0].getLongitude();//((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("longitude")))).get();
            double max_lat = trajectory[0].getLatitude();//((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("latitude")))).get();

            for (int i = 1; i < last; i++) {
                lon = trajectory[i].getLongitude();//((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("longitude")))).get();
                lat = trajectory[i].getLatitude();//((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("latitude")))).get();

                if (min_lon > lon) {
                    min_lon = lon;
                }
                if (max_lon < lon) {
                    max_lon = lon;
                }

                if (min_lat > lat) {
                    min_lat = lat;
                }
                if (max_lat < lat) {
                    max_lat = lat;
                }
            }

            /*
            Object[] ret=new Object[6];
            ret[0]=new DoubleWritable(min_lon);
            ret[1]=new DoubleWritable(max_lon);

            ret[2]=new DoubleWritable(min_lat);
            ret[3]=new DoubleWritable(max_lat);

            ret[4]=new LongWritable(min_ts);
            ret[5]=new LongWritable(max_ts);
*/

            EnvelopeST envelopeST=new EnvelopeST(min_lon, max_lon, min_lat, max_lat, min_ts, max_ts);

            return new BytesWritable(SerDerUtil.mbb_serialize(envelopeST));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
