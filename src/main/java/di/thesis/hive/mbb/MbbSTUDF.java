package di.thesis.hive.mbb;

import di.thesis.hive.utils.SpatioTemporalObjectInspector;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import utils.checking;

public class MbbSTUDF extends GenericUDF{

    private ListObjectInspector listOI;
    private SettableStructObjectInspector structOI;

    private DoubleObjectInspector min_longitude=null;
    private DoubleObjectInspector max_longitude=null;

    private DoubleObjectInspector min_latitude=null;
    private DoubleObjectInspector max_latitude=null;

    private LongObjectInspector min_timestamp=null;
    private LongObjectInspector max_timestamp=null;



    private int mode;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length == 1 ) {
            try {
                listOI = (ListObjectInspector) objectInspectors[0];
                structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

                boolean check= checking.point(structOI);

                if(!check){
                    throw new UDFArgumentException("Invalid traj points structure (var names)");
                }

                mode=1;

            }catch (RuntimeException e) {
                throw new UDFArgumentException(e);
            }
        } else if (objectInspectors.length == 6) {
            try {

                min_longitude = (WritableConstantDoubleObjectInspector) objectInspectors[0];
                max_longitude = (WritableConstantDoubleObjectInspector) objectInspectors[1];

                min_latitude = (WritableConstantDoubleObjectInspector) objectInspectors[2];
                max_latitude = (WritableConstantDoubleObjectInspector) objectInspectors[3];

                min_timestamp = (WritableConstantLongObjectInspector) objectInspectors[4];
                max_timestamp = (WritableConstantLongObjectInspector) objectInspectors[5];

                mode = 6;
            } catch (RuntimeException e) {
                throw new UDFArgumentException(e);
            }
        } else {
            throw new UDFArgumentLengthException("MbbSTUDF only takes 1 argument or 6 arguments: Trajectory, explicit fields");
        }

        return new SpatioTemporalObjectInspector().MbbObjectInspector();
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (mode==1) {
            return Mbb_definiton(deferredObjects);
        } else if (mode==6) {
            return Mbb_definiton(
                    (double)min_longitude.getPrimitiveJavaObject(deferredObjects[0].get()),
                    (double)max_longitude.getPrimitiveJavaObject(deferredObjects[1].get()),
                    (double)min_latitude.getPrimitiveJavaObject(deferredObjects[2].get()),
                    (double)max_latitude.getPrimitiveJavaObject(deferredObjects[3].get()),
                    (long)min_timestamp.getPrimitiveJavaObject(deferredObjects[4].get()),
                    (long)max_timestamp.getPrimitiveJavaObject(deferredObjects[5].get())
                    );
        } else {
            throw new RuntimeException("Invalid input");
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }

    private Object Mbb_definiton(double min_lon, double max_lon, double min_lat, double max_lat,
                                 long min_ts, long max_ts) {
        Object[] ret=new Object[6];
        ret[0]=new DoubleWritable(min_lon);
        ret[1]=new DoubleWritable(max_lon);

        ret[2]=new DoubleWritable(min_lat);
        ret[3]=new DoubleWritable(max_lat);

        ret[4]=new LongWritable(min_ts);
        ret[5]=new LongWritable(max_ts);


        return ret;
    }

    private Object Mbb_definiton(DeferredObject[] deferredObjects) {
        try {
            int last = listOI.getListLength(deferredObjects[0].get()) - 1;

            Object traj=deferredObjects[0].get();

            long min_ts = (long) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("timestamp")));
            long max_ts = (long) (structOI.getStructFieldData(listOI.getListElement(traj, last), structOI.getStructFieldRef("timestamp")));

            double lon;
            double lat;

            double min_lon = (double) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("longitude")));
            double min_lat = (double) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("latitude")));
            double max_lon = (double) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("longitude")));
            double max_lat = (double) (structOI.getStructFieldData(listOI.getListElement(traj, 0), structOI.getStructFieldRef("latitude")));

            for (int i = 1; i < last; i++) {
                lon = (double) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("longitude")));
                lat = (double) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("latitude")));

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

            Object[] ret=new Object[6];
            ret[0]=new DoubleWritable(min_lon);
            ret[1]=new DoubleWritable(max_lon);

            ret[2]=new DoubleWritable(min_lat);
            ret[3]=new DoubleWritable(max_lat);

            ret[4]=new LongWritable(min_ts);
            ret[5]=new LongWritable(max_ts);

            return ret;
        } catch (HiveException e) {
            throw new RuntimeException(e);
        }

    }
}
