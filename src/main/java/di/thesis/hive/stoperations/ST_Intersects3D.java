package di.thesis.hive.stoperations;

import di.thesis.indexing.stOperators.Intersects;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import utils.checking;

public class ST_Intersects3D extends GenericUDF{

    private ListObjectInspector listOI;
    private SettableStructObjectInspector structOI;

    private SettableStructObjectInspector mbb1;
    private SettableStructObjectInspector mbb2;

    private HiveDecimalObjectInspector minx_tolerance;
    private HiveDecimalObjectInspector maxx_tolerance;

    private HiveDecimalObjectInspector miny_tolerance;
    private HiveDecimalObjectInspector maxy_tolerance;

    private IntObjectInspector mint_tolerance;
    private IntObjectInspector maxt_tolerance;


    private int mode;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        if (objectInspectors.length!=8)
            throw new UDFArgumentLengthException("ST_Intersects3D only takes 8 arguments!");

        ObjectInspector a = objectInspectors[0];
        ObjectInspector b = objectInspectors[1];

        ObjectInspector minxOI = objectInspectors[2];
        ObjectInspector maxxOI = objectInspectors[3];

        ObjectInspector minyOI = objectInspectors[4];
        ObjectInspector maxyOI = objectInspectors[5];

        ObjectInspector mintOI = objectInspectors[6];
        ObjectInspector maxtOI = objectInspectors[7];


        try {

            minx_tolerance=(HiveDecimalObjectInspector) minxOI;
            maxx_tolerance=(HiveDecimalObjectInspector) maxxOI;

            miny_tolerance=(HiveDecimalObjectInspector) minyOI;
            maxy_tolerance=(HiveDecimalObjectInspector) maxyOI;

            mint_tolerance=(IntObjectInspector) mintOI;
            maxt_tolerance=(IntObjectInspector) maxtOI;

            if (a instanceof ListObjectInspector && b instanceof SettableStructObjectInspector) {

                listOI = (ListObjectInspector) a;
                structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

                mbb1=(SettableStructObjectInspector)b;

                boolean check = checking.point(structOI);
                boolean check2 = checking.mbb(mbb1);

                if(!check || !check2){
                    throw new UDFArgumentException("Invalid vatiables structure (var names)");
                }
                mode=1;
            } else if (a instanceof SettableStructObjectInspector && b instanceof ListObjectInspector) {
                listOI = (ListObjectInspector) b;
                structOI=(SettableStructObjectInspector)listOI.getListElementObjectInspector();

                mbb1=(SettableStructObjectInspector)a;

                boolean check = checking.point(structOI);
                boolean check2 = checking.mbb(mbb1);

                if(!check || !check2){
                    throw new UDFArgumentException("Invalid vatiables structure (var names)");
                }

                mode=2;
            } else if (a instanceof SettableStructObjectInspector  && b instanceof SettableStructObjectInspector) {

                mbb1=(SettableStructObjectInspector)a;
                mbb2=(SettableStructObjectInspector)b;

                boolean check = checking.mbb((SettableStructObjectInspector)b);
                boolean check2 = checking.mbb((SettableStructObjectInspector)a);

                if(!check || !check2){
                    throw new UDFArgumentException("Invalid traj points structure (var names)");
                }

                mode=3;
            } else {
                throw new UDFArgumentException("Invalid traj points structure (var names)");
            }


        } catch (RuntimeException e) {
            throw new UDFArgumentException(e);
        }

        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {


        double min_ext_lon=minx_tolerance.getPrimitiveJavaObject(deferredObjects[2].get()).doubleValue();
        double max_ext_lon=maxx_tolerance.getPrimitiveJavaObject(deferredObjects[3].get()).doubleValue();

        double min_ext_lat=miny_tolerance.getPrimitiveJavaObject(deferredObjects[4].get()).doubleValue();
        double max_ext_lat=maxy_tolerance.getPrimitiveJavaObject(deferredObjects[5].get()).doubleValue();

        long min_ext_ts=mint_tolerance.get(deferredObjects[6].get());
        long max_ext_ts=maxt_tolerance.get(deferredObjects[7].get());


        if(min_ext_lon<0 ||
                max_ext_lon<0 || min_ext_lat<0 || max_ext_lat<0 || min_ext_ts<0 || max_ext_ts<0){
            throw new RuntimeException("Extend parameters must be posititve!");
        }

        if (mode==1) {
            return new BooleanWritable(mbb_overlap_trajectory(deferredObjects[1], deferredObjects[0], min_ext_lon, max_ext_lon, min_ext_lat, max_ext_lat, min_ext_ts, max_ext_ts));
        } else if (mode==2) {
            return new BooleanWritable(mbb_overlap_trajectory(deferredObjects[0], deferredObjects[1], min_ext_lon, max_ext_lon, min_ext_lat, max_ext_lat, min_ext_ts, max_ext_ts));
        }
        else if (mode==3) {

            double mbb1_minlon=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("minx")))).get() - min_ext_lon;
            double mbb1_maxlon=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("maxx")))).get() + max_ext_lon;

            double mbb1_minlat=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("miny")))).get() - min_ext_lat;
            double mbb1_maxlat=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("maxy")))).get() + max_ext_lat;

            long mbb1_mints=  ((LongWritable)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("mint")))).get() - min_ext_ts;
            long mbb1_maxts=  ((LongWritable)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("maxt")))).get() - max_ext_ts;

/***********************************************************************************************************************************/
            double mbb2_minlon=  ((DoubleWritable)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("minx")))).get();
            double mbb2_maxlon=  ((DoubleWritable)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("maxx")))).get();

            double mbb2_minlat=  ((DoubleWritable)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("miny")))).get();
            double mbb2_maxlat=  ((DoubleWritable)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("maxy")))).get();

            long mbb2_mints=  ((LongWritable)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("mint")))).get();
            long mbb2_maxts=  ((LongWritable)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("maxt")))).get();

            boolean ret= Intersects.apply(mbb1_minlon,mbb1_maxlon,mbb1_minlat,mbb1_maxlat,mbb1_mints,mbb1_maxts,
                    mbb2_minlon,mbb2_maxlon,mbb2_minlat,mbb2_maxlat,mbb2_mints,mbb2_maxts);

            return new BooleanWritable(ret);
        }
        return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "foo";
    }

    private boolean mbb_overlap_trajectory(DeferredObject deferredObjects_mbb, DeferredObject deferredObjects_trajectory , double min_ext_lon, double max_ext_lon,
                                           double min_ext_lat, double max_ext_lat, long min_ext_ts, long max_ext_ts) {
        try {

            /*mbb1*/
            double mbb1_minlon=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("minx")))).get() - min_ext_lon;
            double mbb1_maxlon=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("maxx")))).get() + max_ext_lon;

            double mbb1_minlat=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("miny")))).get() - min_ext_lat;
            double mbb1_maxlat=  ((DoubleWritable)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("maxy")))).get() + max_ext_lat;

            long mbb1_mints=  ((LongWritable)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("mint")))).get() - min_ext_ts;
            long mbb1_maxts=  ((LongWritable)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("maxt")))).get() - max_ext_ts;
            /*mbb1*/

            int trajectory_length=listOI.getListLength(deferredObjects_trajectory.get());
            double trajectory_longitude;
            double trajectory_latitude;
            long trajectory_timestamp;
            boolean result=false;

            Object traj=deferredObjects_trajectory.get();

            for (int i=0; i<trajectory_length-1; i++) {

                trajectory_longitude = ((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("longitude")))).get();
                trajectory_latitude = ((DoubleWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("latitude")))).get();
                trajectory_timestamp = ((LongWritable) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("timestamp")))).get();

                if (
                        Intersects.apply(mbb1_minlon,mbb1_maxlon,mbb1_minlat,mbb1_maxlat,mbb1_mints,mbb1_maxts,
                                trajectory_longitude,trajectory_latitude,trajectory_timestamp)
                ) {
                    result= true;
                    break;
                }
            }
            return result;
        } catch (HiveException e) {
            throw new RuntimeException(e);
        }
    }
}
