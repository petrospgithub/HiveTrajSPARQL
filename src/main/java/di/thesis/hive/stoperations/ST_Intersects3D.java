package di.thesis.hive.stoperations;

import di.thesis.indexing.stOperators.Intersects;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import utils.checking;

public class ST_Intersects3D extends GenericUDF{

    private ListObjectInspector listOI;
    private SettableStructObjectInspector structOI;

    private SettableStructObjectInspector mbb1;
    private SettableStructObjectInspector mbb2;

    private DoubleObjectInspector minx_extend;
    private DoubleObjectInspector maxx_extend;

    private DoubleObjectInspector miny_extend;
    private DoubleObjectInspector maxy_extend;

    private IntObjectInspector mint_extend;
    private IntObjectInspector maxt_extend;


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

            minx_extend=(DoubleObjectInspector) minxOI;
            maxx_extend=(DoubleObjectInspector) maxxOI;

            miny_extend=(DoubleObjectInspector) minyOI;
            maxy_extend=(DoubleObjectInspector) maxyOI;

            mint_extend=(IntObjectInspector) mintOI;
            maxt_extend=(IntObjectInspector) maxtOI;

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

                boolean check = checking.point((SettableStructObjectInspector)b);
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


        double min_ext_lon=minx_extend.get(deferredObjects[2]);
        double max_ext_lon=maxx_extend.get(deferredObjects[3]);

        double min_ext_lat=miny_extend.get(deferredObjects[4]);
        double max_ext_lat=maxy_extend.get(deferredObjects[5]);

        long min_ext_ts=mint_extend.get(deferredObjects[6]);
        long max_ext_ts=maxt_extend.get(deferredObjects[7]);


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

            double mbb1_minlon=  (double)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("minx"))) - min_ext_lon;
            double mbb1_maxlon=  (double)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("maxx"))) + max_ext_lon;

            double mbb1_minlat=  (double)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("miny"))) - min_ext_lat;
            double mbb1_maxlat=  (double)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("maxy"))) + max_ext_lat;

            long mbb1_mints=  (long)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("mint"))) - min_ext_ts;
            long mbb1_maxts=  (long)(mbb1.getStructFieldData(deferredObjects[0].get(), mbb1.getStructFieldRef("maxt"))) - max_ext_ts;

/***********************************************************************************************************************************/
            double mbb2_minlon=  (double)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("minx")));
            double mbb2_maxlon=  (double)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("maxx")));

            double mbb2_minlat=  (double)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("miny")));
            double mbb2_maxlat=  (double)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("maxy")));

            long mbb2_mints=  (long)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("mint")));
            long mbb2_maxts=  (long)(mbb2.getStructFieldData(deferredObjects[1].get(), mbb2.getStructFieldRef("maxt")));

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
            double mbb1_minlon=  (double)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("minx"))) - min_ext_lon;
            double mbb1_maxlon=  (double)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("maxx"))) + max_ext_lon;

            double mbb1_minlat=  (double)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("miny"))) - min_ext_lat;
            double mbb1_maxlat=  (double)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("maxy"))) + max_ext_lat;

            long mbb1_mints=  (long)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("mint"))) - min_ext_ts;
            long mbb1_maxts=  (long)(mbb1.getStructFieldData(deferredObjects_mbb.get(), mbb1.getStructFieldRef("maxt"))) - max_ext_ts;
            /*mbb1*/

            int trajectory_length=listOI.getListLength(deferredObjects_trajectory.get());
            double trajectory_longitude;
            double trajectory_latitude;
            long trajectory_timestamp;
            boolean result=false;

            Object traj=deferredObjects_trajectory.get();

            for (int i=0; i<trajectory_length-1; i++) {

                trajectory_longitude = (double) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("longitude")));
                trajectory_latitude = (double) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("latitude")));
                trajectory_timestamp = (long) (structOI.getStructFieldData(listOI.getListElement(traj, i), structOI.getStructFieldRef("timestamp")));

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
