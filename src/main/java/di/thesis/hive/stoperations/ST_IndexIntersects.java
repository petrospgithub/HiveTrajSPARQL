package di.thesis.hive.stoperations;

import com.vividsolutions.jts.io.ParseException;
import di.thesis.indexing.spatialextension.STRtreeObjID;
import di.thesis.indexing.spatiotemporaljts.STRtree3D;
import di.thesis.indexing.types.EnvelopeST;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.opengis.geometry.Envelope;
import utils.checking;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

public class ST_IndexIntersects  extends GenericUDF {

    private SettableStructObjectInspector queryIO=null;
    private BinaryObjectInspector treeIO=null;

    private DoubleObjectInspector minx_tolerance;
    private DoubleObjectInspector maxx_tolerance;

    private DoubleObjectInspector miny_tolerance;
    private DoubleObjectInspector maxy_tolerance;

    private IntObjectInspector mint_tolerance;
    private IntObjectInspector maxt_tolerance;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length!=8)
            throw new UDFArgumentLengthException("ST_IndexIntersects only takes 8 arguments!");

        try {
            queryIO=(SettableStructObjectInspector) objectInspectors[0];
            treeIO=(BinaryObjectInspector) objectInspectors[1];

            ObjectInspector minxOI = objectInspectors[2];
            ObjectInspector maxxOI = objectInspectors[3];

            ObjectInspector minyOI = objectInspectors[4];
            ObjectInspector maxyOI = objectInspectors[5];

            ObjectInspector mintOI = objectInspectors[6];
            ObjectInspector maxtOI = objectInspectors[7];


            minx_tolerance=(DoubleObjectInspector) minxOI;
            maxx_tolerance=(DoubleObjectInspector) maxxOI;

            miny_tolerance=(DoubleObjectInspector) minyOI;
            maxy_tolerance=(DoubleObjectInspector) maxyOI;

            mint_tolerance=(IntObjectInspector) mintOI;
            maxt_tolerance=(IntObjectInspector) maxtOI;

            boolean check= checking.mbb(queryIO);

            if(!check){
                throw new UDFArgumentException("Invalid box structure (var names)");
            }

            return ObjectInspectorFactory
                    .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                            .writableLongObjectInspector);
        } catch (Exception e) {
            throw new UDFArgumentException(e);
        }

    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {


        BytesWritable tree=treeIO.getPrimitiveWritableObject(deferredObjects[1].get());


        ArrayList<LongWritable> result = new ArrayList<>();

        ByteArrayInputStream bis = new ByteArrayInputStream(tree.getBytes());
        ObjectInput in=null;

        try {

            double min_ext_lon=minx_tolerance.get(deferredObjects[2]);
            double max_ext_lon=maxx_tolerance.get(deferredObjects[3]);

            double min_ext_lat=miny_tolerance.get(deferredObjects[4]);
            double max_ext_lat=maxy_tolerance.get(deferredObjects[5]);

            long min_ext_ts=mint_tolerance.get(deferredObjects[6]);
            long max_ext_ts=maxt_tolerance.get(deferredObjects[7]);

            if(min_ext_lon<0 ||
                    max_ext_lon<0 || min_ext_lat<0 || max_ext_lat<0 || min_ext_ts<0 || max_ext_ts<0){
                throw new RuntimeException("Extend parameters must be posititve!");
            }

            double mbb1_minlon=  (double)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("minx")));
            double mbb1_maxlon=  (double)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("maxx")));

            double mbb1_minlat=  (double)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("miny")));
            double mbb1_maxlat=  (double)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("maxy")));

            long mbb1_mints=  (long)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("mint")));
            long mbb1_maxts=  (long)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("maxt")));

            in = new ObjectInputStream(bis);
            STRtree3D o = (STRtree3D) in.readObject();

            EnvelopeST env=new EnvelopeST(mbb1_minlon-min_ext_lon, mbb1_maxlon+max_ext_lon,
                    mbb1_minlat-min_ext_lat, mbb1_maxlat+max_ext_lat,
                    mbb1_mints-min_ext_ts, mbb1_maxts+max_ext_ts);

            List tree_results=o.queryID(env);

            for (int i=0; i<tree_results.size(); i++) {

                Integer entry=(Integer)((AbstractMap.SimpleImmutableEntry)tree_results).getKey();

                result.add(new LongWritable(entry));
            }

            return result;

        } catch (IOException | ClassNotFoundException e) {
            throw new HiveException(e);
        }


       // return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
