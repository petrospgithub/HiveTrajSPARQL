package di.thesis.hive.stoperations;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import di.thesis.indexing.spatiotemporaljts.STRtree3D;
import di.thesis.indexing.types.EnvelopeST;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import utils.checking;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

public class ST_IndexIntersects  extends GenericUDF {

    private SettableStructObjectInspector queryIO=null;
    private BinaryObjectInspector treeIO=null;

    private HiveDecimalObjectInspector minx_tolerance;
    private HiveDecimalObjectInspector maxx_tolerance;

    private HiveDecimalObjectInspector miny_tolerance;
    private HiveDecimalObjectInspector maxy_tolerance;

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


            minx_tolerance=(HiveDecimalObjectInspector) minxOI;
            maxx_tolerance=(HiveDecimalObjectInspector) maxxOI;

            miny_tolerance=(HiveDecimalObjectInspector) minyOI;
            maxy_tolerance=(HiveDecimalObjectInspector) maxyOI;

            mint_tolerance=(IntObjectInspector) mintOI;
            maxt_tolerance=(IntObjectInspector) maxtOI;

            boolean check= checking.mbb(queryIO);

            if(!check){
                throw new UDFArgumentException("Invalid box structure (var names)");
            }

           // return ObjectInspectorFactory
                //    .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                      //      .writableLongObjectInspector);

             return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        } catch (Exception e) {
            throw new UDFArgumentException(e);
        }

    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {


        BytesWritable tree=treeIO.getPrimitiveWritableObject(deferredObjects[1].get());


        ArrayList<Text> result = new ArrayList<>();

       // ByteArrayInputStream bis = new ByteArrayInputStream(tree.getBytes());
       // ObjectInput in=null;

        try {

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

            double mbb1_minlon=  ((DoubleWritable)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("minx")))).get();
            double mbb1_maxlon=  ((DoubleWritable)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("maxx")))).get();

            double mbb1_minlat=  ((DoubleWritable)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("miny")))).get();
            double mbb1_maxlat=  ((DoubleWritable)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("maxy")))).get();

            long mbb1_mints=  ((LongWritable)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("mint")))).get();
            long mbb1_maxts=  ((LongWritable)(queryIO.getStructFieldData(deferredObjects[0].get(), queryIO.getStructFieldRef("maxt")))).get();
/*
            FSTObjectInput input = new FSTObjectInput(new ByteArrayInputStream(tree.getBytes()));
            STRtree3D retrievedObject = (STRtree3D)input.readObject(STRtree3D.class);
*/

            Kryo kryo = new Kryo();

            Input input = new Input(new ByteArrayInputStream(tree.getBytes()));

            STRtree3D retrievedObject = kryo.readObject(input, STRtree3D.class);
            input.close();

            result.add(new Text(retrievedObject.getClass().getName()));

/*
            EnvelopeST env=new EnvelopeST(mbb1_minlon-min_ext_lon, mbb1_maxlon+max_ext_lon,
                    mbb1_minlat-min_ext_lat, mbb1_maxlat+max_ext_lat,
                    mbb1_mints-min_ext_ts, mbb1_maxts+max_ext_ts);

            List tree_results=retrievedObject.queryID(env);

            for (int i=0; i<tree_results.size(); i++) {

                Long entry=(Long)tree_results.get(i);

                result.add(new LongWritable(entry));
            }
*/
            return result;

        } catch (Exception e) {
            throw new HiveException(e);
        }


       // return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
