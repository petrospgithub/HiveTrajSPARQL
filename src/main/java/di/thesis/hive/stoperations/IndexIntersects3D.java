package di.thesis.hive.stoperations;

import di.thesis.hive.test.LoggerPolygon;
import di.thesis.indexing.spatiotemporaljts.STRtree3D;
import di.thesis.indexing.types.EnvelopeST;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import utils.checking;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

public class IndexIntersects3D extends GenericUDTF {

    private static final Log LOG = LogFactory.getLog(LoggerPolygon.class.getName());


    private SettableStructObjectInspector queryIO=null;
    private BinaryObjectInspector treeIO=null;

    private HiveDecimalObjectInspector minx_tolerance;
    private HiveDecimalObjectInspector maxx_tolerance;

    private HiveDecimalObjectInspector miny_tolerance;
    private HiveDecimalObjectInspector maxy_tolerance;

    private IntObjectInspector mint_tolerance;
    private IntObjectInspector maxt_tolerance;

    private WritableLongObjectInspector partidOI;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        if (objectInspectors.length!=9)
            throw new UDFArgumentLengthException("ST_IndexIntersects only takes 8 arguments!");

        try {
            queryIO = (SettableStructObjectInspector) objectInspectors[0];
            treeIO = (BinaryObjectInspector) objectInspectors[1];

            ObjectInspector minxOI = objectInspectors[2];
            ObjectInspector maxxOI = objectInspectors[3];

            ObjectInspector minyOI = objectInspectors[4];
            ObjectInspector maxyOI = objectInspectors[5];

            ObjectInspector mintOI = objectInspectors[6];
            ObjectInspector maxtOI = objectInspectors[7];


            minx_tolerance = (HiveDecimalObjectInspector) minxOI;
            maxx_tolerance = (HiveDecimalObjectInspector) maxxOI;

            miny_tolerance = (HiveDecimalObjectInspector) minyOI;
            maxy_tolerance = (HiveDecimalObjectInspector) maxyOI;

            mint_tolerance = (IntObjectInspector) mintOI;
            maxt_tolerance = (IntObjectInspector) maxtOI;

            partidOI=(WritableLongObjectInspector)objectInspectors[8];

            boolean check = checking.mbb(queryIO);

            if (!check) {
                throw new UDFArgumentException("Invalid box structure (var names)");
            }
            ArrayList<String> fieldNames = new ArrayList<String>();
            ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
            fieldNames.add("pid");
            fieldNames.add("trajectory_id");
            //fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                    fieldOIs);

        } catch (Exception e) {
            throw new UDFArgumentException(e);
        }

    }

    private transient final Object[] forwardMapObj = new Object[2];

    @Override
    public void process(Object[] objects) throws HiveException {

       // LOG.warn("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ " +  objects[1].getClass().getCanonicalName() );

        BytesWritable tree=treeIO.getPrimitiveWritableObject(objects[1]);

        long pid=partidOI.get(objects[8]);

        //byte[] tree=(byte[])objects[1];

        //ArrayList<LongWritable> result = new ArrayList<>();

        try {

         double min_ext_lon = minx_tolerance.getPrimitiveJavaObject(objects[2]).doubleValue();
         double max_ext_lon = maxx_tolerance.getPrimitiveJavaObject(objects[3]).doubleValue();

         double min_ext_lat = miny_tolerance.getPrimitiveJavaObject(objects[4]).doubleValue();
         double max_ext_lat = maxy_tolerance.getPrimitiveJavaObject(objects[5]).doubleValue();

          long min_ext_ts = mint_tolerance.get(objects[6]);
          long max_ext_ts = maxt_tolerance.get(objects[7]);

            if (min_ext_lon < 0 ||
                    max_ext_lon < 0 || min_ext_lat < 0 || max_ext_lat < 0 || min_ext_ts < 0 || max_ext_ts < 0) {
                throw new RuntimeException("Extend parameters must be posititve!");
            }
/*
            double mbb1_minlon = ((double) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("minx"))));
            double mbb1_maxlon = ((double) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("maxx"))));

            double mbb1_minlat = ((double) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("miny"))));
            double mbb1_maxlat = ((double) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("maxy"))));

            long mbb1_mints = ((long) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("mint"))));
            long mbb1_maxts = ((long) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("maxt"))));
*/

            double mbb1_minlon = ((DoubleWritable) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("minx")))).get();
            double mbb1_maxlon = ((DoubleWritable) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("maxx")))).get();

            double mbb1_minlat = ((DoubleWritable) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("miny")))).get();
            double mbb1_maxlat = ((DoubleWritable) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("maxy")))).get();

            long mbb1_mints = ((LongWritable) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("mint")))).get();
            long mbb1_maxts = ((LongWritable) (queryIO.getStructFieldData(objects[0], queryIO.getStructFieldRef("maxt")))).get();

            STRtree3D retrievedObject = SerializationUtils.deserialize(tree.getBytes());

            EnvelopeST env = new EnvelopeST(mbb1_minlon - min_ext_lon, mbb1_maxlon + max_ext_lon,
                    mbb1_minlat - min_ext_lat, mbb1_maxlat + max_ext_lat,
                    mbb1_mints - min_ext_ts, mbb1_maxts + max_ext_ts);

            List tree_results = retrievedObject.queryID(env);

            for (int i = 0; i < tree_results.size(); i++) {

                Long entry = (Long) tree_results.get(i);
                forwardMapObj[0]=new LongWritable(pid);
                forwardMapObj[1] = new LongWritable(entry);
                forward(forwardMapObj);
            }
        } catch (Exception e) {
            throw new HiveException(e);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
