package di.thesis.hive.spatialIndex;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.OperatorImportFromESRIShape;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import di.thesis.indexing.spatialextension.STRtreeObjID;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class SP_IndexIntersects extends GenericUDF {

    private BinaryObjectInspector queryIO=null;
    private BinaryObjectInspector treeIO=null;
    private final static Logger LOGGER = Logger.getLogger(SP_IndexIntersects.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentLengthException(
                    "The function takes exactly 2 arguments.");
        }

        queryIO=(BinaryObjectInspector) objectInspectors[0];
        treeIO=(BinaryObjectInspector) objectInspectors[1];

        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                        .writableLongObjectInspector);
    }


    @Override
    public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {

        BytesWritable query=queryIO.getPrimitiveWritableObject(arguments[0].get());
        BytesWritable tree=treeIO.getPrimitiveWritableObject(arguments[1].get());
        ArrayList<LongWritable> result = new ArrayList<>();

        ByteArrayInputStream bis = new ByteArrayInputStream(tree.getBytes());
        ObjectInput in=null;

        try {
            in = new ObjectInputStream(bis);
            STRtreeObjID o = (STRtreeObjID) in.readObject();

            ByteBuffer bb = ByteBuffer.wrap(query.getBytes());

            byte[] byteArr = query.getBytes();
            int SIZE_WKID = 4;
            int SIZE_TYPE = 1;
            int offset = SIZE_WKID + SIZE_TYPE;
            ByteBuffer shapeBuffer = ByteBuffer.wrap(byteArr, offset, byteArr.length - offset).slice().order(ByteOrder.LITTLE_ENDIAN);
            int wkid=bb.getInt(0);
            Geometry esriGeom = OperatorImportFromESRIShape.local().execute(0, Geometry.Type.Unknown, shapeBuffer);

            SpatialReference spatialReference = SpatialReference.create(wkid);

            OGCGeometry ogc=OGCGeometry.createFromEsriGeometry(esriGeom, spatialReference);
            com.vividsolutions.jts.geom.Geometry jtsGeom=new WKTReader().read(ogc.asText());
            jtsGeom.setSRID(wkid);

            List tree_results=o.queryID(jtsGeom.getEnvelopeInternal());

            for (int i=0; i<tree_results.size(); i++) {

                Integer entry=(Integer)((AbstractMap.SimpleImmutableEntry)tree_results).getKey();

                result.add(new LongWritable(entry));
            }

            return result;

        } catch (IOException | ClassNotFoundException | ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 2);
        return "IndexIntersects(" + children[0] + ", " + children[1] + ")";
    }

}

