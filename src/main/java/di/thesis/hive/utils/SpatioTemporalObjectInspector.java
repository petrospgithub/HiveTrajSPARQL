package di.thesis.hive.utils;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

public class SpatioTemporalObjectInspector {

    public StandardStructObjectInspector PointObjectInspector() {
        ArrayList<String> structFieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

        // fill struct field names
        //longitude
        structFieldNames.add("longitude");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        // latitude
        structFieldNames.add("latitude");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        structFieldNames.add("timestamp");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        StandardStructObjectInspector si = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,
                structFieldObjectInspectors);
        return si;
    }

    public StandardStructObjectInspector JavaPointObjectInspector() {
        ArrayList<String> structFieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

        // fill struct field names
        //longitude
        structFieldNames.add("longitude");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
        // latitude
        structFieldNames.add("latitude");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
        structFieldNames.add("timestamp");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);

        StandardStructObjectInspector si = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,
                structFieldObjectInspectors);
        return si;
    }

    public SettableStructObjectInspector MbbObjectInspector() {

        /*
        TODO an den leitourgei auto allazw ton orismo tou MBB table kai bazw mono ena struct me 6 pedia...
         */

        PrimitiveObjectInspector minx= PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        PrimitiveObjectInspector maxx=PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

        PrimitiveObjectInspector miny=PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        PrimitiveObjectInspector maxy=PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

        PrimitiveObjectInspector mint=PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        PrimitiveObjectInspector maxt=PrimitiveObjectInspectorFactory.writableLongObjectInspector;

        ArrayList<String> structFieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

        structFieldNames.add("minx");
        structFieldObjectInspectors.add(minx);

        structFieldNames.add("maxx");
        structFieldObjectInspectors.add(maxx);

        structFieldNames.add("miny");
        structFieldObjectInspectors.add(miny);

        structFieldNames.add("maxy");
        structFieldObjectInspectors.add(maxy);

        structFieldNames.add("mint");
        structFieldObjectInspectors.add(mint);

        structFieldNames.add("maxt");
        structFieldObjectInspectors.add(maxt);

        StandardStructObjectInspector si = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,
                structFieldObjectInspectors);

        return si;
    }

    public SettableStructObjectInspector MbbIDObjectInspector() {

        /*
        TODO an den leitourgei auto allazw ton orismo tou MBB table kai bazw mono ena struct me 6 pedia...
         */

        PrimitiveObjectInspector id=PrimitiveObjectInspectorFactory.writableLongObjectInspector;


        PrimitiveObjectInspector minx= PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        PrimitiveObjectInspector maxx=PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

        PrimitiveObjectInspector miny=PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        PrimitiveObjectInspector maxy=PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

        PrimitiveObjectInspector mint=PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        PrimitiveObjectInspector maxt=PrimitiveObjectInspectorFactory.writableLongObjectInspector;

        ArrayList<String> structFieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

        structFieldNames.add("minx");
        structFieldObjectInspectors.add(minx);

        structFieldNames.add("maxx");
        structFieldObjectInspectors.add(maxx);

        structFieldNames.add("miny");
        structFieldObjectInspectors.add(miny);

        structFieldNames.add("maxy");
        structFieldObjectInspectors.add(maxy);

        structFieldNames.add("mint");
        structFieldObjectInspectors.add(mint);

        structFieldNames.add("maxt");
        structFieldObjectInspectors.add(maxt);

        StandardStructObjectInspector si = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,
                structFieldObjectInspectors);

        return si;
    }

    public StandardListObjectInspector TrajectoryObjectInspector() {

        StandardStructObjectInspector pointst=PointObjectInspector();

        return ObjectInspectorFactory.getStandardListObjectInspector(pointst);

    }

    public StandardListObjectInspector JavaTrajectoryObjectInspector() {

        StandardStructObjectInspector pointst=JavaPointObjectInspector();

        return ObjectInspectorFactory.getStandardListObjectInspector(pointst);

    }
}
