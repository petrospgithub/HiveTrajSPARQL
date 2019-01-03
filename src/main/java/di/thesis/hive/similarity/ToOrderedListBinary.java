package di.thesis.hive.similarity;

import di.thesis.hive.utils.SpatioTemporalObjectInspector;
import hivemall.utils.collections.BoundedPriorityQueue;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.CommandLineUtils;
import hivemall.utils.lang.NaturalComparator;
import hivemall.utils.lang.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import scala.Tuple4;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class ToOrderedListBinary extends AbstractGenericUDAFResolver {

    private static final Log LOG = LogFactory.getLog(ToOrderedList.class.getName());


    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
            throws SemanticException {
        //   @SuppressWarnings("deprecation")

/*
        TypeInfo[] typeInfo = info.getParameters();
        ObjectInspector[] argOIs = info.getParameterObjectInspectors();
        if ((typeInfo.length == 1) || (typeInfo.length == 2 && HiveUtils.isConstString(argOIs[1]))) {
            // sort values by value itself w/o key
            if (typeInfo[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(0,
                        "Only primitive type arguments are accepted for value but "
                                + typeInfo[0].getTypeName() + " was passed as the first parameter.");
            }
        } else if ((typeInfo.length == 2)
                || (typeInfo.length == 3 && HiveUtils.isConstString(argOIs[2]))) {

            LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ input ");

            // sort values by key
            if (typeInfo[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(1,
                        "Only primitive type arguments are accepted for key but "
                                + typeInfo[1].getTypeName() + " was passed as the second parameter.");
            }
        } else {
            throw new UDFArgumentTypeException(typeInfo.length - 1,
                    "Number of arguments must be in [1, 3] including constant string for options: "
                            + typeInfo.length);
        }
        */

        return new ToOrderedListBinary.UDAFToOrderedListEvaluator();
    }

    public static class UDAFToOrderedListEvaluator extends GenericUDAFEvaluator {

        private ObjectInspector valueOI;
        private PrimitiveObjectInspector keyOI;

        private ListObjectInspector valueListOI;
        private ListObjectInspector keyListOI;

        private StructObjectInspector internalMergeOI;

        private StructField valueListField;
        private StructField keyListField;
        private StructField sizeField;
        private StructField reverseOrderField;


        @Nonnegative
        private int size;
        private boolean reverseOrder;
        private boolean sortByKey;

        protected Options getOptions() {
            Options opts = new Options();
            opts.addOption("k", true, "To top-k (positive) or tail-k (negative) ordered queue");
            opts.addOption("reverse", "reverse_order", false,
                    "Sort values by key in a reverse (e.g., descending) order [default: false]");
            return opts;
        }

        @Nonnull
        protected final CommandLine parseOptions(String optionValue) throws UDFArgumentException {
            String[] args = optionValue.split("\\s+");
            Options opts = getOptions();
            opts.addOption("help", false, "Show function help");
            CommandLine cl = CommandLineUtils.parseOptions(args, opts);

            if (cl.hasOption("help")) {
                Description funcDesc = getClass().getAnnotation(Description.class);
                final String cmdLineSyntax;
                if (funcDesc == null) {
                    cmdLineSyntax = getClass().getSimpleName();
                } else {
                    String funcName = funcDesc.name();
                    cmdLineSyntax = funcName == null ? getClass().getSimpleName()
                            : funcDesc.value().replace("_FUNC_", funcDesc.name());
                }
                StringWriter sw = new StringWriter();
                sw.write('\n');
                PrintWriter pw = new PrintWriter(sw);
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, cmdLineSyntax, null, opts,
                        HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null, true);
                pw.flush();
                String helpMsg = sw.toString();
                throw new UDFArgumentException(helpMsg);
            }

            return cl;
        }

        protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
            CommandLine cl = null;

            int optionIndex = 1;
            if (sortByKey) {
                optionIndex = 2;
            }

            int k = 0;
            boolean reverseOrder = false;
            if (argOIs.length >= optionIndex + 1) {

                String rawArgs = HiveUtils.getConstString(argOIs[2]);

                cl = parseOptions(rawArgs);

                reverseOrder = cl.hasOption("reverse_order");

                if (cl.hasOption("k")) {
                    k = Integer.parseInt(cl.getOptionValue("k"));
                    if (k == 0) {
                        throw new UDFArgumentException("`k` must be non-zero value: " + k);
                    }
                }
            }
            this.size = Math.abs(k);

            if ((k > 0 && reverseOrder) || (k < 0 && reverseOrder == false)
                    || (k == 0 && reverseOrder == false)) {
                // top-k on reverse order = tail-k on natural order (so, top-k on descending)
                this.reverseOrder = true;
            } else { // (k > 0 && reverseOrder == false) || (k < 0 && reverseOrder) || (k == 0 && reverseOrder)
                // top-k on natural order = tail-k on reverse order (so, top-k on ascending)
                this.reverseOrder = false;
            }

            return cl;
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] argOIs) throws HiveException {
            super.init(mode, argOIs);

            // initialize input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {// from original data
                // this flag will be used in `processOptions` and `iterate` (= when Mode.PARTIAL1 or Mode.COMPLETE)
                this.sortByKey = (argOIs.length == 2 && !HiveUtils.isConstString(argOIs[1]))
                        || (argOIs.length == 3 && HiveUtils.isConstString(argOIs[2]));

                //     LOG.warn("sortByKey "+sortByKey);

                if (sortByKey) {
                    this.valueOI = argOIs[0];
                    this.keyOI = HiveUtils.asPrimitiveObjectInspector(argOIs[1]);
                } else {
                    // sort values by value itself
                    //        LOG.warn("sortByKey mpike");

                    this.valueOI = HiveUtils.asPrimitiveObjectInspector(argOIs[0]);
                    this.keyOI = HiveUtils.asPrimitiveObjectInspector(argOIs[1]);
                }

                //     LOG.warn("sortByKey "+this.keyOI);
                //    LOG.warn("sortByKey "+this.valueOI);


                processOptions(argOIs);
            } else {// from partial aggregation

                StructObjectInspector soi = (StructObjectInspector) argOIs[0];
                this.internalMergeOI = soi;

                // re-extract input value OI
                this.valueListField = soi.getStructFieldRef("valueList");
                StandardListObjectInspector valueListOI = (StandardListObjectInspector) valueListField.getFieldObjectInspector();
                this.valueOI = valueListOI.getListElementObjectInspector();
                this.valueListOI = ObjectInspectorFactory.getStandardListObjectInspector(valueOI);

                // re-extract input key OI
                this.keyListField = soi.getStructFieldRef("keyList");
                StandardListObjectInspector keyListOI = (StandardListObjectInspector) keyListField.getFieldObjectInspector();
                this.keyOI = HiveUtils.asPrimitiveObjectInspector(keyListOI.getListElementObjectInspector());
                this.keyListOI = ObjectInspectorFactory.getStandardListObjectInspector(keyOI);

                this.sizeField = soi.getStructFieldRef("size");
                this.reverseOrderField = soi.getStructFieldRef("reverseOrder");

            }

            // initialize output
            final ObjectInspector outputOI;
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {// terminatePartial
                outputOI = internalMergeOI(valueOI, keyOI);
            } else {// terminate


                List<String> fieldNames = new ArrayList<String>();
                List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

                fieldNames.add("rowid");
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector));

                fieldNames.add("distances");
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));

                // LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  internalMergeOI" );
                fieldNames.add("traja");
                fieldOIs.add(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);

                fieldNames.add("trajb");
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector));


                outputOI=ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

                //outputOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
               // outputOI= PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            }

            return outputOI;
        }

        @Nonnull
        private static StructObjectInspector internalMergeOI(@Nonnull ObjectInspector valueOI,
                                                             @Nonnull PrimitiveObjectInspector keyOI) {
            List<String> fieldNames = new ArrayList<String>();
            List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

            fieldNames.add("valueList");
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(valueOI)));
            fieldNames.add("keyList");
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(keyOI)));
            fieldNames.add("size");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
            fieldNames.add("reverseOrder");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);

            SpatioTemporalObjectInspector trajOI=new SpatioTemporalObjectInspector();

            // LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  internalMergeOI" );
            fieldNames.add("traja");
          //  fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(trajOI.TrajectoryObjectInspector()));

            fieldOIs.add(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);


            fieldNames.add("trajb");
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector));


            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }

        @SuppressWarnings("deprecation")
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            UDAFToOrderedListEvaluator.QueueAggregationBuffer myagg = new UDAFToOrderedListEvaluator.QueueAggregationBuffer();
            reset(myagg);
            return myagg;
        }

        @Override
        public void reset(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            UDAFToOrderedListEvaluator.QueueAggregationBuffer myagg = (UDAFToOrderedListEvaluator.QueueAggregationBuffer) agg;
            myagg.reset(size, reverseOrder);
        }

        @Override
        public void iterate(@SuppressWarnings("deprecation") AggregationBuffer agg,
                            Object[] parameters) throws HiveException {
            if (parameters[0] == null) {
                return;
            }
            final Object value = ObjectInspectorUtils.copyToStandardObject(parameters[0], valueOI);
            final Object key = ObjectInspectorUtils.copyToStandardObject(parameters[1], keyOI);

            // final Object key;
/*
            if (sortByKey) {
                if (parameters[1] == null) {
                    return;
                }
                key = ObjectInspectorUtils.copyToStandardObject(parameters[1], keyOI);
            } else {
                // set value to key
                key = ObjectInspectorUtils.copyToStandardObject(parameters[0], valueOI);
            }
*/
            //          LOG.warn("~~~~~~~~~~~~~~~~~~~~~~~~~~~ key: "+key);
            //        LOG.warn("~~~~~~~~~~~~~~~~~~~~~~~~~~~ value: "+value);

            TupleWithKey tuple = new TupleWithKey(key, value, parameters[3], parameters[4]);

            QueueAggregationBuffer myagg = (QueueAggregationBuffer) agg;

            myagg.iterate(tuple);
        }

        @Override
        public Object terminatePartial(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            QueueAggregationBuffer myagg = (QueueAggregationBuffer) agg;

            Tuple4<List<Object>,List<Object>,Object,List<Object>> tuples = myagg.drainQueue();

            if (tuples == null) {
                return null;
            }

            List<Object> keyList = tuples._1();
            List<Object> valueList = tuples._2();

            //   List<Object> rowList = tuples._3();
            //  List<Object> trajAList = tuples._4();
            //  List<Object> trajBList = tuples._5();


            //        LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ rowList: "+tuples._3());
            //         LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ trajAList: "+tuples._4());
            //        LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ trajBList: "+tuples._5());

            // Object keyList = tuples.get();
            // Object valueList = tuples.getValue();
            //Object keyList = tuples.getKey();

            Object[] partialResult = new Object[6];
            partialResult[0] = valueList;
            partialResult[1] = keyList;
            partialResult[2] = new IntWritable(myagg.size);
            partialResult[3] = new BooleanWritable(myagg.reverseOrder);

            //     partialResult[4] = tuples._3();
            partialResult[4] = tuples._3();
            partialResult[5] = tuples._4();

            return partialResult;
        }

        @Override
        public void merge(@SuppressWarnings("deprecation") AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial == null) {
                return;
            }

            Object valueListObj = internalMergeOI.getStructFieldData(partial, valueListField);
            final List<?> valueListRaw = valueListOI.getList(HiveUtils.castLazyBinaryObject(valueListObj));
            final List<Object> valueList = new ArrayList<Object>();
            for (int i = 0, n = valueListRaw.size(); i < n; i++) {
                valueList.add(ObjectInspectorUtils.copyToStandardObject(valueListRaw.get(i),
                        valueOI));
            }
            Object keyListObj = internalMergeOI.getStructFieldData(partial, keyListField);
            final List<?> keyListRaw = keyListOI.getList(HiveUtils.castLazyBinaryObject(keyListObj));
            final List<Object> keyList = new ArrayList<Object>();
            for (int i = 0, n = keyListRaw.size(); i < n; i++) {
                keyList.add(ObjectInspectorUtils.copyToStandardObject(keyListRaw.get(i), keyOI));
            }

            Object sizeObj = internalMergeOI.getStructFieldData(partial, sizeField);
            int size = PrimitiveObjectInspectorFactory.writableIntObjectInspector.get(sizeObj);

            Object reverseOrderObj = internalMergeOI.getStructFieldData(partial, reverseOrderField);
            boolean reverseOrder = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector.get(reverseOrderObj);

            UDAFToOrderedListEvaluator.QueueAggregationBuffer myagg = (UDAFToOrderedListEvaluator.QueueAggregationBuffer) agg;
            myagg.setOptions(size, reverseOrder);

            //       LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ check " + this.internalMergeOI.getAllStructFieldRefs());

            //  LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ checkA " + internalMergeOI.getStructFieldData(partial, this.internalMergeOI.getStructFieldRef("traja")));
            //   LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ checkB " + internalMergeOI.getStructFieldData(partial, this.internalMergeOI.getStructFieldRef("trajb")));

        //    SpatioTemporalObjectInspector trajOI=new SpatioTemporalObjectInspector();
            ListObjectInspector trajListOI=ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);

/*
            Object trajAListObj = internalMergeOI.getStructFieldData(partial, this.internalMergeOI.getStructFieldRef("traja"));

            final List<?> trajAListRaw = trajListOI.getList(HiveUtils.castLazyBinaryObject(trajAListObj));
            final List<Object> trajAList = new ArrayList<Object>();

            for (int i = 0, n = trajAListRaw.size(); i < n; i++) {
                trajAList.add(ObjectInspectorUtils.copyToStandardObject(trajAListRaw.get(i),
                        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector)); // TODO check!!!
            }
*/

            Object trajAListObj = internalMergeOI.getStructFieldData(partial, this.internalMergeOI.getStructFieldRef("traja"));


            Object trajBListObj = internalMergeOI.getStructFieldData(partial, this.internalMergeOI.getStructFieldRef("trajb"));

            final List<?> trajBListRaw = trajListOI.getList(HiveUtils.castLazyBinaryObject(trajBListObj));
            final List<Object> trajBList = new ArrayList<Object>();

            for (int i = 0, n = trajBListRaw.size(); i < n; i++) {
                trajBList.add(ObjectInspectorUtils.copyToStandardObject(trajBListRaw.get(i),
                        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector));
            }

            myagg.merge(keyList, valueList, trajAListObj, trajBList);
        }

        @Override
        public Object[] terminate(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            UDAFToOrderedListEvaluator.QueueAggregationBuffer myagg = (UDAFToOrderedListEvaluator.QueueAggregationBuffer) agg;
            Tuple4<List<Object>,List<Object>,Object,List<Object>>tuples = myagg.drainQueue();
            if (tuples == null) {
                return null;
            }

            // LOG.warn("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tuples " + tuples );
            Object[] obj=new Object[4];


            obj[0]=tuples._1();
            obj[1]=tuples._2();
            obj[2]=tuples._3();
            obj[3]=tuples._4();

            //return tuples.toString();

            return obj;
        }

        static class QueueAggregationBuffer extends AbstractAggregationBuffer {

            private AbstractQueueHandler queueHandler;

            @Nonnegative
            private int size;
            private boolean reverseOrder;

            QueueAggregationBuffer() {
                super();
            }

            void reset(@Nonnegative int size, boolean reverseOrder) {
                setOptions(size, reverseOrder);
                this.queueHandler = null;
            }

            void setOptions(@Nonnegative int size, boolean reverseOrder) {
                this.size = size;
                this.reverseOrder = reverseOrder;
            }

            void iterate(@Nonnull TupleWithKey tuple) {
                if (queueHandler == null) {
                    initQueueHandler();
                }
                queueHandler.offer(tuple);
            }

            void merge(@Nonnull List<Object> o_keyList, @Nonnull List<Object> o_valueList, Object o_trajA, List<Object> o_trajB) { //TODO CHECK!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                if (queueHandler == null) {
                    initQueueHandler();
                }
                for (int i = 0, n = o_keyList.size(); i < n; i++) {
                    queueHandler.offer(new TupleWithKey(o_keyList.get(i), o_valueList.get(i), o_trajA, o_trajB));
                }
            }

            @Nullable
            Tuple4<List<Object>,List<Object>,Object,List<Object>> drainQueue() {
                if (queueHandler == null) {
                    return null;
                }

                int n = queueHandler.size();
                final Object[] keys = new Object[n];
                final Object[] values = new Object[n];
                //   final Object[] rowA = new Object[n];
                Object trajA = null;
                final Object[] trajB = new Object[n];

                for (int i = n - 1; i >= 0; i--) { // head element in queue should be stored to tail of array

                    TupleWithKey tuple = queueHandler.poll();


                    //       LOG.info(tuple.getRowA());
                    //        LOG.info(tuple.getTrajA());
                    //        LOG.info(tuple.getTrajB());
//

                    keys[i] = tuple.getKey();
                    values[i] = tuple.getValue();
                    //     rowA[i]=tuple.getRowA();
                    trajA=tuple.getTrajA();
                    trajB[i]=tuple.getTrajB();

                }

                queueHandler.clear();

                Tuple4<List<Object>,List<Object>,Object,List<Object>> tuple4=new Tuple4<List<Object>,List<Object>,Object,List<Object>>(Arrays.asList(keys), Arrays.asList(values), trajA, Arrays.asList(trajB));

                return tuple4;
            }

            private void initQueueHandler() {
                final Comparator<TupleWithKey> comparator;
                if (reverseOrder) {
                    comparator = Collections.reverseOrder();
                } else {
                    comparator = NaturalComparator.getInstance();
                }

                if (size > 0) {
                    this.queueHandler = new BoundedQueueHandler(size, comparator);
                } else {
                    this.queueHandler = new QueueHandler(comparator);
                }
            }

        }

        /**
         * Since BoundedPriorityQueue does not directly inherit PriorityQueue, we provide handler
         * class which wraps each of PriorityQueue and BoundedPriorityQueue.
         */
        private static abstract class AbstractQueueHandler {

            abstract void offer(@Nonnull TupleWithKey tuple);

            abstract int size();

            @Nullable
            abstract TupleWithKey poll();

            abstract void clear();

        }

        private static final class QueueHandler extends AbstractQueueHandler {

            private static final int DEFAULT_INITIAL_CAPACITY = 11; // same as PriorityQueue

            @Nonnull
            private final PriorityQueue<TupleWithKey> queue;

            QueueHandler(@Nonnull Comparator<TupleWithKey> comparator) {
                this.queue = new PriorityQueue<TupleWithKey>(DEFAULT_INITIAL_CAPACITY, comparator);
            }

            @Override
            void offer(TupleWithKey tuple) {
                queue.offer(tuple);
            }

            @Override
            int size() {
                return queue.size();
            }

            @Override
            TupleWithKey poll() {
                return queue.poll();
            }

            @Override
            void clear() {
                queue.clear();
            }

        }

        private static final class BoundedQueueHandler extends AbstractQueueHandler {

            @Nonnull
            private final BoundedPriorityQueue<TupleWithKey> queue;

            BoundedQueueHandler(int size, @Nonnull Comparator<TupleWithKey> comparator) {
                this.queue = new BoundedPriorityQueue<TupleWithKey>(size, comparator);
            }

            @Override
            void offer(TupleWithKey tuple) {
                queue.offer(tuple);
            }

            @Override
            int size() {
                return queue.size();
            }

            @Override
            TupleWithKey poll() {
                return queue.poll();
            }

            @Override
            void clear() {
                queue.clear();
            }

        }

        private static final class TupleWithKey implements Comparable<TupleWithKey> {
            @Nonnull
            private final Object key;
            @Nonnull
            private final Object row;
            @Nonnull
            private final Object value;
            @Nonnull
            private final Object trajA;
            @Nonnull
            private final Object trajB;

            TupleWithKey(@CheckForNull Object key, @CheckForNull Object value, @CheckForNull Object trajA, @CheckForNull Object trajB) {
                this.key = Preconditions.checkNotNull(key);
                this.value = Preconditions.checkNotNull(value);
                this.trajA = trajA;
                this.trajB = trajB;
                row = null;
            }

            @Nonnull
            Object getKey() {
                return key;
            }

            @Nonnull
            Object getValue() {
                return value;
            }

            @Nonnull
            Object getTrajA() {
                return trajA;
            }
            @Nonnull
            Object getTrajB() {
                return trajB;
            }

            @Override
            public int compareTo(TupleWithKey o) {
                @SuppressWarnings("unchecked")
                Comparable<? super Object> k = (Comparable<? super Object>) value;
                return k.compareTo(o.getValue());
            }

            @Override
            public String toString() {
                return "{" +
                        "key=" + key +
                        ", value=" + value +
                        ", trajA=" + trajA +
                        ", trajB=" + trajB +
                        '}';
            }
        }
    }
}
