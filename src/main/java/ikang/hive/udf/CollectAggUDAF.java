package ikang.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
//import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by liutao on 2017/4/18.
 * 1. add jar /path/to/ikang-hive-udf-0.1.0.jar;
 * 2. create temporary function collect as 'ikang.hive.udf.CollectAggUDAF';
 * 3. select workno, concat_ws(',', collect(code)) from collecttest group by workno;
 */
@Description(name = "collect", value = "_FUNC_(x) - Returns a list of objects. " +
        "CAUTION will easily OOM on large data sets" )
public class CollectAggUDAF extends AbstractGenericUDAFResolver {

    public CollectAggUDAF() {
        // do nothing
    }

    public static class CollectAggEvaluator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputOI;
        private StandardListObjectInspector listOI;
        private StandardListObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (m == Mode.PARTIAL1) {
                // iterate() and terminatePartial()
                inputOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorUtils.getStandardObjectInspector(inputOI));
            } else {
                if (!(parameters[0] instanceof StandardListObjectInspector)) {
                    // m == COMPLETE: iterate() and terminate()
                    inputOI = (PrimitiveObjectInspector) ObjectInspectorUtils
                            .getStandardObjectInspector(parameters[0]);
                    return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
                } else {
                    // m == PARTIAL2: merge() and terminatePartial()
                    // or m == FINAL: merge() and terminate()
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
                    listOI = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return listOI;
                }
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CollectAggBuffer ret = new CollectAggBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((CollectAggBuffer) agg).container = new ArrayList<Object>();
        }

        // Map side
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);

            Object p = parameters[0];
            if (p != null) {
                CollectAggBuffer myagg = (CollectAggBuffer) agg;
                putIntoList(p, myagg);
            }
        }

        // Map side
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            CollectAggBuffer myagg = (CollectAggBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
            ret.addAll(myagg.container);
            return ret;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            @SuppressWarnings("unchecked")
            ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
            CollectAggBuffer myagg = (CollectAggBuffer) agg;
            for (Object obj : partialResult) {
                putIntoList(obj, myagg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            CollectAggBuffer myagg = (CollectAggBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
            ret.addAll(myagg.container);
            return ret;
        }

        static class CollectAggBuffer extends AbstractAggregationBuffer {
            List<Object> container;
            @Override
            public int estimate() {
                // return JavaDataModel.JAVA64_ARRAY;
                return -1;
            }
        }

        private void putIntoList(Object p, CollectAggBuffer myagg) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(p, this.inputOI);
            myagg.container.add(pCopy);
        }
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                "Only primitive type arguments are accepted but "
                        + parameters[0].getTypeName() + " was passed as parameter 1.");
        }

        return new CollectAggEvaluator();
    }

}
