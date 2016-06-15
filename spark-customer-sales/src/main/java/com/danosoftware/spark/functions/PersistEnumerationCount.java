package com.danosoftware.spark.functions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

public class PersistEnumerationCount implements Function<JavaPairRDD<String, Integer>, Void>
{
    private static final long serialVersionUID = 1L;

    @Override
    public Void call(JavaPairRDD<String, Integer> v1) throws Exception
    {
        //        List<Tuple2<String, Integer>> list = v1.collect();
        //
        //        if (list.size() > 0)
        //        {
        //            AlgorithmResultsIF algorithmResult = new CountedResults(0);
        //            Timestamp time = TimestampUtilities.getCurrentTimestamp();
        //            algorithmResult.setStartTime(time);
        //            algorithmResult.setEndTime(time);
        //            algorithmResult.setSourceStatistic(SourceStatisticType.NUMBER_OF, FieldCategory.STRING);
        //
        //            DataFieldIdentifierIF field = DataIdentifierUtilities.createDataFieldIdentifier("customer", "dataSource", "dataStore",
        //                    "collection", "field", FieldCategory.STRING);
        //            DataFieldVO fieldVO = new DataFieldVO(field, false, false, false, false);
        //
        //            ServiceFacade.getSchemaService().insertFieldIfMissing(fieldVO);
        //
        //            algorithmResult.setDataOriginID(field);
        //
        //            CountedCache<String> countedCache = new CountedCache<String>(0);
        //
        //            for (Tuple2<String, Integer> aTuple : list)
        //            {
        //                Counted<String> countedStringValue = new Counted<String>();
        //                countedStringValue.initialise(aTuple._1);
        //                countedStringValue.setSamplesSeen(aTuple._2);
        //
        //                countedCache.addCountedObject(countedStringValue);
        //
        //            }
        //
        //            ((CountedResults) algorithmResult).setCountedCache(countedCache);
        //
        //            BatchIdentifier batch = new BatchIdentifierBuilder().setBatchId(0L).setSubBatchId(0L).setTaskId(0L).build();
        //
        //            ServiceFacade.getAlgorithmResultsService().persistAlgorithmResults(Arrays.asList(algorithmResult), batch);
        //        }

        // TODO Auto-generated method stub
        return null;
    }

}
