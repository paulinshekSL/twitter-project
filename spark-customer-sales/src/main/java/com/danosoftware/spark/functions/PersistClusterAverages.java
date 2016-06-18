package com.danosoftware.spark.functions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

public class PersistClusterAverages implements Function<JavaPairRDD<Integer, ClusterAvg>, Void>
{
    private static final long serialVersionUID = 1L;

    @Override
    public Void call(JavaPairRDD<Integer, ClusterAvg> v1) throws Exception
    {
        //        List<Tuple2<Integer, ClusterAvg>> list = v1.collect();
        //
        //        List<AlgorithmResultsIF> results = new ArrayList<>();
        //
        //        if (list.size() > 0)
        //        {
        //            DataFieldIdentifierIF field = DataIdentifierUtilities.createDataFieldIdentifier("customer", "dataSource", "dataStore",
        //                    "collection", "field", FieldCategory.STRING);
        //            DataFieldVO fieldVO = new DataFieldVO(field, false, false, false, false);
        //
        //            ServiceFacade.getSchemaService().insertFieldIfMissing(fieldVO);
        //
        //            for (Tuple2<Integer, ClusterAvg> aTuple : list)
        //            {
        //                AlgorithmResultsIF algorithmResult = new NormalIntDistributionResults();
        //                algorithmResult.setDataOriginID(field);
        //                Timestamp time = TimestampUtilities.getCurrentTimestamp();
        //                algorithmResult.setStartTime(time);
        //                algorithmResult.setEndTime(time);
        //                algorithmResult.setSourceStatistic(SourceStatisticType.VALUE_OF, FieldCategory.DECIMAL_PLACE);
        //                algorithmResult.setEnumComboGroup(EnumComboGroupVO
        //                        .generateEnumComboGroup(field, "clusters: " + aTuple._2.clusters().size()));
        //                algorithmResult.getStorage().setDoubleValue(StatisticType.AVERAGE, aTuple._2.avg());
        //                results.add(algorithmResult);
        //            }
        //
        //            BatchIdentifier batch = new BatchIdentifierBuilder().setBatchId(0L).setSubBatchId(0L).setTaskId(0L).build();
        //
        //            ServiceFacade.getAlgorithmResultsService().persistAlgorithmResults(results, batch);
        //        }

        // TODO Auto-generated method stub
        return null;
    }
}
