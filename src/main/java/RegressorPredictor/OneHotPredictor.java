package RegressorPredictor;

public class OneHotPredictor {
    //
//        JavaRDD<List<String>> histories = data.javaRDD()
//                .map(row -> row.getString(0).replaceAll("\\[|\\]", ""))
//                .map(h -> Arrays.asList(h.split(",")));
//
//        List<PurchaseHistory> ids = histories.map(h ->{
//            PurchaseHistory hist = new PurchaseHistory();
//            List<Integer> tmp = new ArrayList<>();
//            for(String s : h){
//                tmp.add(Integer.parseInt(s.split(":")[0].strip()));
//            }
//            hist.setHistory(tmp);
//            return hist;
//        }).collect();
//
//        List<ScoreHistory> scores = histories.map(h ->{
//            List<Float> list = new ArrayList<>();
//            ScoreHistory history = new ScoreHistory();
//            for(String s : h){
//                list.add(Float.parseFloat(s.split(":")[1]));
//            }
//            history.setHistory(list);
//            return history;
//        }).collect();
//
//        data.show();
//
//
//        Encoder<PurchaseHistory> purchaseHistoryEncoder = Encoders.bean(PurchaseHistory.class);
//        Dataset<Row> test = spark.createDataset(ids,purchaseHistoryEncoder).toDF("hist");
//        String[] strings = {"hist"};
//        Dataset<Row> df = test.toDF(strings);
//        OneHotEncoder oneHotEncoder = new OneHotEncoder().setInputCol("hist").setOutputCol("histVect");
//        OneHotEncoderModel model = oneHotEncoder.fit(df);
//        Dataset<Row> encoded = model.transform(df);
//        encoded.show();

}
