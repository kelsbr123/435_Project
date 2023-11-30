package RegressorPredictor;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class Predictor {




    public static void main(String[] args) {

        String inputPath = args[0];
        String outputPath = args[1];


        // Initialize Spark

        SparkSession spark = SparkSession.builder().
                appName("Predictor")
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // Load data (replace "path/to/your/data" with the actual path)
        String[] featureColumns = {"userId", "userName",
                "purchase1", "purchase2", "purchase3",
                "purchase4", "nextPurchase", "score"};
        Dataset<Row> data = spark.read().csv(inputPath).toDF(featureColumns).drop("userId","userName");



        spark.udf().register("stringToFloat", new UDF1<String, Float>() {
            @Override
            public Float call(String input) {
                return Float.parseFloat(input);
            }
        }, DataTypes.FloatType);


        spark.udf().register("stringToInt", new UDF1<String, Integer>() {
            @Override
            public Integer call(String input) {
                return Integer.parseInt(input);
            }
        }, DataTypes.IntegerType);

        Dataset<Row> DF = data.withColumn("p1",
                functions.callUDF("stringToInt",
                        data.col("purchase1"))).drop("purchase1");
        DF = DF.withColumn("p2",
                functions.callUDF("stringToInt",
                        data.col("purchase2"))).drop("purchase2");
        DF = DF.withColumn("p3",
                functions.callUDF("stringToInt",
                        data.col("purchase3"))).drop("purchase3");
        DF = DF.withColumn("p4",
                functions.callUDF("stringToInt",
                        data.col("purchase4"))).drop("purchase4");
        DF = DF.withColumn("next",
                functions.callUDF("stringToInt",
                        data.col("nextPurchase"))).drop("nextPurchase");
        DF = DF.withColumn("label",
                functions.callUDF("stringToFloat",
                        data.col("score"))).drop("score");

        DF.show();

        String[] newFeatures = {"p1","p2","p3","p4","next","label"};



        // Split the data into training and test sets
        Dataset<Row>[] splits = DF.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];



        // Assemble the feature columns into a single vector column
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(newFeatures)
                .setOutputCol("features");


        Dataset<Row> assembledTrainingData = assembler.transform(trainingData);
        Dataset<Row> assembledTestData = assembler.transform(testData);

        assembledTrainingData.show();
        assembledTestData.show();


        // Create a RandomForestRegressor
        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol("label")
                .setFeaturesCol("features")
                .setNumTrees(10); // Number of trees in the forest

        // Train the model
        RandomForestRegressionModel model = rf.fit(assembledTrainingData);

        // Make predictions on the test data
        Dataset<Row> predictions = model.transform(assembledTestData);

        // Show the predictions
        predictions.select("prediction", "label", "features").show();

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label").setPredictionCol("prediction")
                .setMetricName("var");
        double var = evaluator.evaluate(predictions);
        double rmse = evaluator.setMetricName("rmse").evaluate(predictions);

        System.out.printf("Variance: %f RMSE: %f Number of Samples: %d\n", var, rmse, trainingData.count());

        // Stop Spark
        spark.stop();
    }
}
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
