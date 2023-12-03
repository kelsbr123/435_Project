package RegressorPredictor;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Predictor {




    public static void main(String[] args) {

        String inputPath = args[0];


        // Initialize Spark

        SparkSession spark = SparkSession.builder().
                appName("Predictor")
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // Load data (replace "path/to/your/data" with the actual path)
        String[] featureColumns = {"userId", "userName",
                "purchase1", "purchase2", "purchase3",
                "purchase4", "nextPurchase", "score"};
        Dataset<Row> data = spark.read().csv(inputPath).toDF(featureColumns);
        Dataset<Row> users = data.select("userId","userName");
        data = data.drop("userId","userName");



        spark.udf().register("stringToFloat", (UDF1<String, Float>) Float::parseFloat, DataTypes.FloatType);


        spark.udf().register("stringToInt", (UDF1<String, Integer>) Integer::parseInt, DataTypes.IntegerType);

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
        int numTrees = 25;

        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol("label")
                .setFeaturesCol("features")
                .setNumTrees(numTrees); // Number of trees in the forest

        // Train the model
        RandomForestRegressionModel model = rf.fit(assembledTrainingData);

        // Make predictions on the test data
        Dataset<Row> predictions = model.transform(assembledTestData);

        // Show the predictions
        predictions.select("prediction", "label", "features").show();
        predictions.select("prediction", "label").write().format("csv").save("/en_predictions.csv");

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label").setPredictionCol("prediction")
                .setMetricName("var");
        double var = evaluator.evaluate(predictions);
        double rmse = evaluator.setMetricName("rmse").evaluate(predictions);

        List<Row> resultList = new ArrayList<>();
        resultList.add(RowFactory.create(numTrees,trainingData.count(),rmse,var));
        StructType schema = DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("Trees", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Samples", DataTypes.LongType, false),
                        DataTypes.createStructField("RMSE", DataTypes.DoubleType, false),
                        DataTypes.createStructField("Variance", DataTypes.DoubleType, false)
                }
            );
        Dataset<Row> results = spark.createDataFrame(resultList,schema);
        results.show();

        // Stop Spark
        spark.stop();
    }
}
