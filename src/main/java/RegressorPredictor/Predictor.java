package RegressorPredictor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Predictor {

    public static void main(String[] args) {

        String inputPath = args[0];
        String outputPath = args[1];


        // Initialize Spark

        SparkSession spark = SparkSession.builder().
                appName("Predictor").master("local")
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // Load data (replace "path/to/your/data" with the actual path)
        String dataPath = "/test_output/part_r_00000.";
        Dataset<Row> data = spark.read().csv(inputPath);

        // Split the data into training and test sets
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Define the feature columns
        String[] featureColumns = {"userID", "reviewerName", "purchaseHistory", "nextPurchase", "score"};

        // Assemble the feature columns into a single vector column
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureColumns)
                .setOutputCol("features");

        Dataset<Row> assembledTrainingData = assembler.transform(trainingData);
        Dataset<Row> assembledTestData = assembler.transform(testData);

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

        // Stop Spark
        spark.stop();
    }
}
