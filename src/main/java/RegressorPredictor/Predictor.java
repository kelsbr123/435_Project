package RegressorPredictor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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
        String[] featureColumns = {"userID", "purchaseHistory", "nextPurchase", "score"};
        Dataset<Row> data = spark.read().option("delimiter","/").csv(inputPath).drop("_c1").toDF(featureColumns);

        JavaRDD<List<String>> histories = data.javaRDD()
                .map(row -> row.getString(1).replaceAll("\\[|\\]", ""))
                .map(h -> Arrays.asList(h.split(",")));

        JavaRDD<Object> ids = histories.map(h ->{
            ArrayList<String> list = new ArrayList<>();
            for(String s : h){
                list.add(s.split(":")[0]);
            }
            return list;
        }).map(RowFactory::create);

        JavaRDD<Object> scores = histories.map(h ->{
            ArrayList<Float> list = new ArrayList<>();
            for(String s : h){
                list.add(Float.parseFloat(s.split(":")[1]));
            }
            return list;
        }).map(RowFactory::create);

        ids.take(10).forEach(h -> System.out.println(h.toString()));
        scores.take(10).forEach(h -> System.out.println(h.toString()));





        // Split the data into training and test sets
        Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];


        // Assemble the feature columns into a single vector column
//        VectorAssembler assembler = new VectorAssembler()
//                .setInputCols(featureColumns)
//                .setOutputCol("features");

//
//        Dataset<Row> assembledTrainingData = assembler.transform(trainingData);
//        Dataset<Row> assembledTestData = assembler.transform(testData);
//
//        assembledTrainingData.show();
//        assembledTestData.show();

//
//        // Create a RandomForestRegressor
//        RandomForestRegressor rf = new RandomForestRegressor()
//                .setLabelCol("label")
//                .setFeaturesCol("features")
//                .setNumTrees(10); // Number of trees in the forest
//
//        // Train the model
//        RandomForestRegressionModel model = rf.fit(assembledTrainingData);
//
//        // Make predictions on the test data
//        Dataset<Row> predictions = model.transform(assembledTestData);
//
//        // Show the predictions
//        predictions.select("prediction", "label", "features").show();
//
//        // Stop Spark
        spark.stop();
    }
}
