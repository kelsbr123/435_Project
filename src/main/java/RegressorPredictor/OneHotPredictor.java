package RegressorPredictor;

import org.apache.commons.cli.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.internal.config.R;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import spire.random.Op;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class OneHotPredictor {



    private static Options buildOptions(){
        Options options = new Options();
        Option input = new Option("i", "input", true, "input file path");
        input.setRequired(false);
        options.addOption(input)
                .addOption(new Option("p", "predictMode", true, "predict"))
                .addOption(new Option("t", "numTrees", true, "number of trees in forest"))
                .addOption(new Option("h", "history", true, "user purchase history"))
                .addOption(new Option("n", "nextItem", true, "product to label"));
        return options;

    }

    public static void main(String[] args) throws ParseException, IOException {

        SparkSession spark = SparkSession.builder().
                appName("Predictor")
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        Options options = buildOptions();

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

        spark.udf().register("toArray", (UDF1<String, List<String>>)
                s -> Arrays.asList(s.split(" ")), DataTypes.createArrayType(DataTypes.StringType));

        spark.udf().register("stringToFloat", (UDF1<String, Float>) Float::parseFloat, DataTypes.FloatType);


        String inputPath = cmd.getOptionValue("i");
        if(!cmd.hasOption("p")){
            int numTrees = 30;
            if(cmd.hasOption("t")) numTrees = Integer.parseInt(cmd.getOptionValue("t"));
            train(inputPath,numTrees, spark);
        }else{
            predict(cmd.getOptionValue("h"), cmd.getOptionValue("n"), spark);
        }


    }

    public static Dataset<Row> transformData(Dataset<Row> DF, boolean training) throws IOException {
        DF = DF.withColumn("purchaseHistory",
                functions.callUDF("toArray",
                        DF.col("Purchase History"))).drop("Purchase History");
        DF = DF.withColumn("nextPurchase",
                functions.callUDF("toArray",
                        DF.col("Next Purchase"))).drop("Next Purchase");

        Word2VecModel vecModel;

        if(training) {
            Word2Vec word2Vec = new Word2Vec()
                    .setInputCol("purchaseHistory")
                    .setOutputCol("history")
                    .setVectorSize(10)
                    .setMinCount(0);
            vecModel = word2Vec.fit(DF);
            vecModel.save("/vector_model.json");
        }else {
            vecModel = Word2VecModel.load("/vector_model.json");
        }
        DF = vecModel.transform(DF);
        vecModel.setInputCol("nextPurchase").setOutputCol("next");
        DF = vecModel.transform(DF);
        return DF;

    }

    public static void predict(String history, String nextItem, SparkSession spark) throws IOException {

        RandomForestRegressionModel model = RandomForestRegressionModel.load("/trained_model.json");
        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create(history,nextItem));
        Dataset<Row> DF = spark.createDataFrame(list, new StructType());
        DF = transformData(DF,false);
        Dataset<Row> predictions = model.transform(DF);
        predictions.show();



    }


    public static void train(String inputPath, int numTrees, SparkSession spark) throws IOException {

        // Load data (replace "path/to/your/data" with the actual path)
        String[] featureColumns = {"User Id", "User Name", "Purchase History", "Next Purchase", "Score"};
        Dataset<Row> DF = spark.read().csv(inputPath).toDF(featureColumns);
        DF = transformData(DF, true);


        DF = DF.withColumn("label",
                functions.callUDF("stringToFloat",
                        DF.col("score"))).drop("score");


        DF.show();


        String[] newFeatures = {"history", "next", "label"};


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
                .setNumTrees(numTrees); // Number of trees in the forest

        // Train the model
        RandomForestRegressionModel model = rf.fit(assembledTrainingData);
        model.save("/trained_model.json");
        // Make predictions on the test data
        Dataset<Row> predictions = model.transform(assembledTestData);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label").setPredictionCol("prediction")
                .setMetricName("var");
        double var = evaluator.evaluate(predictions);
        double rmse = evaluator.setMetricName("rmse").evaluate(predictions);

        List<Row> resultList = new ArrayList<>();
        resultList.add(RowFactory.create(numTrees, trainingData.count(), rmse, var));
        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("Trees", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Samples", DataTypes.LongType, false),
                        DataTypes.createStructField("RMSE", DataTypes.DoubleType, false),
                        DataTypes.createStructField("Variance", DataTypes.DoubleType, false)
                }
        );
        Dataset<Row> results = spark.createDataFrame(resultList, schema);
        predictions.select("prediction", "label", "features").show();
        results.show();

        // Stop Spark
        spark.stop();

    }
}