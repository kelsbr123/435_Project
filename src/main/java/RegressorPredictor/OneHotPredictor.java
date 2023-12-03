package RegressorPredictor;

import org.apache.commons.cli.*;
import org.apache.commons.lang.ArrayUtils;
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
import scala.Int;
import spire.random.Op;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class OneHotPredictor {



    private static Options buildOptions(){
        Options options = new Options();
        Option input = new Option("i", "input", true, "input file path");
        input.setRequired(true);
        options.addOption(input)
                .addOption(new Option("p", "predictMode", false, "predict using already trained model"))
                .addOption(new Option("m","evaluateMode", false, "evaluate model performance"))
                .addOption(new Option("t", "numTrees", true, "number of trees in forest"))
                .addOption(new Option("v","vectorSize",true,"size of encoding vectors"));

    return options;

    }

    private static void registerUDFS(SparkSession spark){
        spark.udf().register("toArray", (UDF1<String, List<String>>)
                s -> Arrays.asList(s.split(" ")), DataTypes.createArrayType(DataTypes.StringType));

        spark.udf().register("stringToFloat", (UDF1<String, Float>) Float::parseFloat, DataTypes.FloatType);

    }

    private static CommandLine parseArgs(Options options, String[] args) {
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

        return cmd;
    }

    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession.builder().
                appName("OneHotPredictor")
                .getOrCreate();
        registerUDFS(spark);

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Options options = buildOptions();
        CommandLine cmd = parseArgs(options,args);

        String inputPath = cmd.getOptionValue("i");
        String[] featureColumns = {"User Id", "User Name", "Purchase History", "Next Purchase", "Score"};
        Dataset<Row> DF = spark.read().csv(inputPath).toDF(featureColumns);

        int numTrees = Integer.parseInt(cmd.getOptionValue("t","10"));
        int vectorLength = Integer.parseInt(cmd.getOptionValue("v", "5"));

        if(cmd.hasOption("m")){
            modelEvaluator(DF,spark);
        }
        else if(!cmd.hasOption("p")){

            DF = transformData(DF, vectorLength, true, true);
            train(DF , numTrees).write().overwrite().save("/trained_model.json");

        }else{
            RandomForestRegressionModel model = RandomForestRegressionModel.load("/trained_model.json");
            DF = transformData(DF, vectorLength, false, false);
            predict(model, DF);
        }
        spark.stop();


    }
    public static Dataset<Row> buildResults(List<Row> results, SparkSession spark){

        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("Vector Size", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Trees", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Samples", DataTypes.LongType, false),
                        DataTypes.createStructField("RMSE", DataTypes.DoubleType, false),
                        DataTypes.createStructField("Variance", DataTypes.DoubleType, false)
                }
        );
        return spark.createDataFrame(results, schema);

    }

    public static void modelEvaluator(Dataset<Row> dataset, SparkSession spark) throws IOException {

        int[] vectorLength = {5,10,20};
        int[] numTrees = {10,20,50};
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label").setPredictionCol("prediction");
        List<Row> resultList = new ArrayList<>();

        for (int k : vectorLength) {

            Dataset<Row> DF = transformData(dataset, k, true, false);
            Dataset<Row>[] splits = DF.randomSplit(new double[]{0.7, 0.3});
            Dataset<Row> trainingData = splits[0];
            Dataset<Row> testData = splits[1];

            for (int numTree : numTrees) {

                RandomForestRegressionModel model = train(trainingData, numTree);
                Dataset<Row> predictions = model.transform(testData);

                double var = evaluator.setMetricName("var").evaluate(predictions);
                double rmse = evaluator.setMetricName("rmse").evaluate(predictions);

                resultList.add(
                        RowFactory.create(k, numTree, trainingData.count(), rmse, var));
            }
        }
        buildResults(resultList,spark).show();
    }

    public static Dataset<Row> transformData(Dataset<Row> DF, int vectorLength, boolean training, boolean save) throws IOException {

        DF = DF.withColumn("label",
                functions.callUDF("stringToFloat",
                        DF.col("Score"))).drop("Score");

        DF = DF.withColumn("purchaseHistory",
                functions.callUDF("toArray",
                        DF.col("Purchase History")));
        DF = DF.withColumn("nextPurchase",
                functions.callUDF("toArray",
                        DF.col("Next Purchase")));

        Word2VecModel vecModel;
        Word2Vec word2Vec;
        String[] features = {"history","next", "label"};

        if(training) {
            word2Vec = new Word2Vec()
                    .setInputCol("purchaseHistory")
                    .setVectorSize(vectorLength)
                    .setMinCount(0);
            if(save) word2Vec.write().overwrite().save("/vector_model.json");

        }else {
            word2Vec = Word2Vec.load("/vector_model.json");
        }

        vecModel = word2Vec.fit(DF);

        vecModel.setInputCol("purchaseHistory").setOutputCol("history");
        DF = vecModel.transform(DF);
        vecModel.setInputCol("nextPurchase").setOutputCol("next");
        DF = vecModel.transform(DF);

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(features)
                .setOutputCol("features");


        DF = assembler.transform(DF);


        return DF;

    }

    public static RandomForestRegressionModel train(Dataset<Row> trainingData, int numTrees) {


        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol("label")
                .setFeaturesCol("features")
                .setNumTrees(numTrees); // Number of trees in the forest

        // Train the model

        return rf.fit(trainingData);
    }


    public static void predict(RandomForestRegressionModel model, Dataset<Row> DF) throws IOException {

       Dataset<Row> predictions = model.transform(DF);
       predictions.select("User Name", "Next Purchase", "prediction", "label").show();
       predictions.select("prediction","label").write().mode("overwrite").format("csv").save("/onehot_result.csv");
    }

}