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
        input.setRequired(false);
        options.addOption(input)
                .addOption(new Option("p", "predictMode", false, "predict using already trained model"))
                .addOption(new Option("m","evaluateMode", false, "evaluate model performance"))
                .addOption(new Option("t", "numTrees", true, "number of trees in forest"));

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
        String[] featureColumns = {"User Id", "User Name", "Purchase History", "Next Purchase", "Score"};
        Dataset<Row> DF = spark.read().csv(inputPath).toDF(featureColumns);
        if(cmd.hasOption("m")){
            modelEvaluator(DF,spark);
        }
        else if(!cmd.hasOption("p")){
            int numTrees = 10;
            if(cmd.hasOption("t")) numTrees = Integer.parseInt(cmd.getOptionValue("t"));
            DF = transformData(DF, 10, true, true);
            train(DF,numTrees);
        }else{
            predict(cmd.getOptionValue("h"), cmd.getOptionValue("n"), spark);
        }
        spark.stop();


    }

    public static void modelEvaluator(Dataset<Row> dataset, SparkSession spark) throws IOException {

        int[] vectorLength = {5,10,20};
        int[] numTrees = {10,20,50};
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label").setPredictionCol("prediction");

        List<Row> resultList = new ArrayList<>();

        for(int i = 0; i < vectorLength.length; i++) {

            Dataset<Row> DF = transformData(dataset, vectorLength[i], true, false);
            Dataset<Row>[] splits = DF.randomSplit(new double[]{0.7, 0.3});
            Dataset<Row> trainingData = splits[0];
            Dataset<Row> testData = splits[1];

            for (int j = 0; j < numTrees.length; j++) {

                RandomForestRegressionModel model = train(trainingData,numTrees[j]);
                Dataset<Row> predictions = model.transform(testData);

                double var = evaluator.setMetricName("var").evaluate(predictions);
                double rmse = evaluator.setMetricName("rmse").evaluate(predictions);

                resultList.add(
                        RowFactory.create(vectorLength[i], numTrees[j], trainingData.count(), rmse, var));
            }
        }

        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("Vector Size", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Trees", DataTypes.IntegerType, false),
                        DataTypes.createStructField("Samples", DataTypes.LongType, false),
                        DataTypes.createStructField("RMSE", DataTypes.DoubleType, false),
                        DataTypes.createStructField("Variance", DataTypes.DoubleType, false)
                }
        );
        System.out.println(resultList);
        Dataset<Row> results = spark.createDataFrame(resultList, schema);
        results.show();
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
        String[] features = {"history","next"};

        if(training) {

            Word2Vec word2Vec = new Word2Vec()
                    .setInputCol("purchaseHistory")
                    .setVectorSize(vectorLength)
                    .setMinCount(0);
            vecModel = word2Vec.fit(DF);
            if(save) vecModel.write().overwrite().save("/vector_model.json");
            features = (String[]) ArrayUtils.add(features,"label");

        }else {
            vecModel = Word2VecModel.load("/vector_model.json");
        }

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


    public static void predict(String history, String nextItem, SparkSession spark) throws IOException {

//        RandomForestRegressionModel model = RandomForestRegressionModel.load("/trained_model.json");
//        List<Row> list = new ArrayList<>();
//        list.add(RowFactory.create(history,nextItem, 3.0));
//
//        Dataset<Row> DF = spark.createDataFrame(list, DataTypes.createStructType(
//                new StructField[]{
//                        DataTypes.createStructField("Purchase History", DataTypes.StringType, true),
//                        DataTypes.createStructField("Next Purchase", DataTypes.StringType, true),
//                        DataTypes.createStructField("label",DataTypes.DoubleType,true)
//                }
//        ));
//        DF = transformData(DF,false);
//        DF.show();
//        Dataset<Row> predictions = model.transform(DF);
//        predictions.show();



    }

}