package aws.emr.sparkdemo.two2three;



import java.util.ArrayList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



public class SparkSqlTest {
	
	public static void main(String[] args) {
		
		//SparkSession sparkSession=SparkSession.builder().master("local").appName("RDDToDF").getOrCreate();
		SparkSession sparkSession=SparkSession.builder().appName("RDDToDF").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        sc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIASBFZ7TJVHQOPXQC4");
        sc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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");
	    SQLContext sqlContext=new SQLContext(sc);
	    //String inputFile="s3://curated-volvo-demo/BU1/SUBJECT1/demo/public/DMBS_DDE_COSTS/emr_etl_output/etl_demo_result/*.csv";
	    String inputFile=args[0];
	    Dataset<Row> df=sqlContext.read().format("com.databricks.spark.csv")
	    .option("header", "false")  
	    .option("inferSchema", "false")
	        .option("delimiter",",")
	    //.load("C://Users//qiadong//Downloads//part-00000-9c95475e-a340-473a-a443-e3ef6b235afe-c000.csv");
	    .load(inputFile);
	    
	    JavaRDD<Row> temp = df.toJavaRDD();
	    ArrayList<StructField> fields = new ArrayList<StructField>();
	    fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("caseID", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("evID", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("localNum", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("startTime", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("position", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("method", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("dialNumber", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("callDuration", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("starFlag", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("week", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("hours", DataTypes.StringType, true));



	    StructType st = DataTypes.createStructType(fields);
	    Dataset<Row> newdf = sqlContext.createDataFrame(temp, st);
	    newdf.registerTempTable("calllistbean_bak");
	    String sqlcommand = "select id,caseID,evID,localNum,startTime,position,method,dialNumber,callDuration,starFlag,week,hours from calllistbean_bak where startTime='SSSC_1A1B'";
	    Dataset<Row> dataResult = sqlContext.sql(sqlcommand);
	    dataResult.show();
	    dataResult.printSchema();
	    //String outputFile="s3://buckettest/dataframeTest/emr_etl_output1";
	    String outputFile=args[1];
	    dataResult.write().mode(SaveMode.Overwrite).csv(outputFile);
	    sc.stop();
	 
	}

}
