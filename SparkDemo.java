package aws.emr.sparkdemo.one2two;

import org.apache.spark.api.java.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkDemo {

    public static void main(String[] args) {
    	//System.setProperty("hadoop.home.dir", "c:\\winutil\\");
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        //SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIASBFZ7TJVHQOPXQC4");
        sc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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");
        SQLContext sqlContext=new SQLContext(sc);
        String inputFile = args[0];
        //String localFile="s3://submission-volvo-demo/BU1/SYS1/RMDB/demo/public/dmbs_dde_costs/";
        //String localFile="C:\\Users\\qiadong\\Downloads\\LOAD00000001.csv";
        
        JavaRDD<String> logData = sc.textFile(inputFile);
        
        JavaRDD<String> cellData=	logData.filter(new Function<String, Boolean>() {
    		
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String line) throws Exception {
				boolean result=false;
				String vString=line.trim();
				if(!vString.equals("")&&vString.length()>0) {
					result=true;
				}
				
				return result;
			}
		}).repartition(1).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

		
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Iterator<String> sIterator) throws Exception {
				List<String> resukltList=new ArrayList<String>();
				//int sign=0;
				int sign=1;
				while(sIterator.hasNext()) {
					StringBuffer buf = new StringBuffer();
					String datanewString="";
					String lineString=sIterator.next();
					
				/**	Pattern p = Pattern.compile("(\".*?),(.*?\")");
				    Matcher m = p.matcher(lineString);
				    StringBuffer sb=new StringBuffer();
				    while(m.find()){
					   m.appendReplacement(sb,m.group().replace(",", ""));
					   System.out.println(m.group());
				    }
				    m.appendTail(sb);
				    System.out.println(sb);
					String cleanString=sb.toString(); 
					*/
					
					String[] cells=lineString.split(",");
					
					for(int i=0;i<cells.length;i++) {
						String str=cells[i].replace("\"", "").replaceAll("\\s+", "");
						
						cells[i]=str;
					}
					for(String cell:cells) {
						buf.append(cell).append(",");
						
					}
					datanewString=buf.substring(0, buf.length()-1);
					
					if(sign==0) {
						
						datanewString="masterdata_id,"+datanewString;
						sign++;
						
					}else {
							
							datanewString=UUID.randomUUID()+","+datanewString;
							
					}
					System.out.println(datanewString);
					
					resukltList.add(datanewString);
				}
			return 	resukltList.iterator();
			}
		});
        
        JavaRDD<Row> rowRDD = cellData.map(new Function<String, Row>() {
            public Row call(String line) throws Exception {
                String[] parts = line.split(",");
                String sid = parts[0];
                String sname = parts[1];
                String sname1 = parts[2];
                
                String sname2 = parts[3];
                String sname3 = parts[4];
                int sage = Integer.parseInt(parts[5]);
                
                String sname4 = parts[6];
                String sname5 = parts[7];
                String sname6 = parts[8];
                String sname7 = parts[9];
                String sname8 = parts[10];
                String sname9 = parts[11];
                return RowFactory.create(sid, sname,sname1,sname2, sname3,sage,sname4,sname5,sname6,sname7,sname8,sname9);
            }
        });

        ArrayList<StructField> fields = new ArrayList<StructField>();
        
        fields.add(DataTypes.createStructField("sid", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sname", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sname1", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sname2", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sname3", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sage", DataTypes.IntegerType, true)); 
        fields.add(DataTypes.createStructField("sname4", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sname5", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sname6", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sname7", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sname8", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sname9", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = sqlContext.createDataFrame(rowRDD, schema);
        df.registerTempTable("bucket2_test");
        String sqlString="select * from bucket2_test";
        Dataset<Row> table_data=sqlContext.sql(sqlString);
        table_data.show();
        table_data.printSchema();
        //String outputFile = "s3://curated-volvo-demo/BU1/SUBJECT1/demo/public/DMBS_DDE_COSTS/emr_etl_output/etl_demo_result/";
        String outputFile = args[1];
        table_data.write().format("csv").save(outputFile);
		long numlines=logData.count();
		System.out.println("********************************************"+numlines);
        sc.stop();
    }
}
