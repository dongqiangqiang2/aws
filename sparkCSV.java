package aws.emr.sparkcsv;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class sparkCSV {
	
	public static void main(String[] args) {

		SparkConf sparkConf=new SparkConf().setAppName("spark CSV test").setMaster("local");
		//SparkConf sparkConf=new SparkConf().setAppName("spark CSV test");
		JavaSparkContext jsc=new JavaSparkContext(sparkConf);
		jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIASBFZ7TJVHQOPXQC4");
		jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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");
		
		//String localFile="s3://buckettest/datainput/Book1.csv";
		String localFile="C:\\Users\\qiadong\\Desktop\\job_file\\demo\\sparkTest\\aaa";
		
		JavaRDD<String> datas=jsc.textFile(localFile);
		
		JavaRDD<String[]> cellData=	datas.filter(new Function<String, Boolean>() {
		
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
		}).mapPartitions(new FlatMapFunction<Iterator<String>, String[]>() {

		
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String[]> call(Iterator<String> sIterator) throws Exception {
				List<String[]> resukltList=new ArrayList<String[]>();
				
				while(sIterator.hasNext()) {
					String lineString=sIterator.next();
					String[] cells=lineString.split("\\|");
					
					for(int i=0;i<cells.length;i++) {
						String str=cells[i].replace("\"", "").replaceAll("\\s+", "");
						cells[i]=str;
					}
					resukltList.add(cells);
				}
			return 	resukltList.iterator();
			}
		});
		
		long numlines=cellData.count();
		System.out.println("********************************************"+numlines);
		//cellData.saveAsTextFile("s3://buckettest/datainput/aaa");
		
		
		
		cellData.foreachPartition(new VoidFunction<Iterator<String[]>>() {
			
		
			private static final long serialVersionUID = 1L;
			

			@Override
			public void call(Iterator<String[]> strIterator) throws Exception {
				

				int sign=0;
			
				BufferedWriter bWriter;
				String outputFile="C:\\Users\\qiadong\\Desktop\\job_file\\demo\\sparkTest\\bbb";
				//String outputFile="s3://buckettest/datainput/bbb";
				try {
					bWriter = new BufferedWriter(new FileWriter(outputFile));
					
					while(strIterator.hasNext()) {
						//System.out.println("++++++++++++++++++++数据遍历++++++++++++++++++++");
						StringBuffer buf = new StringBuffer();
						String datanewString="";
						String[] cells=strIterator.next();
						
						
						for(String cell:cells) {
							buf.append(cell).append(",");
							
						}
						datanewString=buf.substring(0, buf.length()-1);
						
						if(sign==0) {
							datanewString="masterdata_id,"+datanewString;
							
						}else {
								
								datanewString=sign+","+datanewString;
								
						}
						sign++;
						bWriter.write(datanewString+"\n");
						//System.out.println("++++++++++"+datanewString);
					}
					bWriter.close();
				
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		});
		
		
	}

}
