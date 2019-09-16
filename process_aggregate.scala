package sumx_aggregate

import java.sql.DriverManager
import java.util.Properties

import org.apache.log4j._
import org.apache.spark.SparkConf;
import org.apache.spark._  
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql;
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED

import java.net.Authenticator
import org.apache.hadoop.conf.Configuration

/* for writing logs w.r.t. data load process */
import java.io._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.YearMonth

import sys.process._

object process_aggregate {
  
  def today():String= {
    return new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date())
  }
  
  def lastMonth1TK(refresh_tk:Int):Int=  {
     
     val time_key=refresh_tk
     val inpDate= LocalDate.parse(time_key.toString(), DateTimeFormatter.ofPattern("yyyyMMdd"))
     
     val prevMth1= inpDate.minusMonths(1)     
     val prevMth1Tk= YearMonth.from(prevMth1).atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInt
     
     return prevMth1Tk
  }
  
  
  def lastMonth2TK(refresh_tk:Int):Int=  {
     
     val time_key=refresh_tk
     val inpDate= LocalDate.parse(time_key.toString(), DateTimeFormatter.ofPattern("yyyyMMdd"))
     
     val prevMth2= inpDate.minusMonths(2)
     val prevMth2Tk= YearMonth.from(prevMth2).atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInt
     
     return prevMth2Tk
  }
  
  def sparkConfig ():SparkConf= {
    
//          val spark= SparkSession.builder.appName("TestHive")
//                                             .master("local[*]")
//                                             .config("spark.hadoop.validateOutputSpecs", "false")
//                                             .config("spark.sql.codegen.wholeStage","false")
//                                             .config("spark.sql.warehouse.dir","hdfs://bda1node01.fgb.ae:8020/user/warehouse")
//                                             .config("hive.metastore.uris","thrift://bda1node01.fgb.ae:9083")
//                                             .enableHiveSupport()
    
     val conf = new SparkConf().
                            setAppName("ProcessSumxData").
                            setMaster("local[*]").
                            //set("spark.executor.cores","6").
                            //set("spark.executor.memory","32g").
                            //set("spark.executor.instances", "10").
                            set("spark.sql.warehouse.dir","hdfs://bda1node01.fgb.ae:8020/user/hive/warehouse").
                            set("hive.metastore.uris","thrift://bda1node01.fgb.ae:9083").
                            set("spark.hadoop.validateOutputSpecs", "false").
                            set("spark.sql.codegen.wholeStage","false") // TO STOP CODEGEN TO GENERATE PARQUET STRING WHICH THROWS ERROR
      return conf
  }
  
  def main (args : Array[String]){
       
       /* PRINT OUT ANY ARGUMENTS PASSED */
       println ("Load will take place for:" + args(0))
       println (lastMonth1TK(args(0).toInt))
       println (lastMonth2TK(args(0).toInt))
       
       val sparkConf = sparkConfig() 
                              
       val sc= new org.apache.spark.SparkContext(sparkConf)   
    	 val sqlContext= new org.apache.spark.sql.SQLContext(sc)
       val hc= new org.apache.spark.sql.hive.HiveContext(sc)
       
       hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
       
       /* SETTING HIVE CONFIGURATION FOR DYNAMIC PARTITION */
       //hc.setConf("hive.exec.dynamic.partition","true")
       //hc.setConf("hive.exec.dynamic.partition.mode","nonstrict")
       
       /* AGGREGATE LOCATION */
       val hdfs_raw_base_agg       = "hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_seg_raw_data/T_OUTBOUND_DEAL_BALANCE_AVERAGE_BALANCE_FGB_"+args(0).toString()+".DAT"
       val hdfs_agg_base_pth       = "hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_seg_base_agg"
       val hdfs_cust_agg_base_path = "hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_seg_cust_agg"
       val hdfs_agg                = "hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_seg_base_agg/time_key="+args(0).toString()
       val hdfs_agg_lm1            = "hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_seg_base_agg/time_key="+lastMonth1TK(args(0).toInt).toString()
       val hdfs_agg_lm2            = "hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_seg_base_agg/time_key="+lastMonth2TK(args(0).toInt).toString()
       val hdfs_cust_agg           = "hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_seg_cust_agg/time_key="+args(0).toString()
       val hdfs_gl_agg             = "hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_seg_gl_agg"
       
       
       /* DIMENSION LOCATION */
       val hdfs_dom        ="hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_dom"
       val hdfs_le         ="hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_le"
       val hdfs_pc         ="hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_pc"
       val hdfs_acc        ="hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_gl_acc_fpa"
       val hdfs_dept       ="hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_dept"
       val hdfs_prod       ="hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_prod"
       val hdfs_cov_geo    ="hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_cov_geo"
       val hdfs_cust_seg   ="hdfs://bda1node01.fgb.ae:8020/user/fgb_dip/sumx_data/osx_cust_seg"
       
       
       val pw= new PrintWriter(new File("/home/fgb_dip/scripts/logs/sumx_data_load.log"))
       //pw.println (today()+" "+"One SUMX Data Processing Started at CDHCluster-ns") 
       
       pw.println(today()+ " READING ALL DIMENSIONS REQUIRED FOR AGGREGATE LOADING")
                             
       val dom           = sqlContext.read.parquet(hdfs_dom)
       val acc           = sqlContext.read.parquet(hdfs_acc)
       val le            = sqlContext.read.parquet(hdfs_le)
       val pc            = sqlContext.read.parquet(hdfs_pc)
       val cov_geo       = sqlContext.read.parquet(hdfs_cov_geo)
       val prod          = sqlContext.read.parquet(hdfs_prod)
       val dept          = sqlContext.read.parquet(hdfs_dept)
       val cust_seg      = sqlContext.read.parquet(hdfs_cust_seg)
       
       
       pw.println(today()+ " AGGREGATE LOADING INITIATED")
       
       
       //CALLING THE BASE AGGREGATE LOAD PROCESS
       load_base_agg.osx_seg_base_agg_load(sc, sqlContext, args(0).toString, dom, acc, pw, hdfs_raw_base_agg)
        
       //POST BASE AGGREGATE CREATION CREATE A DATAFRAME ON THE SAME BASE TO PROCESS CUSTOMER AGGGREGATE
       val sumx_data=sqlContext.read
                       .option("basePath",hdfs_agg_base_pth)
                       .parquet(hdfs_agg)
       
       //CALLING THE CUSTOMER AGGREGATE LOAD PROCESS
       load_customer_agg.osx_seg_customer_agg_load(sqlContext, args(0).toString, sumx_data, dom, acc, pw, hdfs_cust_agg_base_path)
       
       //CREATE TWO DATAFRAMES BASED ON LAST TWO MONTHS TO PASS THEM FOR GL AGGREGATE PROCESSING
       val sumx_data_lm1= sqlContext.read
                                    .option("basePath", hdfs_agg_base_pth)
                                    .parquet(hdfs_agg_lm1)
                                    
       val sumx_data_lm2= sqlContext.read
                                    .option("basePath", hdfs_agg_base_pth)
                                    .parquet(hdfs_agg_lm2)
       
       
       //CALLING THE GL AGGREGATE LOAD PROCESS
       load_gl_agg.osx_seg_gl_agg_load(sqlContext, args(0).toString, sumx_data, sumx_data_lm1, sumx_data_lm2, dom, acc, pc, le, cov_geo, prod, 
                                       dept, cust_seg, pw, hdfs_gl_agg)
       
       
       
       //INSTANTIATE IMPALA SHELL AND SET PAREQUET FALLBACK CONFIGURATION AND RECOVER PARTITIONS AFTER NEW WRITE
       pw.println(today()+ " IMPALA PARQUET CONFIG SET AND RECOVER PARTITIONS AFTER NEW WRITE")
       
       val comm = "env -i impala-shell -f /home/fgb_dip/scripts/refresh_osx_impala.txt".!
       
       pw.println(today()+ " COMPLETE")
       pw.close()
              
   }
  
}