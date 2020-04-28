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
import java.io.File
import java.io.PrintWriter
import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.YearMonth

import sys.process._
import java.io.FileNotFoundException
import java.io.IOException

object process_sumx_agg {
  
  def time():String={
    
    /* GET CURRENT DATESTAMP FOR LOG WRITING TO YARN */
    val timeformat= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    return timeformat.format(new Date()).toString()
  }
  
  
  def lastYearSameMonthTK(refresh_tk:Int):Int=  {
     
     val time_key=refresh_tk
     val inpDate= LocalDate.parse(time_key.toString(), DateTimeFormatter.ofPattern("yyyyMMdd"))
     
     val lastYearSM= inpDate.minusMonths(12)
     val lastYearSMTk= YearMonth.from(lastYearSM).atEndOfMonth().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInt
     
     return lastYearSMTk
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
  
   def returnLastDayOfMonth (refreshDate:String):String = {
    
     val dateFormat= new SimpleDateFormat("yyyyMMdd")
     val calendar= Calendar.getInstance()
     
     calendar.setTime(dateFormat.parse(refreshDate))
     
     calendar.add(Calendar.MONTH, 1)
     calendar.set(Calendar.DAY_OF_MONTH, 1)
     calendar.add(Calendar.DATE, -1)
     
     val lastDayofMonth= dateFormat.format(calendar.getTime())
     
     return lastDayofMonth
    
  }
  
  
  def sparkConfig(time:String):SparkConf= {
    
   val conf = new SparkConf().
                            setMaster("yarn").
                            setAppName("ProcessSumx - AggregateLoad").
                            set("spark.hadoop.fs.defaultFS","hdfs://dev1node01.fgb.ae:8020").
                            set("spark.hadoop.yarn.resourcemanager.address", "dev1node04.fgb.ae:8032").
                            set("spark.hadoop.yarn.resourcemanager.hostname","dev1node04.fgb.ae").
                            //set("spark.executor.cores","6").
                            //set("spark.ex ecutor.memory","32g").
                            //set("spark.executor.instances", "10").
                            //set("spark.sql.warehouse.dir","hdfs://bda1node01.fgb.ae:8020/user/hive/warehouse").
                            //set("hive.metastore.uris","thrift://bda1node01.fgb.ae:9083").
                            set("spark.hadoop.validateOutputSpecs", "false").
                            set("spark.sql.codegen.wholeStage","false"). // TO STOP CODEGEN TO GENERATE PARQUET STRING WHICH THROWS ERROR
                            set("spark.hadoop.hadoop.security.authentication", "kerberos").
                            set("spark.hadoop.hadoop.security.authorization", "true").
                            set("spark.hadoop.dfs.namenode.kerberos.principal","hdfs/dev1node01.fgb.ae@FGB.AE").
                            set("spark.hadoop.yarn.resourcemanager.principal", "yarn/dev1node01.fgb.ae@FGB.AE").
                            set("spark.yarn.keytab", "/home/o2072/o2072.keytab").
                            set("spark.yarn.principal", "o2072@FGB.AE").
                            set("spark.yarn.access.hadoopFileSystem", "hdfs://CDHCluster-ns:8020").
                            set("spark.yarn.access.namenodes","hdfs://CDHCluster-ns")
     
     println(time+" "+"SPARK CONFIG set") 
     return conf
   }
  
  
  def main (args : Array[String]){
           
       /* INPUT ARGUMENTS TO PARAMATERIZE LOAD PROCESS */
       val time_key     = args(0).toString()
       val load_process = args(1).toString().toUpperCase()  // TAKES INPUT 1- BASE, 2- CUST, 3- GL
       
       /* PRINT OUT ANY ARGUMENTS PASSED */
       println (time+ " "+"LOAD WILL TAKE PLACE FOR TIMEKEY :" +args(0))
       println (time+ " "+"OSX GL AGG WILL REFER CURR MONTH -1 TIMEKEY :"+lastMonth1TK(args(0).toInt))
       println (time+ " "+"OSX GL AGG WILL REFER CURR MONTH -2 TIMEKEY :"+lastMonth2TK(args(0).toInt))
       println (time+ " "+"OSX GL AGG WILL REFER LAST YEAR SAME MONTH TIMEKEY :"+lastYearSameMonthTK(args(0).toInt))
       
       val sparkConf = sparkConfig(time)                        
       val sc= new org.apache.spark.SparkContext(sparkConf)   
    	 val sqlContext= new org.apache.spark.sql.SQLContext(sc)
       //val hc= new org.apache.spark.sql.hive.HiveContext(sc)
       
       
       /* SETTING HIVE CONFIGURATION FOR DYNAMIC PARTITION */
       //hc.setConf("hive.exec.dynamic.partition","true")
       //hc.setConf("hive.exec.dynamic.partition.mode","nonstrict")
       
       //CHECKING IF REFRESH DATE IS MONTH END OR NOT - BASED ON THAT CHANGE THE OVERWRITE FLAG
       val refreshDate = args(0).toString()
       val monthEndDate = returnLastDayOfMonth(refreshDate)
       val overwrite_flag = if (refreshDate == monthEndDate) 0 else 1
       
       println("Month End Date : "+ monthEndDate)
       println("Overwrite Flag : "+ overwrite_flag)
       
           
       /* AGGREGATE LOCATION */
       val hdfs_seg_base_path         = "/data/fin_onesumx/fin_fct_osx_segmental_agg_rpt"
       val hdfs_trial_base_path       = "/data/fin_onesumx/fin_fct_osx_trial_balance"
       val hdfs_cust_base_path        = "/data/fin_onesumx/fin_fct_osx_segmental_cust_agg_rpt"
       val hdfs_gl_base_path          = "/data/fin_onesumx/fin_fct_osx_segmental_gl_agg_rpt"
       val hdfs_raw_base_agg          = "/raw/onesumx/daily/T_OUTBOUND_DEAL_BALANCE_AVERAGE_BALANCE_FGB_"+args(0).toString()+".DAT"
       val hdfs_agg                   = "/data/fin_onesumx/fin_fct_osx_segmental_agg_rpt/time_key="+args(0).toString()
       val hdfs_cust_agg_base_path    = "/data/fin_onesumx/fin_fct_osx_segmental_cust_agg_rpt"
       val hdfs_gl_agg                = "/data/fin_onesumx/fin_fct_osx_segmental_gl_agg_rpt"
       val hdfs_gl_agg_lm1            = "/data/fin_onesumx/fin_fct_osx_segmental_gl_agg_rpt/time_key="+lastMonth1TK(args(0).toInt).toString()
       val hdfs_gl_agg_lm2            = "/data/fin_onesumx/fin_fct_osx_segmental_gl_agg_rpt/time_key="+lastMonth2TK(args(0).toInt).toString()
       val hdfs_gl_agg_lysm           = "/data/fin_onesumx/fin_fct_osx_segmental_gl_agg_rpt/time_key="+lastYearSameMonthTK(args(0).toInt).toString()
       
       
       /* DIMENSION LOCATION */
       val hdfs_dom                   ="/data/fin_onesumx/dim_osx_domain"
       val hdfs_le                    ="/data/fin_onesumx/dim_osx_legal_entity"
       val hdfs_pc                    ="/data/fin_onesumx/dim_osx_profit_centre"
       val hdfs_acc                   ="/data/fin_onesumx/dim_osx_gl_accounts_fpa"
       val hdfs_dept                  ="/data/fin_onesumx/dim_osx_department"
       val hdfs_prod                  ="/data/fin_onesumx/dim_osx_product"
       val hdfs_cov_geo               ="/data/fin_onesumx/dim_osx_coverage_geography"
       val hdfs_cust_seg              ="/data/fin_onesumx/dim_osx_customer_segment_fpa"
       val hdfs_osx_cust              ="/data/fin_onesumx/dim_osx_customer"
       val hdfs_osx_cont              ="/data/fin_onesumx/dim_osx_contracts"
       
       
       /* BELOW IF ELSE CHECKS IF THE FILE ALREADY EXISTS THEN APPEND ELSE CREATE NEW AND APPEND */
       
//           try {
//                 if (file.exists() && !file.isDirectory()) {
//                    pw= new PrintWriter(new FileOutputStream(new File(savestr), true)) 
//                 }
//                 else {
//                    pw= new PrintWriter(new File(savestr))
//                 }
//           }
//           catch {
//             
//             case ex:FileNotFoundException => ex.printStackTrace()
//             
//           }

       //pw.println (time+" "+ " "+"One SUMX Data Processing Started at CDHCluster-ns") 
       
       println (time +" "+  "READING ALL DIMENSIONS REQUIRED FOR AGGREGATE LOADING")
       //pw.println (time+" "+  " READING ALL DIMENSIONS REQUIRED FOR AGGREGATE LOADING")
       
                             
       val dom           = sqlContext.read.parquet(hdfs_dom)
       val acc           = sqlContext.read.parquet(hdfs_acc)
       val le            = sqlContext.read.parquet(hdfs_le)
       val pc            = sqlContext.read.parquet(hdfs_pc)
       val cov_geo       = sqlContext.read.parquet(hdfs_cov_geo)
       val prod          = sqlContext.read.parquet(hdfs_prod)
       val dept          = sqlContext.read.parquet(hdfs_dept)
       val cust_seg      = sqlContext.read.parquet(hdfs_cust_seg)
       val osx_cust      = sqlContext.read.parquet(hdfs_osx_cust)
       val osx_cont      = sqlContext.read.parquet(hdfs_osx_cont)
       
       if (load_process.equals("BASE")) {
         
           println (time+" "+"BASE AGGREGATE LOAD INITIATED FROM DRIVER CLASS")
           //pw.println (time+" "+  " AGGREGATE LOADING INITIATED")
           
           //CALLING THE BASE AGGREGATE LOAD PROCESS
           println (time+" "+"OSX BASE AGG - CALLING BASE AGGREGATE LOAD FROM DRIVER CLASS")
           load_base_agg.osx_seg_base_agg_load(sc, sqlContext, args(0).toString, dom, acc, le, osx_cust, osx_cont, hdfs_raw_base_agg, overwrite_flag)
           
           println (time+" "+"VERSION CLEANING - CHECK BASE AGGREGATE TO RETAIN LAST FIVE VERSIONS")
           process_version_cleaning.version_cleaning(sc, args(0).toString(), hdfs_seg_base_path, overwrite_flag)
           
           println (time+" "+"VERSION CLEANING - CHECK TRIAL BALANCE TO RETAIN LAST FIVE VERSIONS")
           process_version_cleaning.version_cleaning(sc, args(0).toString(), hdfs_trial_base_path, overwrite_flag)
       
       }     
       else if (load_process.equals("CUST")){
         
           println (time+" "+"CUST AGGREGATE LOAD INITIATED FROM DRIVER CLASS")
           //pw.println (time+" "+  " AGGREGATE LOADING INITIATED")
           
           //CALLING THE BASE AGGREGATE LOAD PROCESS
           println (time+" "+"OSX CUST AGG - CALLING BASE AGGREGATE LOAD FROM DRIVER CLASS")
           println (time+" "+"OSX CUST AGG - READING LATEST VERSION AVAILABE IN BASE TO TRIGGER CUSTOMER AGGREGATE")
           val sumx_data=sqlContext.read
                           .option("basePath",hdfs_seg_base_path)
                           .parquet(hdfs_agg.concat("/last_refresh_flag=Y"))
                           
           //CALLING THE CUSTOMER AGGREGATE LOAD PROCESS
           println (time+" "+"OSX CUST AGG - CALLING CUSTOMER AGGREGATE LOAD FROM DRIVER CLASS")
           load_customer_agg.osx_seg_customer_agg_load(sc,sqlContext, args(0).toString, sumx_data, dom, acc, hdfs_cust_agg_base_path, overwrite_flag)
           
           println (time+" "+"VERSION CLEANING - CHECK CUSTOMER AGGREGATE TO RETAIN LAST FIVE VERSIONS")
           process_version_cleaning.version_cleaning(sc, args(0).toString(), hdfs_cust_base_path, overwrite_flag)
         
       }
       else if (load_process.equals("GL")){
           
          //CREATE TWO DATAFRAMES BASED ON LAST TWO MONTHS AND PASS WITH GL AGGREGATE PROCESSING
           println (time+" "+"OSX GL AGG - CALLING GL AGGREGATE LOAD FROM DRIVER CLASS")
           println (time+" "+"OSX GL AGG - READING LATEST VERSION AVAILABE IN BASE TO TRIGGER GL AGGREGATE")
           val sumx_data=sqlContext.read
                           .option("basePath",hdfs_seg_base_path)
                           .parquet(hdfs_agg.concat("/last_refresh_flag=Y"))
           
           println (time+" "+"OSX GL AGG - READING LM, LM2, LYSM LATEST VERSION AVAILABE FOR GL AGGREGATE")
           val sumx_data_lm1= sqlContext.read
                                        .option("basePath", hdfs_gl_base_path)
                                        .parquet(hdfs_gl_agg_lm1.concat("/last_refresh_flag=Y"))
                                                 
           
           val sumx_data_lm2= sqlContext.read
                                        .option("basePath", hdfs_gl_base_path)
                                        .parquet(hdfs_gl_agg_lm2.concat("/last_refresh_flag=Y"))
                                        
           
           //CREATE A DATAFRAME BASED ON LAST YEAR SAME MONTH AND PASS WITH GL AGGREGATE PROCESSING                         
           val sumx_data_lysm= sqlContext.read
                                         .option("basePath",hdfs_gl_base_path)
                                         .parquet(hdfs_gl_agg_lysm.concat("/last_refresh_flag=Y"))
                                    
           
           //CALLING THE GL AGGREGATE LOAD PROCESS
           load_gl_agg.osx_seg_gl_agg_load(sc, sqlContext, args(0).toString, sumx_data, sumx_data_lm1, sumx_data_lm2, sumx_data_lysm, 
                                           dom, acc, pc, le, cov_geo, prod, dept, cust_seg, hdfs_gl_agg, overwrite_flag)
                                           
           println (time+" "+"VERSION CLEANING - CHECK GL AGGREGATE TO RETAIN LAST FIVE VERSIONS")
           process_version_cleaning.version_cleaning(sc, args(0).toString(), hdfs_gl_base_path, overwrite_flag)
       }
       else if (load_process.equals("REST")) {
           
           //******************************** TRIGGER CUST AGG
           println (time+" "+"CUST AGGREGATE LOAD INITIATED FROM DRIVER CLASS")
           //pw.println (time+" "+  " AGGREGATE LOADING INITIATED")
           
           //CALLING THE BASE AGGREGATE LOAD PROCESS
           println (time+" "+"OSX CUST AGG - CALLING BASE AGGREGATE LOAD FROM DRIVER CLASS")
           println (time+" "+"OSX CUST AGG - READING LATEST VERSION AVAILABE IN BASE TO TRIGGER CUSTOMER AGGREGATE")
           val sumx_data=sqlContext.read
                           .option("basePath",hdfs_seg_base_path)
                           .parquet(hdfs_agg.concat("/last_refresh_flag=Y"))
           
           //CALLING THE CUSTOMER AGGREGATE LOAD PROCESS
           println (time+" "+"OSX CUST AGG - CALLING CUSTOMER AGGREGATE LOAD FROM DRIVER CLASS")
           load_customer_agg.osx_seg_customer_agg_load(sc, sqlContext, args(0).toString, sumx_data, dom, acc, hdfs_cust_agg_base_path, overwrite_flag)
           
           println (time+" "+"VERSION CLEANING - CHECK CUSTOMER AGGREGATE TO RETAIN LAST FIVE VERSIONS")
           process_version_cleaning.version_cleaning(sc, args(0).toString(), hdfs_cust_base_path, overwrite_flag)
           
           
           //******************************** TRIGGER GL AGG
           //CREATE TWO DATAFRAMES BASED ON LAST TWO MONTHS AND PASS WITH GL AGGREGATE PROCESSING
           println (time+" "+"OSX GL AGG - CALLING GL AGGREGATE LOAD FROM DRIVER CLASS") 
           println (time+" "+"OSX GL AGG - READING LM, LM2, LYSM LATEST VERSION AVAILABE FOR GL AGGREGATE")
           val sumx_data_lm1= sqlContext.read
                                        .option("basePath", hdfs_gl_base_path)
                                        .parquet(hdfs_gl_agg_lm1.concat("/last_refresh_flag=Y"))
                                                 
           
           val sumx_data_lm2= sqlContext.read
                                        .option("basePath", hdfs_gl_base_path)
                                        .parquet(hdfs_gl_agg_lm2.concat("/last_refresh_flag=Y"))
                                        
           
           //CREATE A DATAFRAME BASED ON LAST YEAR SAME MONTH AND PASS WITH GL AGGREGATE PROCESSING                         
           val sumx_data_lysm= sqlContext.read
                                         .option("basePath",hdfs_gl_base_path)
                                         .parquet(hdfs_gl_agg_lysm.concat("/last_refresh_flag=Y"))
                                    
           
           //CALLING THE GL AGGREGATE LOAD PROCESS
           load_gl_agg.osx_seg_gl_agg_load(sc, sqlContext, args(0).toString, sumx_data, sumx_data_lm1, sumx_data_lm2, sumx_data_lysm, 
                                           dom, acc, pc, le, cov_geo, prod, dept, cust_seg, hdfs_gl_agg, overwrite_flag)
                                           
           println (time+" "+"VERSION CLEANING - CHECK GL AGGREGATE TO RETAIN LAST FIVE VERSIONS")
           process_version_cleaning.version_cleaning(sc, args(0).toString(), hdfs_gl_base_path, overwrite_flag)
           
       }
       else if (load_process.equals("ALL")) {
         
           println (time+" "+"BASE AGGREGATE LOAD INITIATED FROM DRIVER CLASS")
           //pw.println (time+" "+  " AGGREGATE LOADING INITIATED")
           
           //******************************** TRIGGER BASE AGG
           //CALLING THE BASE AGGREGATE LOAD PROCESS
           println (time+" "+"OSX BASE AGG - CALLING BASE AGGREGATE LOAD FROM DRIVER CLASS")
           load_base_agg.osx_seg_base_agg_load(sc, sqlContext, args(0).toString, dom, acc, le, osx_cust, osx_cont, hdfs_raw_base_agg, overwrite_flag)
           
           println (time+" "+"VERSION CLEANING - CHECK BASE AGGREGATE TO RETAIN LAST FIVE VERSIONS")
           process_version_cleaning.version_cleaning(sc, args(0).toString(), hdfs_seg_base_path, overwrite_flag)
           
           println (time+" "+"VERSION CLEANING - CHECK TRIAL BALANCE TO RETAIN LAST FIVE VERSIONS")
           process_version_cleaning.version_cleaning(sc, args(0).toString(), hdfs_trial_base_path, overwrite_flag)
           
           
           //******************************** TRIGGER CUST AGG
           println (time+" "+"CUST AGGREGATE LOAD INITIATED FROM DRIVER CLASS")
           //pw.println (time+" "+  " AGGREGATE LOADING INITIATED")
           
           //CALLING THE BASE AGGREGATE LOAD PROCESS
           println (time+" "+"OSX CUST AGG - CALLING BASE AGGREGATE LOAD FROM DRIVER CLASS")
           println (time+" "+"OSX CUST AGG - READING LATEST VERSION AVAILABE IN BASE TO TRIGGER CUSTOMER AGGREGATE")
           val sumx_data=sqlContext.read
                           .option("basePath",hdfs_seg_base_path)
                           .parquet(hdfs_agg.concat("/last_refresh_flag=Y"))
           
           //CALLING THE CUSTOMER AGGREGATE LOAD PROCESS
           println (time+" "+"OSX CUST AGG - CALLING CUSTOMER AGGREGATE LOAD FROM DRIVER CLASS")
           load_customer_agg.osx_seg_customer_agg_load(sc, sqlContext, args(0).toString, sumx_data, dom, acc, hdfs_cust_agg_base_path, overwrite_flag)
           
           println (time+" "+"VERSION CLEANING - CHECK CUSTOMER AGGREGATE TO RETAIN LAST FIVE VERSIONS")
           process_version_cleaning.version_cleaning(sc, args(0).toString(), hdfs_cust_base_path, overwrite_flag)
           
           
           //******************************** TRIGGER GL AGG
           //CREATE TWO DATAFRAMES BASED ON LAST TWO MONTHS AND PASS WITH GL AGGREGATE PROCESSING
           println (time+" "+"OSX GL AGG - CALLING GL AGGREGATE LOAD FROM DRIVER CLASS") 
           println (time+" "+"OSX GL AGG - READING LM, LM2, LYSM LATEST VERSION AVAILABE FOR GL AGGREGATE")
           val sumx_data_lm1= sqlContext.read
                                        .option("basePath", hdfs_gl_base_path)
                                        .parquet(hdfs_gl_agg_lm1.concat("/last_refresh_flag=Y"))
                                                 
           
           val sumx_data_lm2= sqlContext.read
                                        .option("basePath", hdfs_gl_base_path)
                                        .parquet(hdfs_gl_agg_lm2.concat("/last_refresh_flag=Y"))
                                        
           
           //CREATE A DATAFRAME BASED ON LAST YEAR SAME MONTH AND PASS WITH GL AGGREGATE PROCESSING                         
           val sumx_data_lysm= sqlContext.read
                                         .option("basePath",hdfs_gl_base_path)
                                         .parquet(hdfs_gl_agg_lysm.concat("/last_refresh_flag=Y"))
                                    
           
           //CALLING THE GL AGGREGATE LOAD PROCESS
           load_gl_agg.osx_seg_gl_agg_load(sc, sqlContext, args(0).toString, sumx_data, sumx_data_lm1, sumx_data_lm2, sumx_data_lysm, 
                                           dom, acc, pc, le, cov_geo, prod, dept, cust_seg, hdfs_gl_agg, overwrite_flag)
                                           
           println (time+" "+"VERSION CLEANING - CHECK GL AGGREGATE TO RETAIN LAST FIVE VERSIONS")
           process_version_cleaning.version_cleaning(sc, args(0).toString(), hdfs_gl_base_path, overwrite_flag)
           
       }
       else {
           println(time+" "+"INVALID OPTION USED. SHOUD USE BETWEEN (BASE, CUST, GL, ALL) TO TRIGGER LOAD PROCESS")
       }
       
       sc.stop()         
   }
  
}
