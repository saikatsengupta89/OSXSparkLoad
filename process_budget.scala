package sumx_aggregate
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.DataFrame
import java.text.SimpleDateFormat
import java.util.Date
import java.time.LocalDate
import java.time.Period
import java.time.YearMonth
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import sys.process._

object process_budget {
  
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
  
  def getMonthEndDates (from_time_key: Int, to_time_key:Int):ListBuffer[Integer] = {
    
    var list_tk= new ListBuffer[Integer]
    var totMonths=0
    val dtf= DateTimeFormatter.ofPattern("yyyyMMdd")
    val start_date= LocalDate.parse(from_time_key.toString(), dtf)
    val end_date= LocalDate.parse(to_time_key.toString(), dtf)
    
    
    val getMonths= Period.between(start_date, end_date).getMonths
    val getYear  = Period.between(start_date, end_date).getYears
    
    
    if (getYear ==1) {
      totMonths = totMonths +12
    }
    
    if (getMonths > 0) {
      totMonths = totMonths + getMonths
    }
    
    for (i <-0 to totMonths) {
      val dt= start_date.plusMonths(i)
      list_tk.append(dt.format(dtf).toInt)
    }
    
    return list_tk
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
  
  def main(args: Array[String]) {
    
      var version_id =0
      var version_id_lm1=0
      var version_id_lm2=0
      var version_id_lysm=0
      
      var sumx_data_lm1:DataFrame  = null
      var sumx_data_lm2:DataFrame  = null
      var sumx_data_lysm:DataFrame = null
      
      val from_time_key= args(0).toInt
      val to_time_key  = args(1).toInt
      val list_tk      = getMonthEndDates(from_time_key, to_time_key)
      
      val sparkConf = sparkConfig(time)                        
      val sc= new org.apache.spark.SparkContext(sparkConf)   
      val sqlContext= new org.apache.spark.sql.SQLContext(sc)
      
      val fs= FileSystem.get(sc.hadoopConfiguration)
      
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
      
      /* BASE AGGREGATE - BASE PATH HDFS */
      val hdfs_seg_base_path         = "/data/fin_onesumx/fin_fct_osx_segmental_agg_rpt"
      val hdfs_cust_agg_base_path    = "/data/fin_onesumx/fin_fct_osx_segmental_cust_agg_rpt"
      val hdfs_gl_agg_base_path      = "/data/fin_onesumx/fin_fct_osx_segmental_gl_agg_rpt"
      
      
      println (time +" "+  "READING ALL DIMENSIONS REQUIRED FOR BUDGET REBASING")
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
       
      list_tk.foreach( x => {
          val time_key= x
          val time_key_lm1  = lastMonth1TK(x)
          val time_key_lm2  = lastMonth2TK(x)
          val time_key_lysm = lastYearSameMonthTK(x)
          
          val hdfs_raw_budget_data= "/raw/onesumx/budget/T_OUTBOUND_DEAL_BALANCE_AVERAGE_BALANCE_FGB_"+x+".DAT"
          val hdfs_agg= hdfs_seg_base_path+ s"/time_key=$time_key"
          
          //CALLING THE BASE AGGREGATE LOAD PROCESS
          println (time+" "+s"BUDGET REBASE INITIATED FOR TIMEKEY - $time_key")
          load_base_agg.osx_seg_base_agg_load(sc, sqlContext, time_key.toString() , dom, acc, le, osx_cust, osx_cont, hdfs_raw_budget_data, 0, 1)
          
          fs.listStatus(new Path(hdfs_agg.concat("/last_refresh_flag=Y")))
            .filter(_.isDir)
            .map(_.getPath)
            .foreach(x=> {
            version_id+=x.toString.substring(x.toString.lastIndexOf("=")+1).toInt
          })
           
          //CALLING THE BASE AGGREGATE LOAD PROCESS
          println (time+" "+"OSX CUST AGG - READING AMOUNT CLASS MGTBGT FROM LATEST VERSION FROM BASE TO PROCESS CUSTOMER AGGREGATE BUDGET REBASE")
          val sumx_data=sqlContext.read
                                  .option("basePath",hdfs_seg_base_path)
                                  .parquet(hdfs_agg.concat(s"/last_refresh_flag=Y/version_id=$version_id/amount_class=MGTBGT"))
           
          //CALLING THE CUSTOMER AGGREGATE LOAD PROCESS
          println (time+" "+s"OSX CUST AGG - CALLING CUSTOMER AGGREGATE LOAD BUDGET REBASE - $time_key")
          load_customer_agg.osx_seg_customer_agg_load(sc, sqlContext, args(0).toString, sumx_data, dom, acc, hdfs_cust_agg_base_path, 0, 1)
           
           
          //CREATE TWO DATAFRAMES BASED ON LAST TWO MONTHS AND PASS WITH GL AGGREGATE PROCESSING
          println (time+" "+"OSX GL AGG - CALLING GL AGGREGATE LOAD FROM DRIVER CLASS") 
          println (time+" "+"OSX GL AGG - READING LM, LM2, LYSM LATEST VERSION AVAILABE FOR GL AGGREGATE")
          
          val hdfs_gl_agg_lm1  = hdfs_gl_agg_base_path+s"/time_key=$time_key_lm1"
          val hdfs_gl_agg_lm2  = hdfs_gl_agg_base_path+s"/time_key=$time_key_lm2"
          val hdfs_gl_agg_lysm = hdfs_gl_agg_base_path+s"/time_key=$time_key_lysm"
          
          
          if (("hadoop fs -test -d ".concat(hdfs_gl_agg_lm1).!)==0) {
            
               fs.listStatus(new Path(hdfs_gl_agg_lm1.concat("/last_refresh_flag=Y")))
                 .filter(_.isDir)
                 .map(_.getPath)
                 .foreach(x=> {
                 version_id_lm1+=x.toString.substring(x.toString.lastIndexOf("=")+1).toInt
               })
              
               sumx_data_lm1= sqlContext.read
                                        .option("basePath", hdfs_gl_agg_base_path)
                                        .parquet(hdfs_gl_agg_lm1.concat(s"/last_refresh_flag=Y/version_id=$version_id_lm1/amount_class=MGTBGT"))
          }
          else {
               sumx_data_lm1= process_budget_dummyDataSet.get_dummy_set(time_key_lm1, sqlContext)
               sumx_data_lm1.show()
            
          }
          
          if (("hadoop fs -test -d ".concat(hdfs_gl_agg_lm2).!)==0) {
             
               fs.listStatus(new Path(hdfs_gl_agg_lm2.concat("/last_refresh_flag=Y")))
                 .filter(_.isDir)
                 .map(_.getPath)
                 .foreach(x=> {
                 version_id_lm2+=x.toString.substring(x.toString.lastIndexOf("=")+1).toInt
               })
              
               sumx_data_lm2= sqlContext.read
                                        .option("basePath", hdfs_gl_agg_base_path)
                                        .parquet(hdfs_gl_agg_lm2.concat(s"/last_refresh_flag=Y/version_id=$version_id_lm2/amount_class=MGTBGT"))
          }
          else {
               sumx_data_lm2= process_budget_dummyDataSet.get_dummy_set(time_key_lm2, sqlContext)
               sumx_data_lm2.show()
          }
          
          if (("hadoop fs -test -d ".concat(hdfs_gl_agg_lysm).!)==0) {
            
               fs.listStatus(new Path(hdfs_gl_agg_lysm.concat("/last_refresh_flag=Y")))
                 .filter(_.isDir)
                 .map(_.getPath)
                 .foreach(x=> {
                 version_id_lysm+=x.toString.substring(x.toString.lastIndexOf("=")+1).toInt
               })
              
               sumx_data_lysm= sqlContext.read
                                        .option("basePath", hdfs_gl_agg_base_path)
                                        .parquet(hdfs_gl_agg_lysm.concat(s"/last_refresh_flag=Y/version_id=$version_id_lm2/amount_class=MGTBGT"))
          }
          else {
               sumx_data_lysm= process_budget_dummyDataSet.get_dummy_set(time_key_lysm, sqlContext)
               sumx_data_lysm.show()
          }
                                    
           
          //CALLING THE GL AGGREGATE LOAD PROCESS
          load_gl_agg.osx_seg_gl_agg_load(sc, sqlContext, args(0).toString, sumx_data, sumx_data_lm1, sumx_data_lm2, sumx_data_lysm, 
                                           dom, acc, pc, le, cov_geo, prod, dept, cust_seg, hdfs_gl_agg_base_path, 0, 1)
          
      })
      
      println (time + " " + "BUDGET REBASE COMPLETED")
      sc.stop()
           
  }
  
}
