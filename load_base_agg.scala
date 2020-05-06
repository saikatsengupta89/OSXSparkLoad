package sumx_aggregate

import java.sql.DriverManager
import java.util.Properties
import org.apache.log4j._
import org.apache.spark._  
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED

import java.net.Authenticator
import org.apache.hadoop.conf.Configuration

/* for writing logs w.r.t. data load process */
import java.io.PrintWriter
import sys.process._
import org.apache.spark.sql.SaveMode

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer
import sys.process._


object load_base_agg {
  
  def osx_seg_base_agg_load (sc:SparkContext,
                             sqlContext:SQLContext,
                             time_key:String,
                             dom:DataFrame, 
                             acc:DataFrame, 
                             le: DataFrame,
                             osx_cust: DataFrame,
                             osx_cont: DataFrame,
                             hdfs_raw_base_agg:String,
                             overwrite_flag:Integer,
                             refresh_budget:Integer) {
    
    
      import sqlContext.implicits._
      
      val hdfs_seg_base_agg_r= "/data/fin_onesumx/fin_fct_osx_segmental_agg_rpt/time_key="+time_key
      val hdfs_seg_base_agg  = "/data/fin_onesumx/fin_fct_osx_segmental_agg_rpt"
      val hdfs_seg_temp      = "/data/fin_onesumx/temp"
      
      var version_id= 0
      val fs= FileSystem.get(sc.hadoopConfiguration)
      
      //DIRECTORY EXISTS ALREADY. REMOVE THE EXISTING DIRECTORY AND THEN APPEND DATA FOR THE TIMEKEY
      /*
      print(hdfs_seg_base_agg_r)
      println (process_sumx_agg.time+" "+"OSX BASE AGG - SEG BASE DIRECTORY FLAG FOR PROCESSING TIME_KEY :"+ ("hadoop fs -test -d ".concat(hdfs_seg_base_agg_r).!))
      if(("hadoop fs -test -d ".concat(hdfs_seg_base_agg_r).!)==0)  
      {
         println (process_sumx_agg.time+" "+ "OSX BASE AGG - DELETE EXISTING PARTITION FOR RELOAD")
         val remove_dir = "hadoop fs -rm -r ".concat(hdfs_seg_base_agg_r).!
         if(remove_dir==0) {
           
           println (process_sumx_agg.time+" "+"OSX BASE AGG - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE BASE AGG RELOAD. LOAD IN PROGRESS")
           //pw.println (process_sumx_agg.time+" "+"OSX BASE - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE BASE AGG RELOAD. LOAD IN PROGRESS")
         }
      }*/
      
      val data= sc.textFile (hdfs_raw_base_agg)
      val header = data.first()
      
      println (process_sumx_agg.time+" "+  "OSX BASE AGG - DIMENSION TABLES INITIATED FOR LOOKUP")
      //pw.println (process_sumx_agg.time+" "+  " OSX BASE - DIMENSION TABLES INITIATED FOR LOOKUP")
      
      
      osx_cont.registerTempTable("DIM_OSX_CONTRACTS")
      osx_cust.registerTempTable("DIM_OSX_CUSTOMER")
      acc.registerTempTable("DIM_OSX_GL_ACC_FPA")
      dom.registerTempTable("DIM_OSX_DOM")
      le.registerTempTable("DIM_OSX_LEGAL_ENTITY")
      
      
      val gl_acc_fpa_transform= sqlContext.sql(
          "SELECT "+
          "GL_ACCOUNT_ID, "+
          "MAX(GL_ACCOUNT_LEVL1_CODE) GL_ACCOUNT_LEVL1_CODE, "+
          "MAX(GL_ACCOUNT_LEVL1_DESC) GL_ACCOUNT_LEVL1_DESC, "+
          "MAX(GL_ACCOUNT_LEVL2_CODE) GL_ACCOUNT_LEVL2_CODE, "+
          "MAX(GL_ACCOUNT_LEVL2_DESC) GL_ACCOUNT_LEVL2_DESC, "+
          "MAX(GL_ACCOUNT_LEVL3_CODE) GL_ACCOUNT_LEVL3_CODE, "+
          "MAX(GL_ACCOUNT_LEVL3_DESC) GL_ACCOUNT_LEVL3_DESC, "+
          "MAX(GL_ACCOUNT_LEVL4_CODE) GL_ACCOUNT_LEVL4_CODE, "+
          "MAX(GL_ACCOUNT_LEVL4_DESC) GL_ACCOUNT_LEVL4_DESC, "+
          "MAX(GL_ACCOUNT_LEVL5_CODE) GL_ACCOUNT_LEVL5_CODE, "+
          "MAX(GL_ACCOUNT_LEVL5_DESC) GL_ACCOUNT_LEVL5_DESC, "+
          "MAX(GL_ACCOUNT_LEVL6_CODE) GL_ACCOUNT_LEVL6_CODE, "+
          "MAX(GL_ACCOUNT_LEVL6_DESC) GL_ACCOUNT_LEVL6_DESC, "+
          "MAX(GL_ACCOUNT_LEVL7_CODE) GL_ACCOUNT_LEVL7_CODE, "+
          "MAX(GL_ACCOUNT_LEVL7_DESC) GL_ACCOUNT_LEVL7_DESC, "+
          "MAX(GL_ACCOUNT_LEVL8_CODE) GL_ACCOUNT_LEVL8_CODE, "+
          "MAX(GL_ACCOUNT_LEVL8_DESC) GL_ACCOUNT_LEVL8_DESC, "+
          "MAX(GL_ACCOUNT_LEVL9_CODE) GL_ACCOUNT_LEVL9_CODE, "+
          "MAX(GL_ACCOUNT_LEVL9_DESC) GL_ACCOUNT_LEVL9_DESC, "+
          "MAX(GL_ACCOUNT_LEVL10_CODE) GL_ACCOUNT_LEVL10_CODE, "+
          "MAX(GL_ACCOUNT_LEVL10_DESC) GL_ACCOUNT_LEVL10_DESC, "+
          "MAX(GL_PL_FLAG) GL_PL_FLAG, "+
          "MAX(IS_INTERNAL_ACCOUNT) IS_INTERNAL_ACCOUNT, "+
          "MAX(DESCRIPTION) DESCRIPTION, "+
          "MAX(ACCOUNT_CATEGORY) ACCOUNT_CATEGORY, "+
          "MAX(ACCOUNT_SECTION) ACCOUNT_SECTION, "+
          "MAX(POSITION_ACCOUNT) POSITION_ACCOUNT, "+
          "MAX(ACCOUNT_SECTION_CONVERSE) ACCOUNT_SECTION_CONVERSE, "+
          "MAX(TO_BE_REVALUED) TO_BE_REVALUED, "+
          "MAX(ACCOUNT_TYPE) ACCOUNT_TYPE, "+
          "MAX(CLASSIFICATION) CLASSIFICATION, "+
          
          "MAX(PRINCIPAL_ACCRUED) PRINCIPAL_ACCRUED, "+
          "MAX(FAB_IFRS9_FLAG) FAB_IFRS9_FLAG, "+
          "MAX(MOODYS_IFRS9_FLAG) MOODYS_IFRS9_FLAG "+
          "FROM DIM_OSX_GL_ACC_FPA "+
          "GROUP BY GL_ACCOUNT_ID"
      )
      
      gl_acc_fpa_transform.persist(StorageLevel.MEMORY_ONLY_SER)
      gl_acc_fpa_transform.registerTempTable("DIM_OSX_GL_ACCOUNTS_FPA")
           
      println (process_sumx_agg.time+" "+  "OSX BASE AGG - BASE DIMENSION ATTRIBUTES DATAFRAME CREATED")
      //pw.println (process_sumx_agg.time+" "+  " OSX BASE - BASE DIMENSION ATTRIBUTES DATAFRAME CREATED")
      
      val base_dim= data.filter(row=> row != header)                                 
                        .map(row=> {val k= row.split("~"); 
                                          (k(49),k(3).toString,k(5).toString,k(6).toString,k(7).toString, k(8).toString,k(9).toString,k(10).toString, k(11).toString,
                                           k(13).toString,k(14).toString,k(17).toString, k(20).toString,k(21).toString,
                                           k(0).concat(if(k(1).length()>1) k(1) else ("0").concat(k(1))).concat(k(50).split(" ")(1)))
                                  })
                        .toDF("ROW_ID","COMMON_KEY","ENTITY","AMOUNT_CLASS","CUSTOMER_NR", "ACCOUNT_CODE","DEAL_ID","PROFIT_CENTRE","ELEMENT1","ELEMENT3",
                             "ELEMENT4","ELEMENT7","CURRENCY","DOMAIN_ID", "TIME_KEY")
      
      //base_dim.show(20)
      base_dim.registerTempTable("BASE_OSX_DIM_ATTR")
      
      println (process_sumx_agg.time+" "+  "OSX BASE AGG - BASE FACTUAL ATTRIBUTES DATAFRAME CREATED")
      //pw.println (process_sumx_agg.time+" "+  " OSX BASE - BASE FACTUAL ATTRIBUTES DATAFRAME CREATED")
      
      val base_fct= data.filter(row=> row != header)                                 
                        .map(row=> {val k= row.split("~"); 
                                          /*(k(49),k(22).toDouble,k(23).toDouble,k(24).toDouble, k(25).toDouble,k(26).toDouble,k(27).toDouble,
                                           k(28).toDouble,k(29).toDouble,k(30).toDouble, k(31).replace("", "0").toDouble, k(32).replace("", "0").toDouble,
                                           k(33).replace("", "0").toDouble, k(34).replace("", "0").toDouble, k(35).replace("", "0").toDouble,
                                           k(36).replace("", "0").toDouble,k(37).replace("", "0").toDouble,k(38).replace("", "0").toDouble,*/                       
                        								   (k(49),
                        								    (if (k(22).isEmpty()) "0" else k(22)).toDouble,
                                            (if (k(23).isEmpty()) "0" else k(23)).toDouble,
                                            (if (k(24).isEmpty()) "0" else k(24)).toDouble,
                                            (if (k(25).isEmpty()) "0" else k(25)).toDouble,
                                            (if (k(26).isEmpty()) "0" else k(26)).toDouble,
                                            (if (k(27).isEmpty()) "0" else k(27)).toDouble,
                                            (if (k(28).isEmpty()) "0" else k(28)).toDouble,
                                            (if (k(29).isEmpty()) "0" else k(29)).toDouble,
                                            (if (k(30).isEmpty()) "0" else k(30)).toDouble,
                                            (if (k(31).isEmpty()) "0" else k(31)).toDouble, 
                                            (if (k(32).isEmpty()) "0" else k(32)).toDouble,
                                            (if (k(33).isEmpty()) "0" else k(33)).toDouble, 
                                            (if (k(34).isEmpty()) "0" else k(34)).toDouble,
                                            (if (k(35).isEmpty()) "0" else k(35)).toDouble,
                                            (if (k(36).isEmpty()) "0" else k(36)).toDouble,
                                            (if (k(37).isEmpty()) "0" else k(37)).toDouble,
                                            (if (k(38).isEmpty()) "0" else k(38)).toDouble,
                                            (if (k(39).isEmpty()) "0" else k(39)).toDouble
                                           )
                      						       
                                    }
                              )
                         .toDF("ROW_ID","MTD_CCY_AMOUNT","MTD_LCL_AMOUNT","MTD_RPT_AMOUNT","YTD_CCY_AMOUNT","YTD_LCL_AMOUNT","YTD_RPT_AMOUNT","YTD_CCY_AMOUNT_REBASED",
                               "YTD_LCL_AMOUNT_REBASED","YTD_RPT_AMOUNT_REBASED","MTDAB_CCY","MTDAB_LCL","MTDAB_RPT","YTDAB_CCY","YTDAB_LCL","YTDAB_RPT",
                               "LTDAB_CCY","LTDAB_LCL","LTDAB_RPT")
                              
       base_fct.registerTempTable("BASE_OSX_FCT_ATTR")
       //base_fct.show()
       
       println (process_sumx_agg.time+" "+  "OSX BASE AGG - BASE AGGREGATE DATAFRAME CREATED")
       //pw.println (process_sumx_agg.time+" "+  " OSX BASE - BASE AGGREGATE DATAFRAME CREATED")
       
       val base_osx_agg= sqlContext.sql (
            "SELECT "+
            "Q.TIME_KEY, "+
            "Q.SOURCE_SYSTEM_ID, "+
            "Q.CUSTOMER_NUMBER, "+
            "Q.CONTRACT_ID, "+
            "Q.CATEGORY_CODE, "+
            "Q.CURRENCY_CODE, "+
            "Q.BANKING_TYPE, "+
            "Q.GL_ACCOUNT_ID, "+
            "Q.LEGAL_ENTITY, "+
            "Q.PROFIT_CENTRE, "+
            "Q.COMMON_KEY, "+
            "Q.BALANCE_SHEET_TYPE, "+
            "Q.OUTSTANDING_BALANCE_MTD_AED MTD_AED, "+
            "Q.OUTSTANDING_BALANCE_MTD_LCY MTD_LCY, "+
            "Q.OUTSTANDING_BALANCE_MTD_ACY MTD_ACY, "+
            "CASE WHEN Q.BS_PL_FLAG='BS' THEN Q.OUTSTANDING_BALANCE_YTD_AED ELSE Q.OUTSTANDING_BALANCE_MTD_AED END OUTSTANDING_BALANCE_MTD_AED, "+
            "CASE WHEN Q.BS_PL_FLAG='BS' THEN Q.OUTSTANDING_BALANCE_YTD_ACY ELSE Q.OUTSTANDING_BALANCE_MTD_ACY END OUTSTANDING_BALANCE_MTD_ACY, "+
            "CASE WHEN Q.BS_PL_FLAG='BS' THEN Q.OUTSTANDING_BALANCE_YTD_LCY ELSE Q.OUTSTANDING_BALANCE_MTD_LCY END OUTSTANDING_BALANCE_MTD_LCY, "+
            "Q.OUTSTANDING_BALANCE_YTD_ACY, "+
            "Q.OUTSTANDING_BALANCE_YTD_LCY, "+
            "Q.OUTSTANDING_BALANCE_YTD_AED, "+
            "Q.BS_PL_FLAG, "+
            "Q.AVG_BOOK_BAL_LTD_ACY, "+
            "Q.AVG_BOOK_BAL_LTD_LCY, "+
            "Q.AVG_BOOK_BAL_LTD_AED, "+
            "Q.AVG_BOOK_BAL_MTD_ACY, "+
            "Q.AVG_BOOK_BAL_MTD_LCY, "+
            "Q.AVG_BOOK_BAL_MTD_AED, "+
            "Q.AVG_BOOK_BAL_YTD_ACY, "+
            "Q.AVG_BOOK_BAL_YTD_LCY, "+
            "Q.AVG_BOOK_BAL_YTD_AED, "+
            "Q.FINAL_SEGMENT, "+
            "Q.CUSTOMER_SEGMENT_CODE, "+
            "Q.DOMAIN_ID, "+
            "Q.IS_INTERNAL_ACCOUNT, "+
            "Q.BANKING_TYPE_CUSTOMER, "+
            "Q.AMOUNT_CLASS "+
            "FROM "+
            "(SELECT "+
            "  D.COMMON_KEY, "+
            "  D.ENTITY LEGAL_ENTITY, "+
            "  D.AMOUNT_CLASS, "+
            "  TRIM(CASE WHEN INSTR(D.CUSTOMER_NR,'#') <> 0 THEN SUBSTR(D.CUSTOMER_NR, 1, INSTR(D.CUSTOMER_NR,'#')-1) ELSE 'NA' END) BANKING_TYPE_CUSTOMER, " +
            "  SUBSTR(D.CUSTOMER_NR, INSTR(D.CUSTOMER_NR,'#')+1) CUSTOMER_NUMBER, "+
            "  D.ACCOUNT_CODE GL_ACCOUNT_ID, "+
            "  TRIM(CASE WHEN INSTR(D.DEAL_ID,'#') <> 0 THEN SUBSTR(D.DEAL_ID, 1, INSTR(D.DEAL_ID,'#')-1) ELSE 'NA' END) BANKING_TYPE, "+
            "  SUBSTR(D.DEAL_ID, INSTR(D.DEAL_ID,'#')+1) CONTRACT_ID, "+
            "  D.PROFIT_CENTRE, "+
            "  D.ELEMENT1 CATEGORY_CODE, "+
            "  D.ELEMENT3 SOURCE_SYSTEM_ID, "+
            "  D.ELEMENT4 FINAL_SEGMENT, "+
            "  D.ELEMENT7 CUSTOMER_SEGMENT_CODE, "+
            "  D.CURRENCY CURRENCY_CODE, "+
            "  D.DOMAIN_ID, "+
            "  NVL(F.MTD_CCY_AMOUNT,0) OUTSTANDING_BALANCE_MTD_ACY, "+
            "  NVL(F.MTD_LCL_AMOUNT,0) OUTSTANDING_BALANCE_MTD_LCY, "+
            "  NVL(F.MTD_RPT_AMOUNT,0) OUTSTANDING_BALANCE_MTD_AED, "+
            "  NVL(F.YTD_CCY_AMOUNT_REBASED,0) OUTSTANDING_BALANCE_YTD_ACY, "+
            "  NVL(F.YTD_LCL_AMOUNT_REBASED,0) OUTSTANDING_BALANCE_YTD_LCY, "+
            "  NVL(F.YTD_RPT_AMOUNT_REBASED,0) OUTSTANDING_BALANCE_YTD_AED, "+
            "  NVL(F.MTDAB_CCY,0) AVG_BOOK_BAL_MTD_ACY, "+
            "  NVL(F.MTDAB_LCL,0) AVG_BOOK_BAL_MTD_LCY, "+
            "  NVL(F.MTDAB_RPT,0) AVG_BOOK_BAL_MTD_AED, "+
            "  NVL(F.YTDAB_CCY,0) AVG_BOOK_BAL_YTD_ACY, "+
            "  NVL(F.YTDAB_LCL,0) AVG_BOOK_BAL_YTD_LCY, "+
            "  NVL(F.YTDAB_RPT,0) AVG_BOOK_BAL_YTD_AED, "+
            "  NVL(F.LTDAB_CCY,0) AVG_BOOK_BAL_LTD_ACY, "+
            "  NVL(F.LTDAB_LCL,0) AVG_BOOK_BAL_LTD_LCY, "+
            "  NVL(F.LTDAB_RPT,0) AVG_BOOK_BAL_LTD_AED, "+
            "  D.TIME_KEY, "+
            "  ACC.GL_ACCOUNT_LEVL2_DESC BALANCE_SHEET_TYPE, "+
            "  ACC.GL_PL_FLAG BS_PL_FLAG, "+
            "  ACC.IS_INTERNAL_ACCOUNT "+
            "  FROM BASE_OSX_DIM_ATTR D "+
            "  INNER JOIN BASE_OSX_FCT_ATTR F ON D.ROW_ID= F.ROW_ID "+
            "  LEFT OUTER JOIN DIM_OSX_GL_ACCOUNTS_FPA ACC ON ACC.GL_ACCOUNT_ID= D.ACCOUNT_CODE"+
            ") Q"
       )
       
       base_osx_agg.registerTempTable("OSX_SEG_BASE_AGG")
       
       //BELOW QUERY TO RECON DATA
       //sqlContext.sql("select COUNT(1) TOT_COUNT, sum(cast(OUTSTANDING_BALANCE_MTD_ACY as DOUBLE)) from OSX_SEG_BASE_AGG").show()
       
       println (process_sumx_agg.time+" "+  "OSX BASE AGG - BASE AGGREGATE BS DATAFRAME CREATION FROM BASE DATAFRAME")
       //pw.println (process_sumx_agg.time+" "+  " OSX BASE - BASE AGGREGATE BS DATAFRAME CREATION FROM BASE DATAFRAME")
       
       val osx_seg_bs=sqlContext.sql(
              "SELECT "+
              "TIME_KEY, "+
              "AVG_BOOK_BAL_LTD_ACY, "+
              "AVG_BOOK_BAL_LTD_LCY, "+
              "AVG_BOOK_BAL_LTD_AED, "+
              "AVG_BOOK_BAL_MTD_ACY, "+
              "AVG_BOOK_BAL_MTD_LCY, "+
              "AVG_BOOK_BAL_MTD_AED, "+
              "AVG_BOOK_BAL_YTD_ACY, "+
              "AVG_BOOK_BAL_YTD_LCY, "+
              "AVG_BOOK_BAL_YTD_AED, "+
              "MTD_AED, "+
              "MTD_LCY, "+
              "MTD_ACY, "+
              "OUTSTANDING_BALANCE_MTD_ACY, "+
              "OUTSTANDING_BALANCE_MTD_LCY, "+
              "OUTSTANDING_BALANCE_MTD_AED, "+
              "OUTSTANDING_BALANCE_YTD_ACY, "+
              "OUTSTANDING_BALANCE_YTD_LCY, "+
              "OUTSTANDING_BALANCE_YTD_AED, "+
              "BALANCE_SHEET_TYPE, "+
              "BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, "+
              "CATEGORY_CODE, "+
              "COMMON_KEY, "+
              "CONTRACT_ID, "+
              "CURRENCY_CODE, "+
              "CUSTOMER_NUMBER, "+
              "CUSTOMER_SEGMENT_CODE, "+
              "DOMAIN_ID, "+
              "FINAL_SEGMENT, "+
              "GL_ACCOUNT_ID, "+
              "IS_INTERNAL_ACCOUNT, "+
              "LEGAL_ENTITY, "+
              "PROFIT_CENTRE, "+
              "SOURCE_SYSTEM_ID, "+
              "AMOUNT_CLASS, "+
              "RANK() OVER (PARTITION BY BANKING_TYPE, BANKING_TYPE_CUSTOMER, CATEGORY_CODE, CONTRACT_ID, CURRENCY_CODE, CUSTOMER_NUMBER, CUSTOMER_SEGMENT_CODE, "+
              "DOMAIN_ID, FINAL_SEGMENT, IS_INTERNAL_ACCOUNT, LEGAL_ENTITY, PROFIT_CENTRE, SOURCE_SYSTEM_ID, AMOUNT_CLASS ORDER BY OUTSTANDING_BALANCE_MTD_AED, "+
              "GL_ACCOUNT_ID, COMMON_KEY) RANK_BS "+
              "FROM OSX_SEG_BASE_AGG "+
              "WHERE BS_PL_FLAG='BS' "
        )
      
      osx_seg_bs.registerTempTable("OSX_SEG_BS_DATA")
      
      println (process_sumx_agg.time+" "+  "OSX BASE AGG - BASE AGGREGATE PL DATAFRAME CREATION FROM BASE DATAFRAME")
      //pw.println (process_sumx_agg.time+" "+  " OSX BASE - BASE AGGREGATE PL DATAFRAME CREATION FROM BASE DATAFRAME")
      
      val osx_seg_pl= sqlContext.sql (
              "SELECT "+
              "TIME_KEY, "+
              "BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, "+
              "CATEGORY_CODE, "+
              "CONTRACT_ID, "+
              "CURRENCY_CODE, "+
              "CUSTOMER_NUMBER, "+
              "CUSTOMER_SEGMENT_CODE, "+
              "DOMAIN_ID, "+
              "FINAL_SEGMENT, "+
              "AGG.GL_ACCOUNT_ID, "+
              "LEGAL_ENTITY, "+
              "PROFIT_CENTRE, "+
              "SOURCE_SYSTEM_ID, "+
              "AMOUNT_CLASS, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60110000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END GROSS_INTEREST_INCOME, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60120000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END GROSS_INTEREST_EXPENSE, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60141100' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END ASSET_COF, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60141200' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END LP_CHARGE, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60131200' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END LP_CREDIT, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60131300' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END LIABILITY_COF, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60150000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END DERIVATIVES_INCOME, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60210000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END FEE_INCOME,                     "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60230000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END FX_INCOME, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60240000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END NET_INVESTMENT_INCOME, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE IN ('60161000','60250000','60260000') THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END OTH_INCOME, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL4_CODE = '60132000' OR GL.GL_ACCOUNT_LEVL5_CODE = '60131100' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END INTERBRANCH_INCOME, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL4_CODE = '60142000' OR GL.GL_ACCOUNT_LEVL5_CODE = '60141300' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END INTERBRANCH_EXPENSE, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60110000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END GROSS_INTEREST_INCOME_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60120000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END GROSS_INTEREST_EXPENSE_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60141100' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END ASSET_COF_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60141200' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END LP_CHARGE_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60131200' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END LP_CREDIT_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60131300' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END LIABILITY_COF_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60150000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END DERIVATIVES_INCOME_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60210000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END FEE_INCOME_MTD_LCY,                     "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60230000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END FX_INCOME_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60240000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END NET_INVESTMENT_INCOME_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE IN ('60161000','60250000','60260000') THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END OTH_INCOME_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL4_CODE = '60132000' OR GL.GL_ACCOUNT_LEVL5_CODE = '60131100' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END INTERBRANCH_INCOME_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL4_CODE = '60142000' OR GL.GL_ACCOUNT_LEVL5_CODE = '60141300' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END INTERBRANCH_EXPENSE_MTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60110000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END GROSS_INTEREST_INCOME_YTD_AED, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60120000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END GROSS_INTEREST_EXPENSE_YTD_AED, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60141100' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END ASSET_COF_YTD_AED, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60141200' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END LP_CHARGE_YTD_AED, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60131200' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END LP_CREDIT_YTD_AED, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60131300' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END LIABILITY_COF_YTD_AED, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60150000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END DERIVATIVES_INCOME_YTD_AED, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60210000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END FEE_INCOME_YTD_AED,                     "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60230000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END FX_INCOME_YTD_AED, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60240000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END NET_INVESTMENT_INCOME_YTD_AED, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE IN ('60161000','60250000','60260000') THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END OTH_INCOME_YTD_AED, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL4_CODE = '60132000' OR GL.GL_ACCOUNT_LEVL5_CODE = '60131100' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END INTERBRANCH_INCOME_YTD_AED, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL4_CODE = '60142000' OR GL.GL_ACCOUNT_LEVL5_CODE = '60141300' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END INTERBRANCH_EXPENSE_YTD_AED,                    "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60110000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END GROSS_INTEREST_INCOME_YTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60120000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END GROSS_INTEREST_EXPENSE_YTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60141100' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END ASSET_COF_YTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60141200' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END LP_CHARGE_YTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60131200' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END LP_CREDIT_YTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL5_CODE = '60131300' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END LIABILITY_COF_YTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60150000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END DERIVATIVES_INCOME_YTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60210000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END FEE_INCOME_YTD_LCY,                     "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60230000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END FX_INCOME_YTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE = '60240000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END NET_INVESTMENT_INCOME_YTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL3_CODE IN ('60161000','60250000','60260000') THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END OTH_INCOME_YTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL4_CODE = '60132000' OR GL.GL_ACCOUNT_LEVL5_CODE = '60131100' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END INTERBRANCH_INCOME_YTD_LCY, "+
              "CASE WHEN GL.GL_ACCOUNT_LEVL4_CODE = '60142000' OR GL.GL_ACCOUNT_LEVL5_CODE = '60141300' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END INTERBRANCH_EXPENSE_YTD_LCY "+
              "FROM "+
              "(SELECT * "+
              "  FROM OSX_SEG_BASE_AGG "+
              "  WHERE BS_PL_FLAG = 'PL' "+
              "  AND DOMAIN_ID IN "+
              "  (SELECT DOMAIN_ID "+
              "    FROM DIM_OSX_DOM "+
              "    WHERE L1_CODE IN ('100000','200000','400000') "+
              "  ) "+
              ")AGG "+
              "INNER JOIN DIM_OSX_GL_ACCOUNTS_FPA GL ON AGG.GL_ACCOUNT_ID = GL.GL_ACCOUNT_ID "
      )
      
      osx_seg_pl.registerTempTable("OSX_SEG_PL_DATA")
      
      println (process_sumx_agg.time+" "+  "OSX BASE AGG - BASE AGGREGATE BS PL UNION FROM BASE DATAFRAME")
      //pw.println (process_sumx_agg.time+" "+  " OSX BASE - BASE AGGREGATE BS PL UNION FROM BASE DATAFRAME")
      
      if (refresh_budget == 1) {
           
          /* LOOKUP THE DATASET IN THE LAST_REFRESH_FLAG=Y */
          fs.listStatus(new Path(s"$hdfs_seg_base_agg/time_key=$time_key/last_refresh_flag=Y"))
            .filter(_.isDir)
            .map(_.getPath)
            .foreach(x=> {
            version_id+=x.toString.substring(x.toString.lastIndexOf("=")+1).toInt
          })
           
          val exist_dir = "hadoop fs -test -d ".concat(s"$hdfs_seg_base_agg/time_key=$time_key/last_refresh_flag=Y/version_id=$version_id/amount_class=MGTBGT").!
           
           if (exist_dir == 0) {
              println (process_sumx_agg.time+" "+  "OSX BASE AGG - REMOVE AMOUNT CLASS - MGTBGT BEFORE BUDGET REBASE")
              val remove_dir = "hadoop fs -rm -r ".concat(s"$hdfs_seg_base_agg/time_key=$time_key/last_refresh_flag=Y/version_id=$version_id/amount_class=MGTBGT").!
              if(remove_dir==0) {
               
                println (process_sumx_agg.time+" "+  "OSX BASE AGG - MGTBGT DIRECTORY FOR "+time_key+" IS REMOVED")
                //pw.println ("OSX BASE - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE BASE AGG RELOAD. LOAD IN PROGRESS")
              }
           }
           
          println(process_sumx_agg.time+" "+"OSX BASE AGG - CREATING DUMMY TEMP VIEW WITH LATEST VERSION ID FOR BUDGET REFRESH")
          val last_version= sqlContext.sql (s"SELECT $version_id AS VERSION_ID")
          last_version.registerTempTable("LKP_LAST_VERSION")
          
          val osx_seg_base= sqlContext.sql (
                  "SELECT  "+
                  "  NVL(BS.TIME_KEY, PL.TIME_KEY) TIME_KEY,  "+
                  "  CAST(VER.VERSION_ID AS INT) VERSION_ID, "+
                  "  NVL(BS.AMOUNT_CLASS,PL.AMOUNT_CLASS) AMOUNT_CLASS,  "+
                  "  CAST(NVL(ASSET_COF, 0 ) AS DOUBLE) ASSET_COF,  "+
                  "  CAST(NVL(ASSET_COF_MTD_LCY, 0 ) AS DOUBLE) ASSET_COF_MTD_LCY,  "+
                  "  CAST(NVL(ASSET_COF_YTD_AED, 0 ) AS DOUBLE) ASSET_COF_YTD_AED,  "+
                  "  CAST(NVL(ASSET_COF_YTD_LCY, 0 ) AS DOUBLE) ASSET_COF_YTD_LCY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_LTD_ACY, 0 ) AS DOUBLE) AVG_BOOK_BAL_LTD_ACY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_LTD_AED, 0 ) AS DOUBLE) AVG_BOOK_BAL_LTD_AED,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_LTD_LCY, 0 ) AS DOUBLE) AVG_BOOK_BAL_LTD_LCY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_MTD_ACY, 0 ) AS DOUBLE) AVG_BOOK_BAL_MTD_ACY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_MTD_AED, 0 ) AS DOUBLE) AVG_BOOK_BAL_MTD_AED,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_MTD_LCY, 0 ) AS DOUBLE) AVG_BOOK_BAL_MTD_LCY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_YTD_ACY, 0 ) AS DOUBLE) AVG_BOOK_BAL_YTD_ACY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_YTD_AED, 0 ) AS DOUBLE) AVG_BOOK_BAL_YTD_AED,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_YTD_LCY, 0 ) AS DOUBLE) AVG_BOOK_BAL_YTD_LCY,  "+
                  "  NVL(BS.BALANCE_SHEET_TYPE,'BS-PL INSERT') BALANCE_SHEET_TYPE,  "+
                  "  NVL(BS.BANKING_TYPE,PL.BANKING_TYPE) BANKING_TYPE,  "+
                  "  NVL(BS.BANKING_TYPE_CUSTOMER,PL.BANKING_TYPE_CUSTOMER) BANKING_TYPE_CUSTOMER,  "+
                  "  'BS' BS_PL_FLAG,  "+
                  "  NVL(BS.CATEGORY_CODE,PL.CATEGORY_CODE) CATEGORY_CODE,  "+
                  "  NVL(BS.COMMON_KEY, 0 ) COMMON_KEY,  "+
                  "  NVL(BS.CONTRACT_ID,PL.CONTRACT_ID) CONTRACT_ID,  "+
                  "  NVL(BS.CURRENCY_CODE,PL.CURRENCY_CODE) CURRENCY_CODE,  "+
                  "  NVL(BS.CUSTOMER_NUMBER,PL.CUSTOMER_NUMBER) CUSTOMER_NUMBER,  "+
                  "  NVL(BS.CUSTOMER_SEGMENT_CODE,PL.CUSTOMER_SEGMENT_CODE) CUSTOMER_SEGMENT_CODE,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME, 0 ) AS DOUBLE) DERIVATIVES_INCOME,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME_MTD_LCY, 0 ) AS DOUBLE) DERIVATIVES_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME_YTD_AED, 0 ) AS DOUBLE) DERIVATIVES_INCOME_YTD_AED,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME_YTD_LCY, 0 ) AS DOUBLE) DERIVATIVES_INCOME_YTD_LCY,  "+
                  "  NVL(BS.DOMAIN_ID,PL.DOMAIN_ID) DOMAIN_ID,  "+
                  "  CAST(NVL(FEE_INCOME, 0 ) AS DOUBLE) FEE_INCOME,  "+
                  "  CAST(NVL(FEE_INCOME_MTD_LCY, 0 ) AS DOUBLE) FEE_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(FEE_INCOME_YTD_AED, 0 ) AS DOUBLE) FEE_INCOME_YTD_AED,  "+
                  "  CAST(NVL(FEE_INCOME_YTD_LCY, 0 ) AS DOUBLE) FEE_INCOME_YTD_LCY,  "+
                  "  NVL(BS.FINAL_SEGMENT,PL.FINAL_SEGMENT) FINAL_SEGMENT,  "+
                  "  CAST(NVL(FX_INCOME, 0 ) AS DOUBLE) FX_INCOME,  "+
                  "  CAST(NVL(FX_INCOME_MTD_LCY, 0 ) AS DOUBLE) FX_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(FX_INCOME_YTD_AED, 0 ) AS DOUBLE) FX_INCOME_YTD_AED,  "+
                  "  CAST(NVL(FX_INCOME_YTD_LCY, 0 ) AS DOUBLE) FX_INCOME_YTD_LCY,  "+
                  "  NVL(BS.GL_ACCOUNT_ID,'999999') GL_ACCOUNT_ID,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE_MTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE_MTD_LCY,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE_YTD_AED, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_AED,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE_YTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_LCY,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME_MTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME_YTD_AED, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME_YTD_AED,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME_YTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE_MTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE_MTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE_YTD_AED, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE_YTD_AED,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE_YTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE_YTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME, 0 ) AS DOUBLE) INTERBRANCH_INCOME,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME_MTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME_YTD_AED, 0 ) AS DOUBLE) INTERBRANCH_INCOME_YTD_AED,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME_YTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_INCOME_YTD_LCY,  "+
                  "  NVL(BS.IS_INTERNAL_ACCOUNT,'999999') IS_INTERNAL_ACCOUNT,  "+
                  "  NVL(BS.LEGAL_ENTITY,PL.LEGAL_ENTITY) LEGAL_ENTITY,  "+
                  "  CAST(NVL(LIABILITY_COF, 0 ) AS DOUBLE) LIABILITY_COF,  "+
                  "  CAST(NVL(LIABILITY_COF_MTD_LCY, 0 ) AS DOUBLE) LIABILITY_COF_MTD_LCY,  "+
                  "  CAST(NVL(LIABILITY_COF_YTD_AED, 0 ) AS DOUBLE) LIABILITY_COF_YTD_AED,  "+
                  "  CAST(NVL(LIABILITY_COF_YTD_LCY, 0 ) AS DOUBLE) LIABILITY_COF_YTD_LCY,  "+
                  "  CAST(NVL(LP_CHARGE, 0 ) AS DOUBLE) LP_CHARGE,  "+
                  "  CAST(NVL(LP_CHARGE_MTD_LCY, 0 ) AS DOUBLE) LP_CHARGE_MTD_LCY,  "+
                  "  CAST(NVL(LP_CHARGE_YTD_AED, 0 ) AS DOUBLE) LP_CHARGE_YTD_AED,  "+
                  "  CAST(NVL(LP_CHARGE_YTD_LCY, 0 ) AS DOUBLE) LP_CHARGE_YTD_LCY,  "+
                  "  CAST(NVL(LP_CREDIT, 0 ) AS DOUBLE) LP_CREDIT,  "+
                  "  CAST(NVL(LP_CREDIT_MTD_LCY, 0 ) AS DOUBLE) LP_CREDIT_MTD_LCY,  "+
                  "  CAST(NVL(LP_CREDIT_YTD_AED, 0 ) AS DOUBLE) LP_CREDIT_YTD_AED,  "+
                  "  CAST(NVL(LP_CREDIT_YTD_LCY, 0 ) AS DOUBLE) LP_CREDIT_YTD_LCY,  "+
                  "  CAST(NVL(MTD_ACY, 0 ) AS DOUBLE) MTD_ACY,  "+
                  "  CAST(NVL(MTD_AED, 0 ) AS DOUBLE) MTD_AED,  "+
                  "  CAST(NVL(MTD_LCY, 0 ) AS DOUBLE) MTD_LCY,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME, 0 ) AS DOUBLE) NET_INTEREST_INCOME,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME_MTD_LCY, 0 ) AS DOUBLE) NET_INTEREST_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME_YTD_AED, 0 ) AS DOUBLE) NET_INTEREST_INCOME_YTD_AED,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME_YTD_LCY, 0 ) AS DOUBLE) NET_INTEREST_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME_MTD_LCY, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME_YTD_AED, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME_YTD_AED,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME_YTD_LCY, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(NET_PL, 0 ) AS DOUBLE) NET_PL,  "+
                  "  CAST(NVL(NET_PL_MTD_LCY, 0 ) AS DOUBLE) NET_PL_MTD_LCY,  "+
                  "  CAST(NVL(NET_PL_YTD_AED, 0 ) AS DOUBLE) NET_PL_YTD_AED,  "+
                  "  CAST(NVL(NET_PL_YTD_LCY, 0 ) AS DOUBLE) NET_PL_YTD_LCY,  "+
                  "  CAST(NVL(OTH_INCOME, 0 ) AS DOUBLE) OTH_INCOME,  "+
                  "  CAST(NVL(OTH_INCOME_MTD_LCY, 0 ) AS DOUBLE) OTH_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(OTH_INCOME_YTD_AED, 0 ) AS DOUBLE) OTH_INCOME_YTD_AED,  "+
                  "  CAST(NVL(OTH_INCOME_YTD_LCY, 0 ) AS DOUBLE) OTH_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_MTD_ACY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_MTD_ACY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_MTD_AED, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_MTD_AED,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_MTD_LCY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_MTD_LCY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_YTD_ACY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_YTD_ACY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_YTD_AED, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_YTD_AED,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_YTD_LCY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_YTD_LCY,  "+
                  "  NVL(BS.PROFIT_CENTRE,PL.PROFIT_CENTRE) PROFIT_CENTRE,  "+
                  "  NVL(BS.SOURCE_SYSTEM_ID,PL.SOURCE_SYSTEM_ID) SOURCE_SYSTEM_ID,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_MTD_AED, 0 ) AS DOUBLE) TOTAL_FTP_AMT_MTD_AED,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_MTD_LCY, 0 ) AS DOUBLE) TOTAL_FTP_AMT_MTD_LCY,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_YTD_AED, 0 ) AS DOUBLE) TOTAL_FTP_AMT_YTD_AED,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_YTD_LCY, 0 ) AS DOUBLE) TOTAL_FTP_AMT_YTD_LCY,  "+
                  "  CAST(NVL((NET_INTEREST_INCOME/(CASE WHEN AVG_BOOK_BAL_MTD_AED = 0 THEN 1 ELSE AVG_BOOK_BAL_MTD_AED END))*(365/(DAY(LAST_DAY(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP))))),0) AS DOUBLE) NET_INT_MARGIN,  "+
                  "  CAST(NVL((NET_INTEREST_INCOME_MTD_LCY/(CASE WHEN AVG_BOOK_BAL_MTD_LCY = 0 THEN 1 ELSE AVG_BOOK_BAL_MTD_LCY END))*(365/(DAY(LAST_DAY(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP))))),0) AS DOUBLE) NET_INT_MARGIN_MTD_LCY, "+
                  "  CAST(NVL((NET_INTEREST_INCOME_YTD_AED/(CASE WHEN AVG_BOOK_BAL_YTD_AED = 0 THEN 1 ELSE AVG_BOOK_BAL_YTD_AED END))*(365/(DATEDIFF(TO_DATE(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP)), TO_DATE(TRUNC(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP), 'year')))+1)),0) AS DOUBLE) NET_INT_MARGIN_YTD_AED, "+
                  "  CAST(NVL((NET_INTEREST_INCOME_YTD_LCY/(CASE WHEN AVG_BOOK_BAL_YTD_LCY = 0 THEN 1 ELSE AVG_BOOK_BAL_YTD_LCY END))*(365/(DATEDIFF(TO_DATE(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP)), TO_DATE(TRUNC(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP), 'year')))+1)),0) AS DOUBLE)  NET_INT_MARGIN_YTD_LCY, "+
                  " 'Y' AS LAST_REFRESH_FLAG "+
                  "FROM  "+
                  "  (SELECT * FROM OSX_SEG_BS_DATA )BS  "+
                  "FULL OUTER JOIN  "+
                  "  (SELECT   "+
                  "    Q3.TIME_KEY,  "+
                  "    Q3.AMOUNT_CLASS,  "+
                  "    Q3.BANKING_TYPE,  "+
                  "    Q3.BANKING_TYPE_CUSTOMER,  "+
                  "    Q3.CATEGORY_CODE,  "+
                  "    Q3.CONTRACT_ID,  "+
                  "    Q3.CURRENCY_CODE,  "+
                  "    Q3.CUSTOMER_NUMBER,  "+
                  "    Q3.CUSTOMER_SEGMENT_CODE,  "+
                  "    Q3.DOMAIN_ID,  "+
                  "    Q3.FINAL_SEGMENT,  "+
                  "    Q3.LEGAL_ENTITY,  "+
                  "    Q3.PROFIT_CENTRE,  "+
                  "    Q3.SOURCE_SYSTEM_ID,  "+
                  "    SUM(Q3.ASSET_COF) ASSET_COF,  "+
                  "    SUM(Q3.ASSET_COF_MTD_LCY) ASSET_COF_MTD_LCY,  "+
                  "    SUM(Q3.ASSET_COF_YTD_AED) ASSET_COF_YTD_AED,  "+
                  "    SUM(Q3.ASSET_COF_YTD_LCY) ASSET_COF_YTD_LCY,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME) DERIVATIVES_INCOME,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME_MTD_LCY) DERIVATIVES_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME_YTD_AED) DERIVATIVES_INCOME_YTD_AED,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME_YTD_LCY) DERIVATIVES_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.FEE_INCOME) FEE_INCOME,  "+
                  "    SUM(Q3.FEE_INCOME_MTD_LCY) FEE_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.FEE_INCOME_YTD_AED) FEE_INCOME_YTD_AED,  "+
                  "    SUM(Q3.FEE_INCOME_YTD_LCY) FEE_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.FX_INCOME) FX_INCOME,  "+
                  "    SUM(Q3.FX_INCOME_MTD_LCY) FX_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.FX_INCOME_YTD_AED) FX_INCOME_YTD_AED,  "+
                  "    SUM(Q3.FX_INCOME_YTD_LCY) FX_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE) GROSS_INTEREST_EXPENSE,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE_MTD_LCY) GROSS_INTEREST_EXPENSE_MTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE_YTD_AED) GROSS_INTEREST_EXPENSE_YTD_AED,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE_YTD_LCY) GROSS_INTEREST_EXPENSE_YTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME) GROSS_INTEREST_INCOME,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME_MTD_LCY) GROSS_INTEREST_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME_YTD_AED) GROSS_INTEREST_INCOME_YTD_AED,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME_YTD_LCY) GROSS_INTEREST_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE) INTERBRANCH_EXPENSE,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE_MTD_LCY) INTERBRANCH_EXPENSE_MTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE_YTD_AED) INTERBRANCH_EXPENSE_YTD_AED,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE_YTD_LCY) INTERBRANCH_EXPENSE_YTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME) INTERBRANCH_INCOME,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME_MTD_LCY) INTERBRANCH_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME_YTD_AED) INTERBRANCH_INCOME_YTD_AED,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME_YTD_LCY) INTERBRANCH_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.LIABILITY_COF) LIABILITY_COF,  "+
                  "    SUM(Q3.LIABILITY_COF_MTD_LCY) LIABILITY_COF_MTD_LCY,  "+
                  "    SUM(Q3.LIABILITY_COF_YTD_AED) LIABILITY_COF_YTD_AED,  "+
                  "    SUM(Q3.LIABILITY_COF_YTD_LCY) LIABILITY_COF_YTD_LCY,  "+
                  "    SUM(Q3.LP_CHARGE) LP_CHARGE,  "+
                  "    SUM(Q3.LP_CHARGE_MTD_LCY) LP_CHARGE_MTD_LCY,  "+
                  "    SUM(Q3.LP_CHARGE_YTD_AED) LP_CHARGE_YTD_AED,  "+
                  "    SUM(Q3.LP_CHARGE_YTD_LCY) LP_CHARGE_YTD_LCY,  "+
                  "    SUM(Q3.LP_CREDIT) LP_CREDIT,  "+
                  "    SUM(Q3.LP_CREDIT_MTD_LCY) LP_CREDIT_MTD_LCY,  "+
                  "    SUM(Q3.LP_CREDIT_YTD_AED) LP_CREDIT_YTD_AED,  "+
                  "    SUM(Q3.LP_CREDIT_YTD_LCY) LP_CREDIT_YTD_LCY,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME) NET_INTEREST_INCOME,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME_MTD_LCY) NET_INTEREST_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME_YTD_AED) NET_INTEREST_INCOME_YTD_AED,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME_YTD_LCY) NET_INTEREST_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME) NET_INVESTMENT_INCOME,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME_MTD_LCY) NET_INVESTMENT_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME_YTD_AED) NET_INVESTMENT_INCOME_YTD_AED,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME_YTD_LCY) NET_INVESTMENT_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.NET_PL) NET_PL,  "+
                  "    SUM(Q3.NET_PL_MTD_LCY) NET_PL_MTD_LCY,  "+
                  "    SUM(Q3.NET_PL_YTD_AED) NET_PL_YTD_AED,  "+
                  "    SUM(Q3.NET_PL_YTD_LCY) NET_PL_YTD_LCY,  "+
                  "    SUM(Q3.OTH_INCOME) OTH_INCOME,  "+
                  "    SUM(Q3.OTH_INCOME_MTD_LCY) OTH_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.OTH_INCOME_YTD_AED) OTH_INCOME_YTD_AED,  "+
                  "    SUM(Q3.OTH_INCOME_YTD_LCY) OTH_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_MTD_AED) TOTAL_FTP_AMT_MTD_AED,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_MTD_LCY) TOTAL_FTP_AMT_MTD_LCY,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_YTD_AED) TOTAL_FTP_AMT_YTD_AED,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_YTD_LCY) TOTAL_FTP_AMT_YTD_LCY,  "+
                  "    1 RANK_PL  "+
                  "  FROM  "+
                  "    (SELECT Q2.*,  "+
                  "      (Q2.NET_INTEREST_INCOME        +Q2.DERIVATIVES_INCOME+Q2.FEE_INCOME+Q2.FX_INCOME+Q2.NET_INVESTMENT_INCOME+Q2.OTH_INCOME+Q2.INTERBRANCH_INCOME+Q2.INTERBRANCH_EXPENSE) NET_PL,  "+
                  "      (Q2.NET_INTEREST_INCOME_MTD_LCY+Q2.DERIVATIVES_INCOME_MTD_LCY+Q2.FEE_INCOME_MTD_LCY+Q2.FX_INCOME_MTD_LCY+Q2.NET_INVESTMENT_INCOME_MTD_LCY+Q2.OTH_INCOME_MTD_LCY+Q2.INTERBRANCH_INCOME_MTD_LCY+Q2.INTERBRANCH_EXPENSE_MTD_LCY) NET_PL_MTD_LCY,  "+
                  "      (Q2.NET_INTEREST_INCOME_YTD_AED+Q2.DERIVATIVES_INCOME_YTD_AED+Q2.FEE_INCOME_YTD_AED+Q2.FX_INCOME_YTD_AED+Q2.NET_INVESTMENT_INCOME_YTD_AED+Q2.OTH_INCOME_YTD_AED+Q2.INTERBRANCH_INCOME_YTD_AED+Q2.INTERBRANCH_EXPENSE_YTD_AED) NET_PL_YTD_AED,  "+
                  "      (Q2.NET_INTEREST_INCOME_YTD_LCY+Q2.DERIVATIVES_INCOME_YTD_LCY+Q2.FEE_INCOME_YTD_LCY+Q2.FX_INCOME_YTD_LCY+Q2.NET_INVESTMENT_INCOME_YTD_LCY+Q2.OTH_INCOME_YTD_LCY+Q2.INTERBRANCH_INCOME_YTD_LCY+Q2.INTERBRANCH_EXPENSE_YTD_LCY) NET_PL_YTD_LCY  "+
                  "    FROM  "+
                  "      (SELECT Q1.*,  "+
                  "        (Q1.GROSS_INTEREST_INCOME         + Q1.ASSET_COF + Q1.LP_CHARGE + Q1.GROSS_INTEREST_EXPENSE + Q1.LIABILITY_COF + Q1.LP_CREDIT) NET_INTEREST_INCOME,  "+
                  "        (Q1.GROSS_INTEREST_INCOME_MTD_LCY + Q1.ASSET_COF_MTD_LCY + Q1.LP_CHARGE_MTD_LCY + Q1.GROSS_INTEREST_EXPENSE_MTD_LCY + Q1.LIABILITY_COF_MTD_LCY + Q1.LP_CREDIT_MTD_LCY) NET_INTEREST_INCOME_MTD_LCY,  "+
                  "        (Q1.GROSS_INTEREST_INCOME_YTD_AED + Q1.ASSET_COF_YTD_AED + Q1.LP_CHARGE_YTD_AED + Q1.GROSS_INTEREST_EXPENSE_YTD_AED + Q1.LIABILITY_COF_YTD_AED + Q1.LP_CREDIT_YTD_AED) NET_INTEREST_INCOME_YTD_AED,  "+
                  "        (Q1.GROSS_INTEREST_INCOME_YTD_LCY + Q1.ASSET_COF_YTD_LCY + Q1.LP_CHARGE_YTD_LCY + Q1.GROSS_INTEREST_EXPENSE_YTD_LCY + Q1.LIABILITY_COF_YTD_LCY + Q1.LP_CREDIT_YTD_LCY) NET_INTEREST_INCOME_YTD_LCY,  "+
                  "        (Q1.ASSET_COF                     + Q1.LP_CHARGE + Q1.LIABILITY_COF + Q1.LP_CREDIT) TOTAL_FTP_AMT_MTD_AED,  "+
                  "        (Q1.ASSET_COF_MTD_LCY             + Q1.LP_CHARGE_MTD_LCY + Q1.LIABILITY_COF_MTD_LCY + Q1.LP_CREDIT_MTD_LCY) TOTAL_FTP_AMT_MTD_LCY,  "+
                  "        (Q1.ASSET_COF_YTD_AED             + Q1.LP_CHARGE_YTD_AED + Q1.LIABILITY_COF_YTD_AED + Q1.LP_CREDIT_YTD_AED) TOTAL_FTP_AMT_YTD_AED,  "+
                  "        (Q1.ASSET_COF_YTD_LCY             + Q1.LP_CHARGE_YTD_LCY + Q1.LIABILITY_COF_YTD_LCY + Q1.LP_CREDIT_YTD_LCY) TOTAL_FTP_AMT_YTD_LCY  "+
                  "      FROM  "+
                  "        (SELECT * FROM OSX_SEG_PL_DATA )Q1  "+
                  "      )Q2  "+
                  "    )Q3  "+
                  "  group by Q3.AMOUNT_CLASS, Q3.BANKING_TYPE, Q3.BANKING_TYPE_CUSTOMER, Q3.CATEGORY_CODE, Q3.CONTRACT_ID, Q3.CURRENCY_CODE, Q3.CUSTOMER_NUMBER, Q3.CUSTOMER_SEGMENT_CODE, Q3.DOMAIN_ID, Q3.FINAL_SEGMENT, Q3.LEGAL_ENTITY, Q3.PROFIT_CENTRE, Q3.SOURCE_SYSTEM_ID, 1  "+
                  ")PL ON BS.AMOUNT_CLASS     = PL.AMOUNT_CLASS  "+
                  "AND BS.BANKING_TYPE          = PL.BANKING_TYPE  "+
                  "AND BS.BANKING_TYPE_CUSTOMER = PL.BANKING_TYPE_CUSTOMER  "+
                  "AND BS.CATEGORY_CODE         = PL.CATEGORY_CODE  "+
                  "AND BS.CONTRACT_ID           = PL.CONTRACT_ID  "+
                  "AND BS.CURRENCY_CODE         = PL.CURRENCY_CODE  "+
                  "AND BS.CUSTOMER_NUMBER       = PL.CUSTOMER_NUMBER  "+
                  "AND BS.CUSTOMER_SEGMENT_CODE = PL.CUSTOMER_SEGMENT_CODE  "+
                  "AND BS.DOMAIN_ID             = PL.DOMAIN_ID  "+
                  "AND BS.FINAL_SEGMENT         = PL.FINAL_SEGMENT  "+
                  "AND BS.LEGAL_ENTITY          = PL.LEGAL_ENTITY  "+
                  "AND BS.PROFIT_CENTRE         = PL.PROFIT_CENTRE  "+
                  "AND BS.SOURCE_SYSTEM_ID      = PL.SOURCE_SYSTEM_ID  "+
                  "AND RANK_BS                  = RANK_PL "+
                  "CROSS JOIN LKP_LAST_VERSION VER "+
                  "UNION ALL "+
                  "SELECT  "+
                  "    TIME_KEY, "+
                  "    CAST(VER.VERSION_ID AS INT) VERSION_ID, "+
                  "    AMOUNT_CLASS, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    CAST(AVG_BOOK_BAL_LTD_ACY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_LTD_AED AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_LTD_LCY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_MTD_ACY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_MTD_AED AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_MTD_LCY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_YTD_ACY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_YTD_AED AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_YTD_LCY AS DOUBLE), "+
                  "    BALANCE_SHEET_TYPE, "+
                  "    BANKING_TYPE, "+
                  "    BANKING_TYPE_CUSTOMER, "+
                  "    BS_PL_FLAG, "+
                  "    CATEGORY_CODE, "+
                  "    COMMON_KEY, "+
                  "    CONTRACT_ID, "+
                  "    CURRENCY_CODE, "+
                  "    CUSTOMER_NUMBER, "+
                  "    CUSTOMER_SEGMENT_CODE, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    DOMAIN_ID, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    FINAL_SEGMENT, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    GL_ACCOUNT_ID, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    IS_INTERNAL_ACCOUNT, "+
                  "    LEGAL_ENTITY, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    MTD_ACY, "+
                  "    MTD_AED, "+
                  "    MTD_LCY, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    CAST(OUTSTANDING_BALANCE_MTD_ACY AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_MTD_AED AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_MTD_LCY AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_YTD_ACY AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_YTD_AED AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_YTD_LCY AS DOUBLE), "+
                  "    PROFIT_CENTRE, "+
                  "    SOURCE_SYSTEM_ID, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  " 'Y' AS LAST_REFRESH_FLAG "+
                  "FROM OSX_SEG_BASE_AGG AGG "+
                  "CROSS JOIN LKP_LAST_VERSION VER "+
                  "WHERE BS_PL_FLAG = 'PL' "
          )
          
          println (process_sumx_agg.time+" "+  "OSX BASE AGG - WRITING BUDGET REBASE TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_seg_base_agg)
          osx_seg_base.repartition(200)
                        .write
                        .partitionBy("time_key", "last_refresh_flag", "version_id", "amount_class", "domain_id")
                        .mode("append")
                        .format("parquet")
                        .option("compression","snappy")
                        .save(hdfs_seg_base_agg)
          
          //osx_seg_base.show(10)
          //println("Total Budget Records : "+ osx_seg_base.count())
          println (process_sumx_agg.time+" "+ "OSX BASE AGG - BUDGET REBASE COMPLETED")
          
          //CALLING THE TRIAL BALANCE PROCESSING FUNCTION
          println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - LOAD CALLED FOR REBASE BUDGET")
          load_trial_balance.osx_seg_trial_balance_load(sc, sqlContext, time_key, dom, acc, le, osx_cust, osx_cont, osx_seg_base, overwrite_flag, 1)
       }
       
       else if ((overwrite_flag==0 && ("hadoop fs -test -d ".concat(hdfs_seg_base_agg_r).!)==0))
       {
          
          println (process_sumx_agg.time+" "+  "OSX BASE AGG - PROCESSING ADDITIONAL VERSION LOAD OF BASE DATASET. APPLICABLE ONLY FOR MONTHLY LOAD SCENARIO" )
          
          /* LOOKUP THE DATASET IN THE LAST_REFRESH_FLAG=Y */
          fs.listStatus(new Path(s"$hdfs_seg_base_agg/time_key=$time_key/last_refresh_flag=Y"))
            .filter(_.isDir)
            .map(_.getPath)
            .foreach(x=> {
             version_id+=x.toString.substring(x.toString.lastIndexOf("=")+1).toInt
          })
          
          /* CHECK IF THERE IS FILE AVAILABLE THEN MOVE THE FILE TO LAST_REFRESH_FLAG=N */
          println(process_sumx_agg.time+" "+"OSX BASE AGG - MOVING THE LATEST VERSION FILE FROM LAST REFRESH FLAG Y TO N")
          if (version_id > 0) {
             
             val exist_dir = "hadoop fs -test -d ".concat(s"$hdfs_seg_base_agg/time_key=$time_key/last_refresh_flag=N").!
             if (exist_dir==1) {
                  "hadoop fs -mkdir ".concat(s"$hdfs_seg_base_agg/time_key=$time_key/last_refresh_flag=N").!
             }
                         
             val move_last_dir = (s"hadoop fs -mv $hdfs_seg_base_agg/time_key=$time_key/last_refresh_flag=Y/version_id=$version_id"+" "+
                                  s"$hdfs_seg_base_agg/time_key=$time_key/last_refresh_flag=N/version_id=$version_id").!
                                  
             if (move_last_dir==0){
                println(process_sumx_agg.time+" "+"OSX BASE AGG - FILE MOVED SUCCESSFULLY")
             }
             else {
                println(process_sumx_agg.time+" "+"OSX BASE AGG - FILE DID NOT MOVE SUCCESSFULLY. TERMINATING THE PROCESS")
             }
             
          }
          else {
                println(process_sumx_agg.time+" "+"OSX BASE AGG - THERE WERE NO FILE IN LAST REFRESH FLAG FOLDER PATH. THIS IS AN EXCEPTION. TERMINATING PROCESS")
                System.exit(0)
          }
                    
          /* CREATING A TEMPORARY LOOKUP TABLE WITH LATEST VERSION ID FOR BELOW OPERATIONS */
          println(process_sumx_agg.time+" "+"OSX BASE AGG - CREATING DUMMY TEMP VIEW WITH PREVIOUS VERSION ID FOR CURRENT VERSION LOAD")
          val last_version= sqlContext.sql (s"SELECT $version_id AS VERSION_ID")
          last_version.registerTempTable("LKP_LAST_VERSION")
          
          val osx_seg_base= sqlContext.sql (
                  "SELECT  "+
                  "  NVL(BS.TIME_KEY, PL.TIME_KEY) TIME_KEY,  "+
                  "  (CAST(VER.VERSION_ID AS INT) + 1) VERSION_ID, "+
                  "  NVL(BS.AMOUNT_CLASS,PL.AMOUNT_CLASS) AMOUNT_CLASS,  "+
                  "  CAST(NVL(ASSET_COF, 0 ) AS DOUBLE) ASSET_COF,  "+
                  "  CAST(NVL(ASSET_COF_MTD_LCY, 0 ) AS DOUBLE) ASSET_COF_MTD_LCY,  "+
                  "  CAST(NVL(ASSET_COF_YTD_AED, 0 ) AS DOUBLE) ASSET_COF_YTD_AED,  "+
                  "  CAST(NVL(ASSET_COF_YTD_LCY, 0 ) AS DOUBLE) ASSET_COF_YTD_LCY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_LTD_ACY, 0 ) AS DOUBLE) AVG_BOOK_BAL_LTD_ACY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_LTD_AED, 0 ) AS DOUBLE) AVG_BOOK_BAL_LTD_AED,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_LTD_LCY, 0 ) AS DOUBLE) AVG_BOOK_BAL_LTD_LCY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_MTD_ACY, 0 ) AS DOUBLE) AVG_BOOK_BAL_MTD_ACY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_MTD_AED, 0 ) AS DOUBLE) AVG_BOOK_BAL_MTD_AED,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_MTD_LCY, 0 ) AS DOUBLE) AVG_BOOK_BAL_MTD_LCY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_YTD_ACY, 0 ) AS DOUBLE) AVG_BOOK_BAL_YTD_ACY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_YTD_AED, 0 ) AS DOUBLE) AVG_BOOK_BAL_YTD_AED,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_YTD_LCY, 0 ) AS DOUBLE) AVG_BOOK_BAL_YTD_LCY,  "+
                  "  NVL(BS.BALANCE_SHEET_TYPE,'BS-PL INSERT') BALANCE_SHEET_TYPE,  "+
                  "  NVL(BS.BANKING_TYPE,PL.BANKING_TYPE) BANKING_TYPE,  "+
                  "  NVL(BS.BANKING_TYPE_CUSTOMER,PL.BANKING_TYPE_CUSTOMER) BANKING_TYPE_CUSTOMER,  "+
                  "  'BS' BS_PL_FLAG,  "+
                  "  NVL(BS.CATEGORY_CODE,PL.CATEGORY_CODE) CATEGORY_CODE,  "+
                  "  NVL(BS.COMMON_KEY, 0 ) COMMON_KEY,  "+
                  "  NVL(BS.CONTRACT_ID,PL.CONTRACT_ID) CONTRACT_ID,  "+
                  "  NVL(BS.CURRENCY_CODE,PL.CURRENCY_CODE) CURRENCY_CODE,  "+
                  "  NVL(BS.CUSTOMER_NUMBER,PL.CUSTOMER_NUMBER) CUSTOMER_NUMBER,  "+
                  "  NVL(BS.CUSTOMER_SEGMENT_CODE,PL.CUSTOMER_SEGMENT_CODE) CUSTOMER_SEGMENT_CODE,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME, 0 ) AS DOUBLE) DERIVATIVES_INCOME,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME_MTD_LCY, 0 ) AS DOUBLE) DERIVATIVES_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME_YTD_AED, 0 ) AS DOUBLE) DERIVATIVES_INCOME_YTD_AED,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME_YTD_LCY, 0 ) AS DOUBLE) DERIVATIVES_INCOME_YTD_LCY,  "+
                  "  NVL(BS.DOMAIN_ID,PL.DOMAIN_ID) DOMAIN_ID,  "+
                  "  CAST(NVL(FEE_INCOME, 0 ) AS DOUBLE) FEE_INCOME,  "+
                  "  CAST(NVL(FEE_INCOME_MTD_LCY, 0 ) AS DOUBLE) FEE_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(FEE_INCOME_YTD_AED, 0 ) AS DOUBLE) FEE_INCOME_YTD_AED,  "+
                  "  CAST(NVL(FEE_INCOME_YTD_LCY, 0 ) AS DOUBLE) FEE_INCOME_YTD_LCY,  "+
                  "  NVL(BS.FINAL_SEGMENT,PL.FINAL_SEGMENT) FINAL_SEGMENT,  "+
                  "  CAST(NVL(FX_INCOME, 0 ) AS DOUBLE) FX_INCOME,  "+
                  "  CAST(NVL(FX_INCOME_MTD_LCY, 0 ) AS DOUBLE) FX_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(FX_INCOME_YTD_AED, 0 ) AS DOUBLE) FX_INCOME_YTD_AED,  "+
                  "  CAST(NVL(FX_INCOME_YTD_LCY, 0 ) AS DOUBLE) FX_INCOME_YTD_LCY,  "+
                  "  NVL(BS.GL_ACCOUNT_ID,'999999') GL_ACCOUNT_ID,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE_MTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE_MTD_LCY,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE_YTD_AED, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_AED,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE_YTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_LCY,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME_MTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME_YTD_AED, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME_YTD_AED,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME_YTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE_MTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE_MTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE_YTD_AED, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE_YTD_AED,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE_YTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE_YTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME, 0 ) AS DOUBLE) INTERBRANCH_INCOME,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME_MTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME_YTD_AED, 0 ) AS DOUBLE) INTERBRANCH_INCOME_YTD_AED,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME_YTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_INCOME_YTD_LCY,  "+
                  "  NVL(BS.IS_INTERNAL_ACCOUNT,'999999') IS_INTERNAL_ACCOUNT,  "+
                  "  NVL(BS.LEGAL_ENTITY,PL.LEGAL_ENTITY) LEGAL_ENTITY,  "+
                  "  CAST(NVL(LIABILITY_COF, 0 ) AS DOUBLE) LIABILITY_COF,  "+
                  "  CAST(NVL(LIABILITY_COF_MTD_LCY, 0 ) AS DOUBLE) LIABILITY_COF_MTD_LCY,  "+
                  "  CAST(NVL(LIABILITY_COF_YTD_AED, 0 ) AS DOUBLE) LIABILITY_COF_YTD_AED,  "+
                  "  CAST(NVL(LIABILITY_COF_YTD_LCY, 0 ) AS DOUBLE) LIABILITY_COF_YTD_LCY,  "+
                  "  CAST(NVL(LP_CHARGE, 0 ) AS DOUBLE) LP_CHARGE,  "+
                  "  CAST(NVL(LP_CHARGE_MTD_LCY, 0 ) AS DOUBLE) LP_CHARGE_MTD_LCY,  "+
                  "  CAST(NVL(LP_CHARGE_YTD_AED, 0 ) AS DOUBLE) LP_CHARGE_YTD_AED,  "+
                  "  CAST(NVL(LP_CHARGE_YTD_LCY, 0 ) AS DOUBLE) LP_CHARGE_YTD_LCY,  "+
                  "  CAST(NVL(LP_CREDIT, 0 ) AS DOUBLE) LP_CREDIT,  "+
                  "  CAST(NVL(LP_CREDIT_MTD_LCY, 0 ) AS DOUBLE) LP_CREDIT_MTD_LCY,  "+
                  "  CAST(NVL(LP_CREDIT_YTD_AED, 0 ) AS DOUBLE) LP_CREDIT_YTD_AED,  "+
                  "  CAST(NVL(LP_CREDIT_YTD_LCY, 0 ) AS DOUBLE) LP_CREDIT_YTD_LCY,  "+
                  "  CAST(NVL(MTD_ACY, 0 ) AS DOUBLE) MTD_ACY,  "+
                  "  CAST(NVL(MTD_AED, 0 ) AS DOUBLE) MTD_AED,  "+
                  "  CAST(NVL(MTD_LCY, 0 ) AS DOUBLE) MTD_LCY,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME, 0 ) AS DOUBLE) NET_INTEREST_INCOME,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME_MTD_LCY, 0 ) AS DOUBLE) NET_INTEREST_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME_YTD_AED, 0 ) AS DOUBLE) NET_INTEREST_INCOME_YTD_AED,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME_YTD_LCY, 0 ) AS DOUBLE) NET_INTEREST_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME_MTD_LCY, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME_YTD_AED, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME_YTD_AED,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME_YTD_LCY, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(NET_PL, 0 ) AS DOUBLE) NET_PL,  "+
                  "  CAST(NVL(NET_PL_MTD_LCY, 0 ) AS DOUBLE) NET_PL_MTD_LCY,  "+
                  "  CAST(NVL(NET_PL_YTD_AED, 0 ) AS DOUBLE) NET_PL_YTD_AED,  "+
                  "  CAST(NVL(NET_PL_YTD_LCY, 0 ) AS DOUBLE) NET_PL_YTD_LCY,  "+
                  "  CAST(NVL(OTH_INCOME, 0 ) AS DOUBLE) OTH_INCOME,  "+
                  "  CAST(NVL(OTH_INCOME_MTD_LCY, 0 ) AS DOUBLE) OTH_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(OTH_INCOME_YTD_AED, 0 ) AS DOUBLE) OTH_INCOME_YTD_AED,  "+
                  "  CAST(NVL(OTH_INCOME_YTD_LCY, 0 ) AS DOUBLE) OTH_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_MTD_ACY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_MTD_ACY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_MTD_AED, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_MTD_AED,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_MTD_LCY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_MTD_LCY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_YTD_ACY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_YTD_ACY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_YTD_AED, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_YTD_AED,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_YTD_LCY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_YTD_LCY,  "+
                  "  NVL(BS.PROFIT_CENTRE,PL.PROFIT_CENTRE) PROFIT_CENTRE,  "+
                  "  NVL(BS.SOURCE_SYSTEM_ID,PL.SOURCE_SYSTEM_ID) SOURCE_SYSTEM_ID,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_MTD_AED, 0 ) AS DOUBLE) TOTAL_FTP_AMT_MTD_AED,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_MTD_LCY, 0 ) AS DOUBLE) TOTAL_FTP_AMT_MTD_LCY,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_YTD_AED, 0 ) AS DOUBLE) TOTAL_FTP_AMT_YTD_AED,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_YTD_LCY, 0 ) AS DOUBLE) TOTAL_FTP_AMT_YTD_LCY,  "+
                  "  CAST(NVL((NET_INTEREST_INCOME/(CASE WHEN AVG_BOOK_BAL_MTD_AED = 0 THEN 1 ELSE AVG_BOOK_BAL_MTD_AED END))*(365/(DAY(LAST_DAY(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP))))),0) AS DOUBLE) NET_INT_MARGIN,  "+
                  "  CAST(NVL((NET_INTEREST_INCOME_MTD_LCY/(CASE WHEN AVG_BOOK_BAL_MTD_LCY = 0 THEN 1 ELSE AVG_BOOK_BAL_MTD_LCY END))*(365/(DAY(LAST_DAY(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP))))),0) AS DOUBLE) NET_INT_MARGIN_MTD_LCY, "+
                  "  CAST(NVL((NET_INTEREST_INCOME_YTD_AED/(CASE WHEN AVG_BOOK_BAL_YTD_AED = 0 THEN 1 ELSE AVG_BOOK_BAL_YTD_AED END))*(365/(DATEDIFF(TO_DATE(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP)), TO_DATE(TRUNC(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP), 'year')))+1)),0) AS DOUBLE) NET_INT_MARGIN_YTD_AED, "+
                  "  CAST(NVL((NET_INTEREST_INCOME_YTD_LCY/(CASE WHEN AVG_BOOK_BAL_YTD_LCY = 0 THEN 1 ELSE AVG_BOOK_BAL_YTD_LCY END))*(365/(DATEDIFF(TO_DATE(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP)), TO_DATE(TRUNC(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP), 'year')))+1)),0) AS DOUBLE)  NET_INT_MARGIN_YTD_LCY, "+
                  " 'Y' AS LAST_REFRESH_FLAG "+
                  "FROM  "+
                  "  (SELECT * FROM OSX_SEG_BS_DATA )BS  "+
                  "FULL OUTER JOIN  "+
                  "  (SELECT   "+
                  "    Q3.TIME_KEY,  "+
                  "    Q3.AMOUNT_CLASS,  "+
                  "    Q3.BANKING_TYPE,  "+
                  "    Q3.BANKING_TYPE_CUSTOMER,  "+
                  "    Q3.CATEGORY_CODE,  "+
                  "    Q3.CONTRACT_ID,  "+
                  "    Q3.CURRENCY_CODE,  "+
                  "    Q3.CUSTOMER_NUMBER,  "+
                  "    Q3.CUSTOMER_SEGMENT_CODE,  "+
                  "    Q3.DOMAIN_ID,  "+
                  "    Q3.FINAL_SEGMENT,  "+
                  "    Q3.LEGAL_ENTITY,  "+
                  "    Q3.PROFIT_CENTRE,  "+
                  "    Q3.SOURCE_SYSTEM_ID,  "+
                  "    SUM(Q3.ASSET_COF) ASSET_COF,  "+
                  "    SUM(Q3.ASSET_COF_MTD_LCY) ASSET_COF_MTD_LCY,  "+
                  "    SUM(Q3.ASSET_COF_YTD_AED) ASSET_COF_YTD_AED,  "+
                  "    SUM(Q3.ASSET_COF_YTD_LCY) ASSET_COF_YTD_LCY,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME) DERIVATIVES_INCOME,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME_MTD_LCY) DERIVATIVES_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME_YTD_AED) DERIVATIVES_INCOME_YTD_AED,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME_YTD_LCY) DERIVATIVES_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.FEE_INCOME) FEE_INCOME,  "+
                  "    SUM(Q3.FEE_INCOME_MTD_LCY) FEE_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.FEE_INCOME_YTD_AED) FEE_INCOME_YTD_AED,  "+
                  "    SUM(Q3.FEE_INCOME_YTD_LCY) FEE_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.FX_INCOME) FX_INCOME,  "+
                  "    SUM(Q3.FX_INCOME_MTD_LCY) FX_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.FX_INCOME_YTD_AED) FX_INCOME_YTD_AED,  "+
                  "    SUM(Q3.FX_INCOME_YTD_LCY) FX_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE) GROSS_INTEREST_EXPENSE,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE_MTD_LCY) GROSS_INTEREST_EXPENSE_MTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE_YTD_AED) GROSS_INTEREST_EXPENSE_YTD_AED,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE_YTD_LCY) GROSS_INTEREST_EXPENSE_YTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME) GROSS_INTEREST_INCOME,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME_MTD_LCY) GROSS_INTEREST_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME_YTD_AED) GROSS_INTEREST_INCOME_YTD_AED,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME_YTD_LCY) GROSS_INTEREST_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE) INTERBRANCH_EXPENSE,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE_MTD_LCY) INTERBRANCH_EXPENSE_MTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE_YTD_AED) INTERBRANCH_EXPENSE_YTD_AED,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE_YTD_LCY) INTERBRANCH_EXPENSE_YTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME) INTERBRANCH_INCOME,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME_MTD_LCY) INTERBRANCH_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME_YTD_AED) INTERBRANCH_INCOME_YTD_AED,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME_YTD_LCY) INTERBRANCH_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.LIABILITY_COF) LIABILITY_COF,  "+
                  "    SUM(Q3.LIABILITY_COF_MTD_LCY) LIABILITY_COF_MTD_LCY,  "+
                  "    SUM(Q3.LIABILITY_COF_YTD_AED) LIABILITY_COF_YTD_AED,  "+
                  "    SUM(Q3.LIABILITY_COF_YTD_LCY) LIABILITY_COF_YTD_LCY,  "+
                  "    SUM(Q3.LP_CHARGE) LP_CHARGE,  "+
                  "    SUM(Q3.LP_CHARGE_MTD_LCY) LP_CHARGE_MTD_LCY,  "+
                  "    SUM(Q3.LP_CHARGE_YTD_AED) LP_CHARGE_YTD_AED,  "+
                  "    SUM(Q3.LP_CHARGE_YTD_LCY) LP_CHARGE_YTD_LCY,  "+
                  "    SUM(Q3.LP_CREDIT) LP_CREDIT,  "+
                  "    SUM(Q3.LP_CREDIT_MTD_LCY) LP_CREDIT_MTD_LCY,  "+
                  "    SUM(Q3.LP_CREDIT_YTD_AED) LP_CREDIT_YTD_AED,  "+
                  "    SUM(Q3.LP_CREDIT_YTD_LCY) LP_CREDIT_YTD_LCY,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME) NET_INTEREST_INCOME,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME_MTD_LCY) NET_INTEREST_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME_YTD_AED) NET_INTEREST_INCOME_YTD_AED,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME_YTD_LCY) NET_INTEREST_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME) NET_INVESTMENT_INCOME,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME_MTD_LCY) NET_INVESTMENT_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME_YTD_AED) NET_INVESTMENT_INCOME_YTD_AED,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME_YTD_LCY) NET_INVESTMENT_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.NET_PL) NET_PL,  "+
                  "    SUM(Q3.NET_PL_MTD_LCY) NET_PL_MTD_LCY,  "+
                  "    SUM(Q3.NET_PL_YTD_AED) NET_PL_YTD_AED,  "+
                  "    SUM(Q3.NET_PL_YTD_LCY) NET_PL_YTD_LCY,  "+
                  "    SUM(Q3.OTH_INCOME) OTH_INCOME,  "+
                  "    SUM(Q3.OTH_INCOME_MTD_LCY) OTH_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.OTH_INCOME_YTD_AED) OTH_INCOME_YTD_AED,  "+
                  "    SUM(Q3.OTH_INCOME_YTD_LCY) OTH_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_MTD_AED) TOTAL_FTP_AMT_MTD_AED,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_MTD_LCY) TOTAL_FTP_AMT_MTD_LCY,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_YTD_AED) TOTAL_FTP_AMT_YTD_AED,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_YTD_LCY) TOTAL_FTP_AMT_YTD_LCY,  "+
                  "    1 RANK_PL  "+
                  "  FROM  "+
                  "    (SELECT Q2.*,  "+
                  "      (Q2.NET_INTEREST_INCOME        +Q2.DERIVATIVES_INCOME+Q2.FEE_INCOME+Q2.FX_INCOME+Q2.NET_INVESTMENT_INCOME+Q2.OTH_INCOME+Q2.INTERBRANCH_INCOME+Q2.INTERBRANCH_EXPENSE) NET_PL,  "+
                  "      (Q2.NET_INTEREST_INCOME_MTD_LCY+Q2.DERIVATIVES_INCOME_MTD_LCY+Q2.FEE_INCOME_MTD_LCY+Q2.FX_INCOME_MTD_LCY+Q2.NET_INVESTMENT_INCOME_MTD_LCY+Q2.OTH_INCOME_MTD_LCY+Q2.INTERBRANCH_INCOME_MTD_LCY+Q2.INTERBRANCH_EXPENSE_MTD_LCY) NET_PL_MTD_LCY,  "+
                  "      (Q2.NET_INTEREST_INCOME_YTD_AED+Q2.DERIVATIVES_INCOME_YTD_AED+Q2.FEE_INCOME_YTD_AED+Q2.FX_INCOME_YTD_AED+Q2.NET_INVESTMENT_INCOME_YTD_AED+Q2.OTH_INCOME_YTD_AED+Q2.INTERBRANCH_INCOME_YTD_AED+Q2.INTERBRANCH_EXPENSE_YTD_AED) NET_PL_YTD_AED,  "+
                  "      (Q2.NET_INTEREST_INCOME_YTD_LCY+Q2.DERIVATIVES_INCOME_YTD_LCY+Q2.FEE_INCOME_YTD_LCY+Q2.FX_INCOME_YTD_LCY+Q2.NET_INVESTMENT_INCOME_YTD_LCY+Q2.OTH_INCOME_YTD_LCY+Q2.INTERBRANCH_INCOME_YTD_LCY+Q2.INTERBRANCH_EXPENSE_YTD_LCY) NET_PL_YTD_LCY  "+
                  "    FROM  "+
                  "      (SELECT Q1.*,  "+
                  "        (Q1.GROSS_INTEREST_INCOME         + Q1.ASSET_COF + Q1.LP_CHARGE + Q1.GROSS_INTEREST_EXPENSE + Q1.LIABILITY_COF + Q1.LP_CREDIT) NET_INTEREST_INCOME,  "+
                  "        (Q1.GROSS_INTEREST_INCOME_MTD_LCY + Q1.ASSET_COF_MTD_LCY + Q1.LP_CHARGE_MTD_LCY + Q1.GROSS_INTEREST_EXPENSE_MTD_LCY + Q1.LIABILITY_COF_MTD_LCY + Q1.LP_CREDIT_MTD_LCY) NET_INTEREST_INCOME_MTD_LCY,  "+
                  "        (Q1.GROSS_INTEREST_INCOME_YTD_AED + Q1.ASSET_COF_YTD_AED + Q1.LP_CHARGE_YTD_AED + Q1.GROSS_INTEREST_EXPENSE_YTD_AED + Q1.LIABILITY_COF_YTD_AED + Q1.LP_CREDIT_YTD_AED) NET_INTEREST_INCOME_YTD_AED,  "+
                  "        (Q1.GROSS_INTEREST_INCOME_YTD_LCY + Q1.ASSET_COF_YTD_LCY + Q1.LP_CHARGE_YTD_LCY + Q1.GROSS_INTEREST_EXPENSE_YTD_LCY + Q1.LIABILITY_COF_YTD_LCY + Q1.LP_CREDIT_YTD_LCY) NET_INTEREST_INCOME_YTD_LCY,  "+
                  "        (Q1.ASSET_COF                     + Q1.LP_CHARGE + Q1.LIABILITY_COF + Q1.LP_CREDIT) TOTAL_FTP_AMT_MTD_AED,  "+
                  "        (Q1.ASSET_COF_MTD_LCY             + Q1.LP_CHARGE_MTD_LCY + Q1.LIABILITY_COF_MTD_LCY + Q1.LP_CREDIT_MTD_LCY) TOTAL_FTP_AMT_MTD_LCY,  "+
                  "        (Q1.ASSET_COF_YTD_AED             + Q1.LP_CHARGE_YTD_AED + Q1.LIABILITY_COF_YTD_AED + Q1.LP_CREDIT_YTD_AED) TOTAL_FTP_AMT_YTD_AED,  "+
                  "        (Q1.ASSET_COF_YTD_LCY             + Q1.LP_CHARGE_YTD_LCY + Q1.LIABILITY_COF_YTD_LCY + Q1.LP_CREDIT_YTD_LCY) TOTAL_FTP_AMT_YTD_LCY  "+
                  "      FROM  "+
                  "        (SELECT * FROM OSX_SEG_PL_DATA )Q1  "+
                  "      )Q2  "+
                  "    )Q3  "+
                  "  group by Q3.AMOUNT_CLASS, Q3.BANKING_TYPE, Q3.BANKING_TYPE_CUSTOMER, Q3.CATEGORY_CODE, Q3.CONTRACT_ID, Q3.CURRENCY_CODE, Q3.CUSTOMER_NUMBER, Q3.CUSTOMER_SEGMENT_CODE, Q3.DOMAIN_ID, Q3.FINAL_SEGMENT, Q3.LEGAL_ENTITY, Q3.PROFIT_CENTRE, Q3.SOURCE_SYSTEM_ID, 1  "+
                  ")PL ON BS.AMOUNT_CLASS     = PL.AMOUNT_CLASS  "+
                  "AND BS.BANKING_TYPE          = PL.BANKING_TYPE  "+
                  "AND BS.BANKING_TYPE_CUSTOMER = PL.BANKING_TYPE_CUSTOMER  "+
                  "AND BS.CATEGORY_CODE         = PL.CATEGORY_CODE  "+
                  "AND BS.CONTRACT_ID           = PL.CONTRACT_ID  "+
                  "AND BS.CURRENCY_CODE         = PL.CURRENCY_CODE  "+
                  "AND BS.CUSTOMER_NUMBER       = PL.CUSTOMER_NUMBER  "+
                  "AND BS.CUSTOMER_SEGMENT_CODE = PL.CUSTOMER_SEGMENT_CODE  "+
                  "AND BS.DOMAIN_ID             = PL.DOMAIN_ID  "+
                  "AND BS.FINAL_SEGMENT         = PL.FINAL_SEGMENT  "+
                  "AND BS.LEGAL_ENTITY          = PL.LEGAL_ENTITY  "+
                  "AND BS.PROFIT_CENTRE         = PL.PROFIT_CENTRE  "+
                  "AND BS.SOURCE_SYSTEM_ID      = PL.SOURCE_SYSTEM_ID  "+
                  "AND RANK_BS                  = RANK_PL "+
                  "CROSS JOIN LKP_LAST_VERSION VER "+
                  "UNION ALL "+
                  "SELECT  "+
                  "    TIME_KEY, "+
                  "    (CAST(VER.VERSION_ID AS INT) + 1) VERSION_ID, "+
                  "    AMOUNT_CLASS, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    CAST(AVG_BOOK_BAL_LTD_ACY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_LTD_AED AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_LTD_LCY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_MTD_ACY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_MTD_AED AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_MTD_LCY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_YTD_ACY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_YTD_AED AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_YTD_LCY AS DOUBLE), "+
                  "    BALANCE_SHEET_TYPE, "+
                  "    BANKING_TYPE, "+
                  "    BANKING_TYPE_CUSTOMER, "+
                  "    BS_PL_FLAG, "+
                  "    CATEGORY_CODE, "+
                  "    COMMON_KEY, "+
                  "    CONTRACT_ID, "+
                  "    CURRENCY_CODE, "+
                  "    CUSTOMER_NUMBER, "+
                  "    CUSTOMER_SEGMENT_CODE, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    DOMAIN_ID, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    FINAL_SEGMENT, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    GL_ACCOUNT_ID, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    IS_INTERNAL_ACCOUNT, "+
                  "    LEGAL_ENTITY, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    MTD_ACY, "+
                  "    MTD_AED, "+
                  "    MTD_LCY, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    CAST(OUTSTANDING_BALANCE_MTD_ACY AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_MTD_AED AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_MTD_LCY AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_YTD_ACY AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_YTD_AED AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_YTD_LCY AS DOUBLE), "+
                  "    PROFIT_CENTRE, "+
                  "    SOURCE_SYSTEM_ID, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  " 'Y' AS LAST_REFRESH_FLAG "+
                  "FROM OSX_SEG_BASE_AGG AGG "+
                  "CROSS JOIN LKP_LAST_VERSION VER "+
                  "WHERE BS_PL_FLAG = 'PL' "
          )
          
          println (process_sumx_agg.time+" "+  "OSX BASE AGG - WRITING LATEST VERSION TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_seg_base_agg)
          
          osx_seg_base.repartition(200)
                        .write
                        .partitionBy("time_key", "last_refresh_flag", "version_id", "amount_class", "domain_id")
                        .mode("append")
                        .format("parquet")
                        .option("compression","snappy")
                        .save(hdfs_seg_base_agg)
          
          println (process_sumx_agg.time+" "+ "OSX BASE AGG - AGGREGATE LOAD COMPLETED")
          
          osx_seg_base.persist(StorageLevel.MEMORY_ONLY_SER)
          
          //CALLING THE TRIAL BALANCE PROCESSING FUNCTION
          println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - LOAD CALLED FROM BASE LOAD FUNCTION")
          load_trial_balance.osx_seg_trial_balance_load(sc, sqlContext, time_key, dom, acc, le, osx_cust, osx_cont, osx_seg_base, overwrite_flag,0)
          
         
       }
       
       // IF BASE REFRESH IS CALLED FOR BUDGET REFRESH ONLY      
       else 
       {   
          
           if(("hadoop fs -test -d ".concat(hdfs_seg_base_agg_r).!)==0)  
           {
              println (process_sumx_agg.time+" "+  "OSX BASE AGG - DELETING EXISTING DIRECTORY ")
              val remove_dir = "hadoop fs -rm -r ".concat(hdfs_seg_base_agg_r).!
              if(remove_dir==0) {
               
                println (process_sumx_agg.time+" "+  "OSX BASE AGG - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE BASE AGG RELOAD ")
                //pw.println ("OSX BASE - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE BASE AGG RELOAD. LOAD IN PROGRESS")
              }
              println (process_sumx_agg.time+" "+  "OSX BASE AGG - PROCESSING VERSION OVERWRITE OF BASE DATASET. APPLICABLE ONLY FOR DAILY LOAD SCENARIO" )
           }
           else {
             
              println (process_sumx_agg.time+" "+  "OSX BASE AGG - PROCESSING FIRST VERSION OF BASE DATASET. APPLICABLE FOR BOTH DAILY AND MONTHLY LOAD SCENARIO" )
           }
         
           // THIS IS THE CASE WHEN THERE ARE NO PREVIOUS LOADS FOR THE PARTICULAR TIME_KEY
           // BY DEFAULT THIS IS LOADED WITH VERSION ID = 1 AS THE FIRST VERSION ENTRY.
            
           
           val osx_seg_base= sqlContext.sql (
                  "SELECT  "+
                  "  NVL(BS.TIME_KEY, PL.TIME_KEY) TIME_KEY,  "+
                  "  CAST(1 AS INT) VERSION_ID, "+
                  "  NVL(BS.AMOUNT_CLASS,PL.AMOUNT_CLASS) AMOUNT_CLASS,  "+
                  "  CAST(NVL(ASSET_COF, 0 ) AS DOUBLE) ASSET_COF,  "+
                  "  CAST(NVL(ASSET_COF_MTD_LCY, 0 ) AS DOUBLE) ASSET_COF_MTD_LCY,  "+
                  "  CAST(NVL(ASSET_COF_YTD_AED, 0 ) AS DOUBLE) ASSET_COF_YTD_AED,  "+
                  "  CAST(NVL(ASSET_COF_YTD_LCY, 0 ) AS DOUBLE) ASSET_COF_YTD_LCY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_LTD_ACY, 0 ) AS DOUBLE) AVG_BOOK_BAL_LTD_ACY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_LTD_AED, 0 ) AS DOUBLE) AVG_BOOK_BAL_LTD_AED,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_LTD_LCY, 0 ) AS DOUBLE) AVG_BOOK_BAL_LTD_LCY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_MTD_ACY, 0 ) AS DOUBLE) AVG_BOOK_BAL_MTD_ACY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_MTD_AED, 0 ) AS DOUBLE) AVG_BOOK_BAL_MTD_AED,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_MTD_LCY, 0 ) AS DOUBLE) AVG_BOOK_BAL_MTD_LCY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_YTD_ACY, 0 ) AS DOUBLE) AVG_BOOK_BAL_YTD_ACY,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_YTD_AED, 0 ) AS DOUBLE) AVG_BOOK_BAL_YTD_AED,  "+
                  "  CAST(NVL(AVG_BOOK_BAL_YTD_LCY, 0 ) AS DOUBLE) AVG_BOOK_BAL_YTD_LCY,  "+
                  "  NVL(BS.BALANCE_SHEET_TYPE,'BS-PL INSERT') BALANCE_SHEET_TYPE,  "+
                  "  NVL(BS.BANKING_TYPE,PL.BANKING_TYPE) BANKING_TYPE,  "+
                  "  NVL(BS.BANKING_TYPE_CUSTOMER,PL.BANKING_TYPE_CUSTOMER) BANKING_TYPE_CUSTOMER,  "+
                  "  'BS' BS_PL_FLAG,  "+
                  "  NVL(BS.CATEGORY_CODE,PL.CATEGORY_CODE) CATEGORY_CODE,  "+
                  "  NVL(BS.COMMON_KEY, 0 ) COMMON_KEY,  "+
                  "  NVL(BS.CONTRACT_ID,PL.CONTRACT_ID) CONTRACT_ID,  "+
                  "  NVL(BS.CURRENCY_CODE,PL.CURRENCY_CODE) CURRENCY_CODE,  "+
                  "  NVL(BS.CUSTOMER_NUMBER,PL.CUSTOMER_NUMBER) CUSTOMER_NUMBER,  "+
                  "  NVL(BS.CUSTOMER_SEGMENT_CODE,PL.CUSTOMER_SEGMENT_CODE) CUSTOMER_SEGMENT_CODE,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME, 0 ) AS DOUBLE) DERIVATIVES_INCOME,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME_MTD_LCY, 0 ) AS DOUBLE) DERIVATIVES_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME_YTD_AED, 0 ) AS DOUBLE) DERIVATIVES_INCOME_YTD_AED,  "+
                  "  CAST(NVL(DERIVATIVES_INCOME_YTD_LCY, 0 ) AS DOUBLE) DERIVATIVES_INCOME_YTD_LCY,  "+
                  "  NVL(BS.DOMAIN_ID,PL.DOMAIN_ID) DOMAIN_ID,  "+
                  "  CAST(NVL(FEE_INCOME, 0 ) AS DOUBLE) FEE_INCOME,  "+
                  "  CAST(NVL(FEE_INCOME_MTD_LCY, 0 ) AS DOUBLE) FEE_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(FEE_INCOME_YTD_AED, 0 ) AS DOUBLE) FEE_INCOME_YTD_AED,  "+
                  "  CAST(NVL(FEE_INCOME_YTD_LCY, 0 ) AS DOUBLE) FEE_INCOME_YTD_LCY,  "+
                  "  NVL(BS.FINAL_SEGMENT,PL.FINAL_SEGMENT) FINAL_SEGMENT,  "+
                  "  CAST(NVL(FX_INCOME, 0 ) AS DOUBLE) FX_INCOME,  "+
                  "  CAST(NVL(FX_INCOME_MTD_LCY, 0 ) AS DOUBLE) FX_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(FX_INCOME_YTD_AED, 0 ) AS DOUBLE) FX_INCOME_YTD_AED,  "+
                  "  CAST(NVL(FX_INCOME_YTD_LCY, 0 ) AS DOUBLE) FX_INCOME_YTD_LCY,  "+
                  "  NVL(BS.GL_ACCOUNT_ID,'999999') GL_ACCOUNT_ID,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE_MTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE_MTD_LCY,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE_YTD_AED, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_AED,  "+
                  "  CAST(NVL(GROSS_INTEREST_EXPENSE_YTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_LCY,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME_MTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME_YTD_AED, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME_YTD_AED,  "+
                  "  CAST(NVL(GROSS_INTEREST_INCOME_YTD_LCY, 0 ) AS DOUBLE) GROSS_INTEREST_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE_MTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE_MTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE_YTD_AED, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE_YTD_AED,  "+
                  "  CAST(NVL(INTERBRANCH_EXPENSE_YTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_EXPENSE_YTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME, 0 ) AS DOUBLE) INTERBRANCH_INCOME,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME_MTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME_YTD_AED, 0 ) AS DOUBLE) INTERBRANCH_INCOME_YTD_AED,  "+
                  "  CAST(NVL(INTERBRANCH_INCOME_YTD_LCY, 0 ) AS DOUBLE) INTERBRANCH_INCOME_YTD_LCY,  "+
                  "  NVL(BS.IS_INTERNAL_ACCOUNT,'999999') IS_INTERNAL_ACCOUNT,  "+
                  "  NVL(BS.LEGAL_ENTITY,PL.LEGAL_ENTITY) LEGAL_ENTITY,  "+
                  "  CAST(NVL(LIABILITY_COF, 0 ) AS DOUBLE) LIABILITY_COF,  "+
                  "  CAST(NVL(LIABILITY_COF_MTD_LCY, 0 ) AS DOUBLE) LIABILITY_COF_MTD_LCY,  "+
                  "  CAST(NVL(LIABILITY_COF_YTD_AED, 0 ) AS DOUBLE) LIABILITY_COF_YTD_AED,  "+
                  "  CAST(NVL(LIABILITY_COF_YTD_LCY, 0 ) AS DOUBLE) LIABILITY_COF_YTD_LCY,  "+
                  "  CAST(NVL(LP_CHARGE, 0 ) AS DOUBLE) LP_CHARGE,  "+
                  "  CAST(NVL(LP_CHARGE_MTD_LCY, 0 ) AS DOUBLE) LP_CHARGE_MTD_LCY,  "+
                  "  CAST(NVL(LP_CHARGE_YTD_AED, 0 ) AS DOUBLE) LP_CHARGE_YTD_AED,  "+
                  "  CAST(NVL(LP_CHARGE_YTD_LCY, 0 ) AS DOUBLE) LP_CHARGE_YTD_LCY,  "+
                  "  CAST(NVL(LP_CREDIT, 0 ) AS DOUBLE) LP_CREDIT,  "+
                  "  CAST(NVL(LP_CREDIT_MTD_LCY, 0 ) AS DOUBLE) LP_CREDIT_MTD_LCY,  "+
                  "  CAST(NVL(LP_CREDIT_YTD_AED, 0 ) AS DOUBLE) LP_CREDIT_YTD_AED,  "+
                  "  CAST(NVL(LP_CREDIT_YTD_LCY, 0 ) AS DOUBLE) LP_CREDIT_YTD_LCY,  "+
                  "  CAST(NVL(MTD_ACY, 0 ) AS DOUBLE) MTD_ACY,  "+
                  "  CAST(NVL(MTD_AED, 0 ) AS DOUBLE) MTD_AED,  "+
                  "  CAST(NVL(MTD_LCY, 0 ) AS DOUBLE) MTD_LCY,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME, 0 ) AS DOUBLE) NET_INTEREST_INCOME,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME_MTD_LCY, 0 ) AS DOUBLE) NET_INTEREST_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME_YTD_AED, 0 ) AS DOUBLE) NET_INTEREST_INCOME_YTD_AED,  "+
                  "  CAST(NVL(NET_INTEREST_INCOME_YTD_LCY, 0 ) AS DOUBLE) NET_INTEREST_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME_MTD_LCY, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME_YTD_AED, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME_YTD_AED,  "+
                  "  CAST(NVL(NET_INVESTMENT_INCOME_YTD_LCY, 0 ) AS DOUBLE) NET_INVESTMENT_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(NET_PL, 0 ) AS DOUBLE) NET_PL,  "+
                  "  CAST(NVL(NET_PL_MTD_LCY, 0 ) AS DOUBLE) NET_PL_MTD_LCY,  "+
                  "  CAST(NVL(NET_PL_YTD_AED, 0 ) AS DOUBLE) NET_PL_YTD_AED,  "+
                  "  CAST(NVL(NET_PL_YTD_LCY, 0 ) AS DOUBLE) NET_PL_YTD_LCY,  "+
                  "  CAST(NVL(OTH_INCOME, 0 ) AS DOUBLE) OTH_INCOME,  "+
                  "  CAST(NVL(OTH_INCOME_MTD_LCY, 0 ) AS DOUBLE) OTH_INCOME_MTD_LCY,  "+
                  "  CAST(NVL(OTH_INCOME_YTD_AED, 0 ) AS DOUBLE) OTH_INCOME_YTD_AED,  "+
                  "  CAST(NVL(OTH_INCOME_YTD_LCY, 0 ) AS DOUBLE) OTH_INCOME_YTD_LCY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_MTD_ACY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_MTD_ACY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_MTD_AED, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_MTD_AED,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_MTD_LCY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_MTD_LCY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_YTD_ACY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_YTD_ACY,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_YTD_AED, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_YTD_AED,  "+
                  "  CAST(NVL(OUTSTANDING_BALANCE_YTD_LCY, 0 ) AS DOUBLE) OUTSTANDING_BALANCE_YTD_LCY,  "+
                  "  NVL(BS.PROFIT_CENTRE,PL.PROFIT_CENTRE) PROFIT_CENTRE,  "+
                  "  NVL(BS.SOURCE_SYSTEM_ID,PL.SOURCE_SYSTEM_ID) SOURCE_SYSTEM_ID,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_MTD_AED, 0 ) AS DOUBLE) TOTAL_FTP_AMT_MTD_AED,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_MTD_LCY, 0 ) AS DOUBLE) TOTAL_FTP_AMT_MTD_LCY,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_YTD_AED, 0 ) AS DOUBLE) TOTAL_FTP_AMT_YTD_AED,  "+
                  "  CAST(NVL(TOTAL_FTP_AMT_YTD_LCY, 0 ) AS DOUBLE) TOTAL_FTP_AMT_YTD_LCY,  "+
                  "  CAST(NVL((NET_INTEREST_INCOME/(CASE WHEN AVG_BOOK_BAL_MTD_AED = 0 THEN 1 ELSE AVG_BOOK_BAL_MTD_AED END))*(365/(DAY(LAST_DAY(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP))))),0) AS DOUBLE) NET_INT_MARGIN,  "+
                  "  CAST(NVL((NET_INTEREST_INCOME_MTD_LCY/(CASE WHEN AVG_BOOK_BAL_MTD_LCY = 0 THEN 1 ELSE AVG_BOOK_BAL_MTD_LCY END))*(365/(DAY(LAST_DAY(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP))))),0) AS DOUBLE) NET_INT_MARGIN_MTD_LCY, "+
                  "  CAST(NVL((NET_INTEREST_INCOME_YTD_AED/(CASE WHEN AVG_BOOK_BAL_YTD_AED = 0 THEN 1 ELSE AVG_BOOK_BAL_YTD_AED END))*(365/(DATEDIFF(TO_DATE(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP)), TO_DATE(TRUNC(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP), 'year')))+1)),0) AS DOUBLE) NET_INT_MARGIN_YTD_AED, "+
                  "  CAST(NVL((NET_INTEREST_INCOME_YTD_LCY/(CASE WHEN AVG_BOOK_BAL_YTD_LCY = 0 THEN 1 ELSE AVG_BOOK_BAL_YTD_LCY END))*(365/(DATEDIFF(TO_DATE(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP)), TO_DATE(TRUNC(CAST(UNIX_TIMESTAMP(NVL(BS.TIME_KEY, PL.TIME_KEY),'yyyyMMdd') AS TIMESTAMP), 'year')))+1)),0) AS DOUBLE)  NET_INT_MARGIN_YTD_LCY, "+
                  "  'Y' LAST_REFRESH_FLAG "+
                  "FROM  "+
                  "  (SELECT * FROM OSX_SEG_BS_DATA )BS  "+
                  "FULL OUTER JOIN  "+
                  "  (SELECT   "+
                  "    Q3.TIME_KEY,  "+
                  "    Q3.AMOUNT_CLASS,  "+
                  "    Q3.BANKING_TYPE,  "+
                  "    Q3.BANKING_TYPE_CUSTOMER,  "+
                  "    Q3.CATEGORY_CODE,  "+
                  "    Q3.CONTRACT_ID,  "+
                  "    Q3.CURRENCY_CODE,  "+
                  "    Q3.CUSTOMER_NUMBER,  "+
                  "    Q3.CUSTOMER_SEGMENT_CODE,  "+
                  "    Q3.DOMAIN_ID,  "+
                  "    Q3.FINAL_SEGMENT,  "+
                  "    Q3.LEGAL_ENTITY,  "+
                  "    Q3.PROFIT_CENTRE,  "+
                  "    Q3.SOURCE_SYSTEM_ID,  "+
                  "    SUM(Q3.ASSET_COF) ASSET_COF,  "+
                  "    SUM(Q3.ASSET_COF_MTD_LCY) ASSET_COF_MTD_LCY,  "+
                  "    SUM(Q3.ASSET_COF_YTD_AED) ASSET_COF_YTD_AED,  "+
                  "    SUM(Q3.ASSET_COF_YTD_LCY) ASSET_COF_YTD_LCY,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME) DERIVATIVES_INCOME,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME_MTD_LCY) DERIVATIVES_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME_YTD_AED) DERIVATIVES_INCOME_YTD_AED,  "+
                  "    SUM(Q3.DERIVATIVES_INCOME_YTD_LCY) DERIVATIVES_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.FEE_INCOME) FEE_INCOME,  "+
                  "    SUM(Q3.FEE_INCOME_MTD_LCY) FEE_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.FEE_INCOME_YTD_AED) FEE_INCOME_YTD_AED,  "+
                  "    SUM(Q3.FEE_INCOME_YTD_LCY) FEE_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.FX_INCOME) FX_INCOME,  "+
                  "    SUM(Q3.FX_INCOME_MTD_LCY) FX_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.FX_INCOME_YTD_AED) FX_INCOME_YTD_AED,  "+
                  "    SUM(Q3.FX_INCOME_YTD_LCY) FX_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE) GROSS_INTEREST_EXPENSE,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE_MTD_LCY) GROSS_INTEREST_EXPENSE_MTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE_YTD_AED) GROSS_INTEREST_EXPENSE_YTD_AED,  "+
                  "    SUM(Q3.GROSS_INTEREST_EXPENSE_YTD_LCY) GROSS_INTEREST_EXPENSE_YTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME) GROSS_INTEREST_INCOME,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME_MTD_LCY) GROSS_INTEREST_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME_YTD_AED) GROSS_INTEREST_INCOME_YTD_AED,  "+
                  "    SUM(Q3.GROSS_INTEREST_INCOME_YTD_LCY) GROSS_INTEREST_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE) INTERBRANCH_EXPENSE,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE_MTD_LCY) INTERBRANCH_EXPENSE_MTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE_YTD_AED) INTERBRANCH_EXPENSE_YTD_AED,  "+
                  "    SUM(Q3.INTERBRANCH_EXPENSE_YTD_LCY) INTERBRANCH_EXPENSE_YTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME) INTERBRANCH_INCOME,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME_MTD_LCY) INTERBRANCH_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME_YTD_AED) INTERBRANCH_INCOME_YTD_AED,  "+
                  "    SUM(Q3.INTERBRANCH_INCOME_YTD_LCY) INTERBRANCH_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.LIABILITY_COF) LIABILITY_COF,  "+
                  "    SUM(Q3.LIABILITY_COF_MTD_LCY) LIABILITY_COF_MTD_LCY,  "+
                  "    SUM(Q3.LIABILITY_COF_YTD_AED) LIABILITY_COF_YTD_AED,  "+
                  "    SUM(Q3.LIABILITY_COF_YTD_LCY) LIABILITY_COF_YTD_LCY,  "+
                  "    SUM(Q3.LP_CHARGE) LP_CHARGE,  "+
                  "    SUM(Q3.LP_CHARGE_MTD_LCY) LP_CHARGE_MTD_LCY,  "+
                  "    SUM(Q3.LP_CHARGE_YTD_AED) LP_CHARGE_YTD_AED,  "+
                  "    SUM(Q3.LP_CHARGE_YTD_LCY) LP_CHARGE_YTD_LCY,  "+
                  "    SUM(Q3.LP_CREDIT) LP_CREDIT,  "+
                  "    SUM(Q3.LP_CREDIT_MTD_LCY) LP_CREDIT_MTD_LCY,  "+
                  "    SUM(Q3.LP_CREDIT_YTD_AED) LP_CREDIT_YTD_AED,  "+
                  "    SUM(Q3.LP_CREDIT_YTD_LCY) LP_CREDIT_YTD_LCY,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME) NET_INTEREST_INCOME,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME_MTD_LCY) NET_INTEREST_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME_YTD_AED) NET_INTEREST_INCOME_YTD_AED,  "+
                  "    SUM(Q3.NET_INTEREST_INCOME_YTD_LCY) NET_INTEREST_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME) NET_INVESTMENT_INCOME,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME_MTD_LCY) NET_INVESTMENT_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME_YTD_AED) NET_INVESTMENT_INCOME_YTD_AED,  "+
                  "    SUM(Q3.NET_INVESTMENT_INCOME_YTD_LCY) NET_INVESTMENT_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.NET_PL) NET_PL,  "+
                  "    SUM(Q3.NET_PL_MTD_LCY) NET_PL_MTD_LCY,  "+
                  "    SUM(Q3.NET_PL_YTD_AED) NET_PL_YTD_AED,  "+
                  "    SUM(Q3.NET_PL_YTD_LCY) NET_PL_YTD_LCY,  "+
                  "    SUM(Q3.OTH_INCOME) OTH_INCOME,  "+
                  "    SUM(Q3.OTH_INCOME_MTD_LCY) OTH_INCOME_MTD_LCY,  "+
                  "    SUM(Q3.OTH_INCOME_YTD_AED) OTH_INCOME_YTD_AED,  "+
                  "    SUM(Q3.OTH_INCOME_YTD_LCY) OTH_INCOME_YTD_LCY,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_MTD_AED) TOTAL_FTP_AMT_MTD_AED,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_MTD_LCY) TOTAL_FTP_AMT_MTD_LCY,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_YTD_AED) TOTAL_FTP_AMT_YTD_AED,  "+
                  "    SUM(Q3.TOTAL_FTP_AMT_YTD_LCY) TOTAL_FTP_AMT_YTD_LCY,  "+
                  "    1 RANK_PL  "+
                  "  FROM  "+
                  "    (SELECT Q2.*,  "+
                  "      (Q2.NET_INTEREST_INCOME        +Q2.DERIVATIVES_INCOME+Q2.FEE_INCOME+Q2.FX_INCOME+Q2.NET_INVESTMENT_INCOME+Q2.OTH_INCOME+Q2.INTERBRANCH_INCOME+Q2.INTERBRANCH_EXPENSE) NET_PL,  "+
                  "      (Q2.NET_INTEREST_INCOME_MTD_LCY+Q2.DERIVATIVES_INCOME_MTD_LCY+Q2.FEE_INCOME_MTD_LCY+Q2.FX_INCOME_MTD_LCY+Q2.NET_INVESTMENT_INCOME_MTD_LCY+Q2.OTH_INCOME_MTD_LCY+Q2.INTERBRANCH_INCOME_MTD_LCY+Q2.INTERBRANCH_EXPENSE_MTD_LCY) NET_PL_MTD_LCY,  "+
                  "      (Q2.NET_INTEREST_INCOME_YTD_AED+Q2.DERIVATIVES_INCOME_YTD_AED+Q2.FEE_INCOME_YTD_AED+Q2.FX_INCOME_YTD_AED+Q2.NET_INVESTMENT_INCOME_YTD_AED+Q2.OTH_INCOME_YTD_AED+Q2.INTERBRANCH_INCOME_YTD_AED+Q2.INTERBRANCH_EXPENSE_YTD_AED) NET_PL_YTD_AED,  "+
                  "      (Q2.NET_INTEREST_INCOME_YTD_LCY+Q2.DERIVATIVES_INCOME_YTD_LCY+Q2.FEE_INCOME_YTD_LCY+Q2.FX_INCOME_YTD_LCY+Q2.NET_INVESTMENT_INCOME_YTD_LCY+Q2.OTH_INCOME_YTD_LCY+Q2.INTERBRANCH_INCOME_YTD_LCY+Q2.INTERBRANCH_EXPENSE_YTD_LCY) NET_PL_YTD_LCY  "+
                  "    FROM  "+
                  "      (SELECT Q1.*,  "+
                  "        (Q1.GROSS_INTEREST_INCOME         + Q1.ASSET_COF + Q1.LP_CHARGE + Q1.GROSS_INTEREST_EXPENSE + Q1.LIABILITY_COF + Q1.LP_CREDIT) NET_INTEREST_INCOME,  "+
                  "        (Q1.GROSS_INTEREST_INCOME_MTD_LCY + Q1.ASSET_COF_MTD_LCY + Q1.LP_CHARGE_MTD_LCY + Q1.GROSS_INTEREST_EXPENSE_MTD_LCY + Q1.LIABILITY_COF_MTD_LCY + Q1.LP_CREDIT_MTD_LCY) NET_INTEREST_INCOME_MTD_LCY,  "+
                  "        (Q1.GROSS_INTEREST_INCOME_YTD_AED + Q1.ASSET_COF_YTD_AED + Q1.LP_CHARGE_YTD_AED + Q1.GROSS_INTEREST_EXPENSE_YTD_AED + Q1.LIABILITY_COF_YTD_AED + Q1.LP_CREDIT_YTD_AED) NET_INTEREST_INCOME_YTD_AED,  "+
                  "        (Q1.GROSS_INTEREST_INCOME_YTD_LCY + Q1.ASSET_COF_YTD_LCY + Q1.LP_CHARGE_YTD_LCY + Q1.GROSS_INTEREST_EXPENSE_YTD_LCY + Q1.LIABILITY_COF_YTD_LCY + Q1.LP_CREDIT_YTD_LCY) NET_INTEREST_INCOME_YTD_LCY,  "+
                  "        (Q1.ASSET_COF                     + Q1.LP_CHARGE + Q1.LIABILITY_COF + Q1.LP_CREDIT) TOTAL_FTP_AMT_MTD_AED,  "+
                  "        (Q1.ASSET_COF_MTD_LCY             + Q1.LP_CHARGE_MTD_LCY + Q1.LIABILITY_COF_MTD_LCY + Q1.LP_CREDIT_MTD_LCY) TOTAL_FTP_AMT_MTD_LCY,  "+
                  "        (Q1.ASSET_COF_YTD_AED             + Q1.LP_CHARGE_YTD_AED + Q1.LIABILITY_COF_YTD_AED + Q1.LP_CREDIT_YTD_AED) TOTAL_FTP_AMT_YTD_AED,  "+
                  "        (Q1.ASSET_COF_YTD_LCY             + Q1.LP_CHARGE_YTD_LCY + Q1.LIABILITY_COF_YTD_LCY + Q1.LP_CREDIT_YTD_LCY) TOTAL_FTP_AMT_YTD_LCY  "+
                  "      FROM  "+
                  "        (SELECT * FROM OSX_SEG_PL_DATA )Q1  "+
                  "      )Q2  "+
                  "    )Q3  "+
                  "  group by Q3.AMOUNT_CLASS, Q3.BANKING_TYPE, Q3.BANKING_TYPE_CUSTOMER, Q3.CATEGORY_CODE, Q3.CONTRACT_ID, Q3.CURRENCY_CODE, Q3.CUSTOMER_NUMBER, Q3.CUSTOMER_SEGMENT_CODE, Q3.DOMAIN_ID, Q3.FINAL_SEGMENT, Q3.LEGAL_ENTITY, Q3.PROFIT_CENTRE, Q3.SOURCE_SYSTEM_ID, 1  "+
                  ")PL ON BS.AMOUNT_CLASS     = PL.AMOUNT_CLASS  "+
                  "AND BS.BANKING_TYPE          = PL.BANKING_TYPE  "+
                  "AND BS.BANKING_TYPE_CUSTOMER = PL.BANKING_TYPE_CUSTOMER  "+
                  "AND BS.CATEGORY_CODE         = PL.CATEGORY_CODE  "+
                  "AND BS.CONTRACT_ID           = PL.CONTRACT_ID  "+
                  "AND BS.CURRENCY_CODE         = PL.CURRENCY_CODE  "+
                  "AND BS.CUSTOMER_NUMBER       = PL.CUSTOMER_NUMBER  "+
                  "AND BS.CUSTOMER_SEGMENT_CODE = PL.CUSTOMER_SEGMENT_CODE  "+
                  "AND BS.DOMAIN_ID             = PL.DOMAIN_ID  "+
                  "AND BS.FINAL_SEGMENT         = PL.FINAL_SEGMENT  "+
                  "AND BS.LEGAL_ENTITY          = PL.LEGAL_ENTITY  "+
                  "AND BS.PROFIT_CENTRE         = PL.PROFIT_CENTRE  "+
                  "AND BS.SOURCE_SYSTEM_ID      = PL.SOURCE_SYSTEM_ID  "+
                  "AND RANK_BS                  = RANK_PL "+
                  "UNION ALL "+
                  "SELECT  "+
                  "    TIME_KEY, "+
                  "    CAST(1 AS INT) VERSION_ID, "+
                  "    AMOUNT_CLASS, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    CAST(AVG_BOOK_BAL_LTD_ACY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_LTD_AED AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_LTD_LCY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_MTD_ACY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_MTD_AED AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_MTD_LCY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_YTD_ACY AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_YTD_AED AS DOUBLE), "+
                  "    CAST(AVG_BOOK_BAL_YTD_LCY AS DOUBLE), "+
                  "    BALANCE_SHEET_TYPE, "+
                  "    BANKING_TYPE, "+
                  "    BANKING_TYPE_CUSTOMER, "+
                  "    BS_PL_FLAG, "+
                  "    CATEGORY_CODE, "+
                  "    COMMON_KEY, "+
                  "    CONTRACT_ID, "+
                  "    CURRENCY_CODE, "+
                  "    CUSTOMER_NUMBER, "+
                  "    CUSTOMER_SEGMENT_CODE, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    DOMAIN_ID, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    FINAL_SEGMENT, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    GL_ACCOUNT_ID, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    IS_INTERNAL_ACCOUNT, "+
                  "    LEGAL_ENTITY, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    MTD_ACY, "+
                  "    MTD_AED, "+
                  "    MTD_LCY, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    CAST(OUTSTANDING_BALANCE_MTD_ACY AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_MTD_AED AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_MTD_LCY AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_YTD_ACY AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_YTD_AED AS DOUBLE), "+
                  "    CAST(OUTSTANDING_BALANCE_YTD_LCY AS DOUBLE), "+
                  "    PROFIT_CENTRE, "+
                  "    SOURCE_SYSTEM_ID, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    NULL, "+
                  "    'Y' as LAST_REFRESH_FLAG "+
                  "FROM OSX_SEG_BASE_AGG WHERE BS_PL_FLAG = 'PL' "
          )
          
          //osx_seg_insert.show(30)
          
          println (process_sumx_agg.time+" "+  "OSX BASE AGG - WRITING LATEST VERSION TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_seg_base_agg)
          
          osx_seg_base.repartition(200)
                        .write
                        .partitionBy("time_key", "last_refresh_flag", "version_id", "amount_class", "domain_id")
                        .mode("append")
                        .format("parquet")
                        .option("compression","snappy")
                        .save(hdfs_seg_base_agg)
          
          println (process_sumx_agg.time+" "+ "OSX BASE AGG - AGGREGATE LOAD COMPLETED")
          
          osx_seg_base.persist(StorageLevel.MEMORY_ONLY_SER)
          
          //CALLING THE TRIAL BALANCE PROCESSING FUNCTION
          println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - LOAD CALLED FROM BASE LOAD FUNCTION")
          load_trial_balance.osx_seg_trial_balance_load(sc, sqlContext, time_key, dom, acc, le, osx_cust, osx_cont, osx_seg_base, overwrite_flag,0)
         
       }             
                   
      
  }
  
}
