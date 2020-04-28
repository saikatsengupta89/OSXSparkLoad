package sumx_aggregate

import org.apache.log4j._
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED

/* for writing logs w.r.t. data load process */
import java.io._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import sys.process._
import org.apache.spark.SparkContext

object load_customer_agg {
      
    def osx_seg_customer_agg_load (sc:SparkContext,
                                   sqlContext:SQLContext, 
                                   time_key:String,
                                   sumx_data:DataFrame, 
                                   dom:DataFrame, 
                                   acc:DataFrame, 
                                   hdfs_seg_cust_agg:String,
                                   overwrite_flag:Integer) 
    {
      
       /**************************** CREATING CUSTOMER LEVEL AGGREGATE FROM BASE LEVEL ******************************/
       
       
       val hdfs_seg_cust_agg_r= "/data/fin_onesumx/fin_fct_osx_segmental_cust_agg_rpt/time_key="+time_key
       val hdfs_seg_cust_agg  = "/data/fin_onesumx/fin_fct_osx_segmental_cust_agg_rpt"
       val hdfs_seg_temp      = "/data/fin_onesumx/temp"
       
       val fs= FileSystem.get(sc.hadoopConfiguration)
       var version_id=0
       
//       //DIRECTORY EXISTS ALREADY. REMOVE THE EXISTING DIRECTORY AND THEN APPEND DATA FOR THE TIMEKEY
//        println (process_sumx_agg.time+" "+"OSX CUST AGG - DIRECTORY FLAG FOR PROCESSING TIME_KEY :"+ ("hadoop fs -test -d ".concat(hdfs_seg_cust_agg_r).!))
//        if(("hadoop fs -test -d ".concat(hdfs_seg_cust_agg_r).!)==0)  
//        {
//           println (process_sumx_agg.time+" "+ "OSX CUST AGG - DELETE EXISTING PARTITION FOR RELOAD")
//           val remove_dir = "hadoop fs -rm -r ".concat(hdfs_seg_cust_agg_r).!
//           if(remove_dir==0) {
//             
//             println (process_sumx_agg.time+" "+"OSX CUST AGG - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE TRIAL BAL RELOAD. LOAD IN PROGRESS")
//             //pw.println (process_sumx_agg.time+" "+"OSX BASE - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE BASE AGG RELOAD. LOAD IN PROGRESS")
//           }
//        }
       
       println(process_sumx_agg.time+" "+"OSX CUST AGG - DIMENSION TABLES INITIATED FOR LOOKUP")
       //pw.println(process_sumx_agg.time+" "+"OSX CUST - DIMENSION TABLES INITIATED FOR LOOKUP")
       
       sumx_data.registerTempTable("OSX_DEAL_DATA")
       dom.registerTempTable("OSX_DOM")
       acc.registerTempTable("OSX_ACC_FPA")
       
       println(process_sumx_agg.time+" "+"OSX CUST AGG - CUSTOMER AGGREGATE DATAFRAME CREATED")
       //pw.println(process_sumx_agg.time+" "+"OSX CUST - CUSTOMER AGGREGATE DATAFRAME CREATED")
       
       if (overwrite_flag==0 && ("hadoop fs -test -d ".concat(hdfs_seg_cust_agg_r).!)==0)
       {
          
          println (process_sumx_agg.time+" "+  "OSX CUST AGG - PROCESSING ADDITIONAL VERSION LOAD OF CUSTOMER DATASET. APPLICABLE ONLY FOR MONTHLY LOAD SCENARIO" )
                    
          /* LOOKUP THE DATASET IN THE LAST_REFRESH_FLAG=Y */
          fs.listStatus(new Path(s"$hdfs_seg_cust_agg/time_key=$time_key/last_refresh_flag=Y"))
            .filter(_.isDir)
            .map(_.getPath)
            .foreach(x=> {
             version_id+=x.toString.substring(x.toString.lastIndexOf("=")+1).toInt
          })
          
          /* CHECK IF THERE IS FILE AVAILABLE THEN MOVE THE FILE TO LAST_REFRESH_FLAG=N */
          println(process_sumx_agg.time+" "+"OSX CUST AGG - MOVING THE LATEST VERSION FILE FROM LAST REFRESH FLAG Y TO N")
          if (version_id > 0) {
             
             val exist_dir = "hadoop fs -test -d ".concat(s"$hdfs_seg_cust_agg/time_key=$time_key/last_refresh_flag=N").!                     
             if (exist_dir==1) {
                  "hadoop fs -mkdir ".concat(s"$hdfs_seg_cust_agg/time_key=$time_key/last_refresh_flag=N").!
             }
            
             val move_last_dir = (s"hadoop fs -mv $hdfs_seg_cust_agg/time_key=$time_key/last_refresh_flag=Y/version_id=$version_id"+" "+
                                  s"$hdfs_seg_cust_agg/time_key=$time_key/last_refresh_flag=N/version_id=$version_id").!
                                  
             if (move_last_dir==0){
                println(process_sumx_agg.time+" "+"OSX CUST AGG - FILE MOVED SUCCESSFULLY")
             }
             else {
                println(process_sumx_agg.time+" "+"OSX CUST AGG - FILE DID NOT MOVE SUCCESSFULLY. TERMINATING THE PROCESS")
             }
             
          }
          else {
                println(process_sumx_agg.time+" "+"OSX CUST AGG - THERE WERE NO FILE IN LAST REFRESH FLAG FOLDER PATH. THIS IS AN EXCEPTION. TERMINATING PROCESS")
          }
                    
          /* CREATING A TEMPORARY LOOKUP TABLE WITH LATEST VERSION ID FOR BELOW OPERATIONS */
          println(process_sumx_agg.time+" "+"OSX CUST AGG - CREATING DUMMY TEMP VIEW WITH PREVIOUS VERSION ID FOR CURRENT VERSION LOAD")
          val last_version= sqlContext.sql (s"SELECT $version_id AS VERSION_ID")
          last_version.registerTempTable("LKP_LAST_VERSION")
          
          val cust_seg_data= sqlContext.sql (
              "SELECT  "+
              "TIME_KEY, "+
              "(CAST(LKP.VERSION_ID AS INT) + 1) AS VERSION_ID, "+
              "DOMAIN_ID, "+
              "AMOUNT_CLASS, "+
              "BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, "+
              "BS_PL_FLAG, "+
              "CURRENCY_CODE, "+
              "CUSTOMER_NUMBER, "+
              "CUSTOMER_SEGMENT_CODE, "+
              "DEPARTMENT_ID, "+
              "FINAL_SEGMENT, "+
              "GL_ACCOUNT_ID, "+
              "LEGAL_ENTITY, "+
              "PRODUCT_CODE, "+
              "PROFIT_CENTRE_CD, "+
              "SOURCE_SYSTEM_ID, "+
              "CAST(SUM(ASSET_COF) AS DOUBLE) ASSET_COF, "+
              "CAST(SUM(ASSET_COF_MTD_LCY) AS DOUBLE) ASSET_COF_MTD_LCY, "+
              "CAST(SUM(ASSET_COF_YTD_AED) AS DOUBLE) ASSET_COF_YTD_AED, "+
              "CAST(SUM(ASSET_COF_YTD_LCY) AS DOUBLE) ASSET_COF_YTD_LCY, "+
              "CAST(SUM(AVG_BOOK_BAL) AS DOUBLE) AVG_BOOK_BAL, "+
              "CAST(SUM(AVG_BOOK_BAL_LCY) AS DOUBLE) AVG_BOOK_BAL_LCY, "+
              "CAST(SUM(AVG_BOOK_BAL_YTD) AS DOUBLE) AVG_BOOK_BAL_YTD, "+
              "CAST(SUM(AVG_BOOK_BAL_YTD_LCY) AS DOUBLE) AVG_BOOK_BAL_YTD_LCY,   "+
              "CAST(SUM(DERIVATIVES_INCOME) AS DOUBLE) DERIVATIVES_INCOME, "+
              "CAST(SUM(DERIVATIVES_INCOME_MTD_LCY) AS DOUBLE) DERIVATIVES_INCOME_MTD_LCY, "+
              "CAST(SUM(DERIVATIVES_INCOME_YTD_AED) AS DOUBLE) DERIVATIVES_INCOME_YTD_AED, "+
              "CAST(SUM(DERIVATIVES_INCOME_YTD_LCY) AS DOUBLE) DERIVATIVES_INCOME_YTD_LCY, "+
              "CAST(SUM(FEE_INCOME) AS DOUBLE) FEE_INCOME, "+
              "CAST(SUM(FEE_INCOME_MTD_LCY) AS DOUBLE) FEE_INCOME_MTD_LCY, "+
              "CAST(SUM(FEE_INCOME_YTD_AED) AS DOUBLE) FEE_INCOME_YTD_AED, "+
              "CAST(SUM(FEE_INCOME_YTD_LCY) AS DOUBLE) FEE_INCOME_YTD_LCY, "+
              "CAST(SUM(FX_INCOME) AS DOUBLE) FX_INCOME, "+
              "CAST(SUM(FX_INCOME_MTD_LCY) AS DOUBLE) FX_INCOME_MTD_LCY, "+
              "CAST(SUM(FX_INCOME_YTD_AED) AS DOUBLE) FX_INCOME_YTD_AED, "+
              "CAST(SUM(FX_INCOME_YTD_LCY) AS DOUBLE) FX_INCOME_YTD_LCY, "+
              "CAST(SUM(GROSS_INTEREST_EXPENSE) AS DOUBLE) GROSS_INTEREST_EXPENSE, "+
              "CAST(SUM(GROSS_INTEREST_EXPENSE_MTD_LCY) AS DOUBLE) GROSS_INTEREST_EXPENSE_MTD_LCY, "+
              "CAST(SUM(GROSS_INTEREST_EXPENSE_YTD_AED) AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_AED, "+
              "CAST(SUM(GROSS_INTEREST_EXPENSE_YTD_LCY) AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_LCY, "+
              "CAST(SUM(GROSS_INTEREST_INCOME) AS DOUBLE) GROSS_INTEREST_INCOME, "+
              "CAST(SUM(GROSS_INTEREST_INCOME_MTD_LCY) AS DOUBLE) GROSS_INTEREST_INCOME_MTD_LCY, "+
              "CAST(SUM(GROSS_INTEREST_INCOME_YTD_AED) AS DOUBLE) GROSS_INTEREST_INCOME_YTD_AED, "+
              "CAST(SUM(GROSS_INTEREST_INCOME_YTD_LCY) AS DOUBLE) GROSS_INTEREST_INCOME_YTD_LCY, "+
              "CAST(SUM(INTERBRANCH_EXPENSE) AS DOUBLE) INTERBRANCH_EXPENSE, "+
              "CAST(SUM(INTERBRANCH_EXPENSE_MTD_LCY) AS DOUBLE) INTERBRANCH_EXPENSE_MTD_LCY, "+
              "CAST(SUM(INTERBRANCH_EXPENSE_YTD_AED) AS DOUBLE) INTERBRANCH_EXPENSE_YTD_AED, "+
              "CAST(SUM(INTERBRANCH_EXPENSE_YTD_LCY) AS DOUBLE) INTERBRANCH_EXPENSE_YTD_LCY, "+
              "CAST(SUM(INTERBRANCH_INCOME) AS DOUBLE) INTERBRANCH_INCOME, "+
              "CAST(SUM(INTERBRANCH_INCOME_MTD_LCY) AS DOUBLE) INTERBRANCH_INCOME_MTD_LCY, "+
              "CAST(SUM(INTERBRANCH_INCOME_YTD_AED) AS DOUBLE) INTERBRANCH_INCOME_YTD_AED, "+
              "CAST(SUM(INTERBRANCH_INCOME_YTD_LCY) AS DOUBLE) INTERBRANCH_INCOME_YTD_LCY, "+
              "CAST(SUM(LIABILITY_COF) AS DOUBLE) LIABILITY_COF, "+
              "CAST(SUM(LIABILITY_COF_MTD_LCY) AS DOUBLE) LIABILITY_COF_MTD_LCY, "+
              "CAST(SUM(LIABILITY_COF_YTD_AED) AS DOUBLE) LIABILITY_COF_YTD_AED, "+
              "CAST(SUM(LIABILITY_COF_YTD_LCY) AS DOUBLE) LIABILITY_COF_YTD_LCY, "+
              "CAST(SUM(LP_CHARGE) AS DOUBLE) LP_CHARGE, "+
              "CAST(SUM(LP_CHARGE_MTD_LCY) AS DOUBLE) LP_CHARGE_MTD_LCY, "+
              "CAST(SUM(LP_CHARGE_YTD_AED) AS DOUBLE) LP_CHARGE_YTD_AED, "+
              "CAST(SUM(LP_CHARGE_YTD_LCY) AS DOUBLE) LP_CHARGE_YTD_LCY, "+
              "CAST(SUM(LP_CREDIT) AS DOUBLE) LP_CREDIT, "+
              "CAST(SUM(LP_CREDIT_MTD_LCY) AS DOUBLE) LP_CREDIT_MTD_LCY, "+
              "CAST(SUM(LP_CREDIT_YTD_AED) AS DOUBLE) LP_CREDIT_YTD_AED, "+
              "CAST(SUM(LP_CREDIT_YTD_LCY) AS DOUBLE) LP_CREDIT_YTD_LCY, "+
              "CAST(SUM(NET_INT_MARGIN) AS DOUBLE) NET_INT_MARGIN, "+
              "CAST(SUM(NET_INT_MARGIN_MTD_LCY) AS DOUBLE) NET_INT_MARGIN_MTD_LCY, "+
              "CAST(SUM(NET_INT_MARGIN_YTD_AED) AS DOUBLE) NET_INT_MARGIN_YTD_AED, "+
              "CAST(SUM(NET_INT_MARGIN_YTD_LCY) AS DOUBLE) NET_INT_MARGIN_YTD_LCY, "+
              "CAST(SUM(NET_INTEREST_INCOME) AS DOUBLE) NET_INTEREST_INCOME, "+
              "CAST(SUM(NET_INTEREST_INCOME_MTD_LCY) AS DOUBLE) NET_INTEREST_INCOME_MTD_LCY, "+
              "CAST(SUM(NET_INTEREST_INCOME_YTD_AED) AS DOUBLE) NET_INTEREST_INCOME_YTD_AED, "+
              "CAST(SUM(NET_INTEREST_INCOME_YTD_LCY) AS DOUBLE) NET_INTEREST_INCOME_YTD_LCY, "+
              "CAST(SUM(NET_INVESTMENT_INCOME) AS DOUBLE) NET_INVESTMENT_INCOME, "+
              "CAST(SUM(NET_INVESTMENT_INCOME_MTD_LCY) AS DOUBLE) NET_INVESTMENT_INCOME_MTD_LCY, "+
              "CAST(SUM(NET_INVESTMENT_INCOME_YTD_AED) AS DOUBLE) NET_INVESTMENT_INCOME_YTD_AED, "+
              "CAST(SUM(NET_INVESTMENT_INCOME_YTD_LCY) AS DOUBLE) NET_INVESTMENT_INCOME_YTD_LCY, "+
              "CAST(SUM(NET_PL) AS DOUBLE) NET_PL, "+
              "CAST(SUM(NET_PL_MTD_LCY) AS DOUBLE) NET_PL_MTD_LCY, "+
              "CAST(SUM(NET_PL_YTD_AED) AS DOUBLE) NET_PL_YTD_AED, "+
              "CAST(SUM(NET_PL_YTD_LCY) AS DOUBLE) NET_PL_YTD_LCY, "+
              "CAST(SUM(OTH_INCOME) AS DOUBLE) OTH_INCOME, "+
              "CAST(SUM(OTHER_INCOME_MTD_LCY) AS DOUBLE) OTHER_INCOME_MTD_LCY, "+
              "CAST(SUM(OTHER_INCOME_YTD_AED) AS DOUBLE) OTHER_INCOME_YTD_AED, "+
              "CAST(SUM(OTHER_INCOME_YTD_LCY) AS DOUBLE) OTHER_INCOME_YTD_LCY, "+
              "CAST(SUM(OUTSTANDING_BALANCE_MTD) AS DOUBLE) OUTSTANDING_BALANCE_MTD, "+
              "CAST(SUM(OUTSTANDING_BALANCE_MTD_LCY) AS DOUBLE) OUTSTANDING_BALANCE_MTD_LCY, "+
              "CAST(SUM(OUTSTANDING_BALANCE_YTD) AS DOUBLE) OUTSTANDING_BALANCE_YTD, "+
              "CAST(SUM(OUTSTANDING_BALANCE_YTD_LCY) AS DOUBLE) OUTSTANDING_BALANCE_YTD_LCY, "+
              "CAST(SUM(RWA_CREDIT_RISK) AS DOUBLE) RWA_CREDIT_RISK, "+
              "CAST(SUM(RWA_MARKET_RISK) AS DOUBLE) RWA_MARKET_RISK, "+
              "CAST(SUM(RWA_OPERATIONAL_RISK) AS DOUBLE) RWA_OPERATIONAL_RISK, "+
              "CAST(SUM(RWA_TOTAL) AS DOUBLE) RWA_TOTAL, "+
              "CAST(SUM(TOTAL_FTP_AMT) AS DOUBLE) TOTAL_FTP_AMT, "+
              "CAST(SUM(TOTAL_FTP_AMT_LCY) AS DOUBLE) TOTAL_FTP_AMT_LCY, "+
              "CAST(SUM(TOTAL_FTP_AMT_YTD) AS DOUBLE) TOTAL_FTP_AMT_YTD, "+
              "'Y' LAST_REFRESH_FLAG "+
              "FROM "+
              "(SELECT (RWA_CREDIT_RISK + RWA_MARKET_RISK + RWA_MARKET_RISK) RWA_TOTAL, "+
              "Q1.* "+
              "FROM "+
              "(SELECT AMOUNT_CLASS, "+
              "BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, "+
              "BS_PL_FLAG, "+
              "CURRENCY_CODE, "+
              "CUSTOMER_NUMBER, "+
              "CUSTOMER_SEGMENT_CODE, "+
              "FINAL_SEGMENT DEPARTMENT_ID, "+
              "AGG.DOMAIN_ID, "+
              "FINAL_SEGMENT, "+
              "AGG.GL_ACCOUNT_ID, "+
              "LEGAL_ENTITY, "+
              "CATEGORY_CODE PRODUCT_CODE, "+
              "PROFIT_CENTRE PROFIT_CENTRE_CD, "+
              "SOURCE_SYSTEM_ID, "+
              "TIME_KEY, "+
              "ASSET_COF, "+
              "ASSET_COF_MTD_LCY, "+
              "ASSET_COF_YTD_AED, "+
              "ASSET_COF_YTD_LCY, "+
              "AVG_BOOK_BAL_MTD_AED AVG_BOOK_BAL, "+
              "AVG_BOOK_BAL_MTD_LCY AVG_BOOK_BAL_LCY, "+
              "AVG_BOOK_BAL_YTD_AED AVG_BOOK_BAL_YTD, "+
              "AVG_BOOK_BAL_YTD_LCY AVG_BOOK_BAL_YTD_LCY, "+
              "DERIVATIVES_INCOME, "+
              "DERIVATIVES_INCOME_MTD_LCY, "+
              "DERIVATIVES_INCOME_YTD_AED, "+
              "DERIVATIVES_INCOME_YTD_LCY, "+
              "FEE_INCOME, "+
              "FEE_INCOME_MTD_LCY, "+
              "FEE_INCOME_YTD_AED, "+
              "FEE_INCOME_YTD_LCY, "+
              "FX_INCOME, "+
              "FX_INCOME_MTD_LCY, "+
              "FX_INCOME_YTD_AED, "+
              "FX_INCOME_YTD_LCY, "+
              "GROSS_INTEREST_EXPENSE, "+
              "GROSS_INTEREST_EXPENSE_MTD_LCY, "+
              "GROSS_INTEREST_EXPENSE_YTD_AED, "+
              "GROSS_INTEREST_EXPENSE_YTD_LCY, "+
              "GROSS_INTEREST_INCOME, "+
              "GROSS_INTEREST_INCOME_MTD_LCY, "+
              "GROSS_INTEREST_INCOME_YTD_AED, "+
              "GROSS_INTEREST_INCOME_YTD_LCY, "+
              "INTERBRANCH_EXPENSE, "+
              "INTERBRANCH_EXPENSE_MTD_LCY, "+
              "INTERBRANCH_EXPENSE_YTD_AED, "+
              "INTERBRANCH_EXPENSE_YTD_LCY, "+
              "INTERBRANCH_INCOME, "+
              "INTERBRANCH_INCOME_MTD_LCY, "+
              "INTERBRANCH_INCOME_YTD_AED, "+
              "INTERBRANCH_INCOME_YTD_LCY, "+
              "LIABILITY_COF, "+
              "LIABILITY_COF_MTD_LCY, "+
              "LIABILITY_COF_YTD_AED, "+
              "LIABILITY_COF_YTD_LCY, "+
              "LP_CHARGE, "+
              "LP_CHARGE_MTD_LCY, "+
              "LP_CHARGE_YTD_AED, "+
              "LP_CHARGE_YTD_LCY, "+
              "LP_CREDIT, "+
              "LP_CREDIT_MTD_LCY, "+
              "LP_CREDIT_YTD_AED, "+
              "LP_CREDIT_YTD_LCY, "+
              "NET_INT_MARGIN, "+
              "NET_INT_MARGIN_MTD_LCY, "+
              "NET_INT_MARGIN_YTD_AED, "+
              "NET_INT_MARGIN_YTD_LCY, "+
              "NET_INTEREST_INCOME, "+
              "NET_INTEREST_INCOME_MTD_LCY, "+
              "NET_INTEREST_INCOME_YTD_AED, "+
              "NET_INTEREST_INCOME_YTD_LCY, "+
              "NET_INVESTMENT_INCOME, "+
              "NET_INVESTMENT_INCOME_MTD_LCY, "+
              "NET_INVESTMENT_INCOME_YTD_AED, "+
              "NET_INVESTMENT_INCOME_YTD_LCY, "+
              "NET_PL, "+
              "NET_PL_MTD_LCY, "+
              "NET_PL_YTD_AED, "+
              "NET_PL_YTD_LCY, "+
              "OTH_INCOME, "+
              "OTH_INCOME_MTD_LCY OTHER_INCOME_MTD_LCY, "+
              "OTH_INCOME_YTD_AED OTHER_INCOME_YTD_AED, "+
              "OTH_INCOME_YTD_LCY OTHER_INCOME_YTD_LCY, "+
              "OUTSTANDING_BALANCE_MTD_AED OUTSTANDING_BALANCE_MTD, "+
              "OUTSTANDING_BALANCE_MTD_LCY, "+
              "OUTSTANDING_BALANCE_YTD_AED OUTSTANDING_BALANCE_YTD, "+
              "OUTSTANDING_BALANCE_YTD_LCY, "+
              "CASE WHEN ((DOM.L4_CODE = '511100') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '913716')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_CREDIT_RISK, "+
              "CASE WHEN ((DOM.L4_CODE = '511200') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '813714')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_MARKET_RISK, "+
              "CASE WHEN ((DOM.L4_CODE = '511300') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '813715')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_OPERATIONAL_RISK, "+
              "TOTAL_FTP_AMT_MTD_AED TOTAL_FTP_AMT, "+
              "TOTAL_FTP_AMT_MTD_LCY TOTAL_FTP_AMT_LCY, "+
              "TOTAL_FTP_AMT_YTD_AED TOTAL_FTP_AMT_YTD "+
              "FROM "+
              "OSX_DEAL_DATA AGG "+
              "LEFT OUTER JOIN OSX_DOM DOM ON DOM.DOMAIN_ID= AGG.DOMAIN_ID "+
              "LEFT OUTER JOIN OSX_ACC_FPA ACC ON ACC.GL_ACCOUNT_ID= AGG.GL_ACCOUNT_ID "+
              ") Q1) "+
              "CROSS JOIN LKP_LAST_VERSION LKP "+
              "GROUP BY AMOUNT_CLASS, (CAST(LKP.VERSION_ID AS INT) + 1), BANKING_TYPE, BANKING_TYPE_CUSTOMER, BS_PL_FLAG, CURRENCY_CODE, CUSTOMER_NUMBER, "+
              "CUSTOMER_SEGMENT_CODE, DEPARTMENT_ID, DOMAIN_ID, FINAL_SEGMENT, GL_ACCOUNT_ID, LEGAL_ENTITY, PRODUCT_CODE, "+ 
              "PROFIT_CENTRE_CD, SOURCE_SYSTEM_ID, TIME_KEY"
          )
          
          println (process_sumx_agg.time+" "+  "OSX CUST AGG - WRITING DATAFRAME TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_seg_cust_agg)
          cust_seg_data.repartition(200)
                       .write
                       .partitionBy("time_key", "last_refresh_flag", "version_id", "domain_id")
                       .mode("append")
                       .format("parquet")
                       .option("compression","snappy")
                       .save(hdfs_seg_cust_agg)
                       
          println(process_sumx_agg.time+" "+ "OSX CUST AGG - AGGREGATE WRITE COMPLETED")
          
       }
       else {
         
         if(("hadoop fs -test -d ".concat(hdfs_seg_cust_agg_r).!)==0)  
          {
              println (process_sumx_agg.time+" "+  "OSX CUST AGG - DELETING EXISTING DIRECTORY ")
              val remove_dir = "hadoop fs -rm -r ".concat(hdfs_seg_cust_agg_r).!
              print(remove_dir)
              if(remove_dir==0) {
               
                println (process_sumx_agg.time+" "+  "OSX CUST AGG - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE CUSTOMER AGG RELOAD ")
                //pw.println ("OSX BASE - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE BASE AGG RELOAD. LOAD IN PROGRESS")
              }
              println (process_sumx_agg.time+" "+  "OSX CUST AGG - PROCESSING VERSION OVERWRITE OF CUSTOMER AGG DATASET. APPLICABLE ONLY FOR DAILY LOAD SCENARIO" )
           }
           else {
             
              println (process_sumx_agg.time+" "+  "OSX CUST AGG - PROCESSING FIRST VERSION OF CUSTOMER AGG DATASET. APPLICABLE FOR BOTH DAILY AND MONTHLY LOAD SCENARIO" )
          }
         
          val cust_seg_data= sqlContext.sql (
              "SELECT  "+
              "TIME_KEY, "+
              "CAST(1 AS INT) VERSION_ID, "+
              "DOMAIN_ID, "+
              "AMOUNT_CLASS, "+
              "BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, "+
              "BS_PL_FLAG, "+
              "CURRENCY_CODE, "+
              "CUSTOMER_NUMBER, "+
              "CUSTOMER_SEGMENT_CODE, "+
              "DEPARTMENT_ID, "+
              "FINAL_SEGMENT, "+
              "GL_ACCOUNT_ID, "+
              "LEGAL_ENTITY, "+
              "PRODUCT_CODE, "+
              "PROFIT_CENTRE_CD, "+
              "SOURCE_SYSTEM_ID, "+
              "CAST(SUM(ASSET_COF) AS DOUBLE) ASSET_COF, "+
              "CAST(SUM(ASSET_COF_MTD_LCY) AS DOUBLE) ASSET_COF_MTD_LCY, "+
              "CAST(SUM(ASSET_COF_YTD_AED) AS DOUBLE) ASSET_COF_YTD_AED, "+
              "CAST(SUM(ASSET_COF_YTD_LCY) AS DOUBLE) ASSET_COF_YTD_LCY, "+
              "CAST(SUM(AVG_BOOK_BAL) AS DOUBLE) AVG_BOOK_BAL, "+
              "CAST(SUM(AVG_BOOK_BAL_LCY) AS DOUBLE) AVG_BOOK_BAL_LCY, "+
              "CAST(SUM(AVG_BOOK_BAL_YTD) AS DOUBLE) AVG_BOOK_BAL_YTD, "+
              "CAST(SUM(AVG_BOOK_BAL_YTD_LCY) AS DOUBLE) AVG_BOOK_BAL_YTD_LCY,   "+
              "CAST(SUM(DERIVATIVES_INCOME) AS DOUBLE) DERIVATIVES_INCOME, "+
              "CAST(SUM(DERIVATIVES_INCOME_MTD_LCY) AS DOUBLE) DERIVATIVES_INCOME_MTD_LCY, "+
              "CAST(SUM(DERIVATIVES_INCOME_YTD_AED) AS DOUBLE) DERIVATIVES_INCOME_YTD_AED, "+
              "CAST(SUM(DERIVATIVES_INCOME_YTD_LCY) AS DOUBLE) DERIVATIVES_INCOME_YTD_LCY, "+
              "CAST(SUM(FEE_INCOME) AS DOUBLE) FEE_INCOME, "+
              "CAST(SUM(FEE_INCOME_MTD_LCY) AS DOUBLE) FEE_INCOME_MTD_LCY, "+
              "CAST(SUM(FEE_INCOME_YTD_AED) AS DOUBLE) FEE_INCOME_YTD_AED, "+
              "CAST(SUM(FEE_INCOME_YTD_LCY) AS DOUBLE) FEE_INCOME_YTD_LCY, "+
              "CAST(SUM(FX_INCOME) AS DOUBLE) FX_INCOME, "+
              "CAST(SUM(FX_INCOME_MTD_LCY) AS DOUBLE) FX_INCOME_MTD_LCY, "+
              "CAST(SUM(FX_INCOME_YTD_AED) AS DOUBLE) FX_INCOME_YTD_AED, "+
              "CAST(SUM(FX_INCOME_YTD_LCY) AS DOUBLE) FX_INCOME_YTD_LCY, "+
              "CAST(SUM(GROSS_INTEREST_EXPENSE) AS DOUBLE) GROSS_INTEREST_EXPENSE, "+
              "CAST(SUM(GROSS_INTEREST_EXPENSE_MTD_LCY) AS DOUBLE) GROSS_INTEREST_EXPENSE_MTD_LCY, "+
              "CAST(SUM(GROSS_INTEREST_EXPENSE_YTD_AED) AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_AED, "+
              "CAST(SUM(GROSS_INTEREST_EXPENSE_YTD_LCY) AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_LCY, "+
              "CAST(SUM(GROSS_INTEREST_INCOME) AS DOUBLE) GROSS_INTEREST_INCOME, "+
              "CAST(SUM(GROSS_INTEREST_INCOME_MTD_LCY) AS DOUBLE) GROSS_INTEREST_INCOME_MTD_LCY, "+
              "CAST(SUM(GROSS_INTEREST_INCOME_YTD_AED) AS DOUBLE) GROSS_INTEREST_INCOME_YTD_AED, "+
              "CAST(SUM(GROSS_INTEREST_INCOME_YTD_LCY) AS DOUBLE) GROSS_INTEREST_INCOME_YTD_LCY, "+
              "CAST(SUM(INTERBRANCH_EXPENSE) AS DOUBLE) INTERBRANCH_EXPENSE, "+
              "CAST(SUM(INTERBRANCH_EXPENSE_MTD_LCY) AS DOUBLE) INTERBRANCH_EXPENSE_MTD_LCY, "+
              "CAST(SUM(INTERBRANCH_EXPENSE_YTD_AED) AS DOUBLE) INTERBRANCH_EXPENSE_YTD_AED, "+
              "CAST(SUM(INTERBRANCH_EXPENSE_YTD_LCY) AS DOUBLE) INTERBRANCH_EXPENSE_YTD_LCY, "+
              "CAST(SUM(INTERBRANCH_INCOME) AS DOUBLE) INTERBRANCH_INCOME, "+
              "CAST(SUM(INTERBRANCH_INCOME_MTD_LCY) AS DOUBLE) INTERBRANCH_INCOME_MTD_LCY, "+
              "CAST(SUM(INTERBRANCH_INCOME_YTD_AED) AS DOUBLE) INTERBRANCH_INCOME_YTD_AED, "+
              "CAST(SUM(INTERBRANCH_INCOME_YTD_LCY) AS DOUBLE) INTERBRANCH_INCOME_YTD_LCY, "+
              "CAST(SUM(LIABILITY_COF) AS DOUBLE) LIABILITY_COF, "+
              "CAST(SUM(LIABILITY_COF_MTD_LCY) AS DOUBLE) LIABILITY_COF_MTD_LCY, "+
              "CAST(SUM(LIABILITY_COF_YTD_AED) AS DOUBLE) LIABILITY_COF_YTD_AED, "+
              "CAST(SUM(LIABILITY_COF_YTD_LCY) AS DOUBLE) LIABILITY_COF_YTD_LCY, "+
              "CAST(SUM(LP_CHARGE) AS DOUBLE) LP_CHARGE, "+
              "CAST(SUM(LP_CHARGE_MTD_LCY) AS DOUBLE) LP_CHARGE_MTD_LCY, "+
              "CAST(SUM(LP_CHARGE_YTD_AED) AS DOUBLE) LP_CHARGE_YTD_AED, "+
              "CAST(SUM(LP_CHARGE_YTD_LCY) AS DOUBLE) LP_CHARGE_YTD_LCY, "+
              "CAST(SUM(LP_CREDIT) AS DOUBLE) LP_CREDIT, "+
              "CAST(SUM(LP_CREDIT_MTD_LCY) AS DOUBLE) LP_CREDIT_MTD_LCY, "+
              "CAST(SUM(LP_CREDIT_YTD_AED) AS DOUBLE) LP_CREDIT_YTD_AED, "+
              "CAST(SUM(LP_CREDIT_YTD_LCY) AS DOUBLE) LP_CREDIT_YTD_LCY, "+
              "CAST(SUM(NET_INT_MARGIN) AS DOUBLE) NET_INT_MARGIN, "+
              "CAST(SUM(NET_INT_MARGIN_MTD_LCY) AS DOUBLE) NET_INT_MARGIN_MTD_LCY, "+
              "CAST(SUM(NET_INT_MARGIN_YTD_AED) AS DOUBLE) NET_INT_MARGIN_YTD_AED, "+
              "CAST(SUM(NET_INT_MARGIN_YTD_LCY) AS DOUBLE) NET_INT_MARGIN_YTD_LCY, "+
              "CAST(SUM(NET_INTEREST_INCOME) AS DOUBLE) NET_INTEREST_INCOME, "+
              "CAST(SUM(NET_INTEREST_INCOME_MTD_LCY) AS DOUBLE) NET_INTEREST_INCOME_MTD_LCY, "+
              "CAST(SUM(NET_INTEREST_INCOME_YTD_AED) AS DOUBLE) NET_INTEREST_INCOME_YTD_AED, "+
              "CAST(SUM(NET_INTEREST_INCOME_YTD_LCY) AS DOUBLE) NET_INTEREST_INCOME_YTD_LCY, "+
              "CAST(SUM(NET_INVESTMENT_INCOME) AS DOUBLE) NET_INVESTMENT_INCOME, "+
              "CAST(SUM(NET_INVESTMENT_INCOME_MTD_LCY) AS DOUBLE) NET_INVESTMENT_INCOME_MTD_LCY, "+
              "CAST(SUM(NET_INVESTMENT_INCOME_YTD_AED) AS DOUBLE) NET_INVESTMENT_INCOME_YTD_AED, "+
              "CAST(SUM(NET_INVESTMENT_INCOME_YTD_LCY) AS DOUBLE) NET_INVESTMENT_INCOME_YTD_LCY, "+
              "CAST(SUM(NET_PL) AS DOUBLE) NET_PL, "+
              "CAST(SUM(NET_PL_MTD_LCY) AS DOUBLE) NET_PL_MTD_LCY, "+
              "CAST(SUM(NET_PL_YTD_AED) AS DOUBLE) NET_PL_YTD_AED, "+
              "CAST(SUM(NET_PL_YTD_LCY) AS DOUBLE) NET_PL_YTD_LCY, "+
              "CAST(SUM(OTH_INCOME) AS DOUBLE) OTH_INCOME, "+
              "CAST(SUM(OTHER_INCOME_MTD_LCY) AS DOUBLE) OTHER_INCOME_MTD_LCY, "+
              "CAST(SUM(OTHER_INCOME_YTD_AED) AS DOUBLE) OTHER_INCOME_YTD_AED, "+
              "CAST(SUM(OTHER_INCOME_YTD_LCY) AS DOUBLE) OTHER_INCOME_YTD_LCY, "+
              "CAST(SUM(OUTSTANDING_BALANCE_MTD) AS DOUBLE) OUTSTANDING_BALANCE_MTD, "+
              "CAST(SUM(OUTSTANDING_BALANCE_MTD_LCY) AS DOUBLE) OUTSTANDING_BALANCE_MTD_LCY, "+
              "CAST(SUM(OUTSTANDING_BALANCE_YTD) AS DOUBLE) OUTSTANDING_BALANCE_YTD, "+
              "CAST(SUM(OUTSTANDING_BALANCE_YTD_LCY) AS DOUBLE) OUTSTANDING_BALANCE_YTD_LCY, "+
              "CAST(SUM(RWA_CREDIT_RISK) AS DOUBLE) RWA_CREDIT_RISK, "+
              "CAST(SUM(RWA_MARKET_RISK) AS DOUBLE) RWA_MARKET_RISK, "+
              "CAST(SUM(RWA_OPERATIONAL_RISK) AS DOUBLE) RWA_OPERATIONAL_RISK, "+
              "CAST(SUM(RWA_TOTAL) AS DOUBLE) RWA_TOTAL, "+
              "CAST(SUM(TOTAL_FTP_AMT) AS DOUBLE) TOTAL_FTP_AMT, "+
              "CAST(SUM(TOTAL_FTP_AMT_LCY) AS DOUBLE) TOTAL_FTP_AMT_LCY, "+
              "CAST(SUM(TOTAL_FTP_AMT_YTD) AS DOUBLE) TOTAL_FTP_AMT_YTD, "+
              "'Y' LAST_REFRESH_FLAG "+
              "FROM "+
              "(SELECT (RWA_CREDIT_RISK + RWA_MARKET_RISK + RWA_MARKET_RISK) RWA_TOTAL, "+
              "Q1.* "+
              "FROM "+
              "(SELECT AMOUNT_CLASS, "+
              "BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, "+
              "BS_PL_FLAG, "+
              "CURRENCY_CODE, "+
              "CUSTOMER_NUMBER, "+
              "CUSTOMER_SEGMENT_CODE, "+
              "FINAL_SEGMENT DEPARTMENT_ID, "+
              "AGG.DOMAIN_ID, "+
              "FINAL_SEGMENT, "+
              "AGG.GL_ACCOUNT_ID, "+
              "LEGAL_ENTITY, "+
              "CATEGORY_CODE PRODUCT_CODE, "+
              "PROFIT_CENTRE PROFIT_CENTRE_CD, "+
              "SOURCE_SYSTEM_ID, "+
              "TIME_KEY, "+
              "ASSET_COF, "+
              "ASSET_COF_MTD_LCY, "+
              "ASSET_COF_YTD_AED, "+
              "ASSET_COF_YTD_LCY, "+
              "AVG_BOOK_BAL_MTD_AED AVG_BOOK_BAL, "+
              "AVG_BOOK_BAL_MTD_LCY AVG_BOOK_BAL_LCY, "+
              "AVG_BOOK_BAL_YTD_AED AVG_BOOK_BAL_YTD, "+
              "AVG_BOOK_BAL_YTD_LCY AVG_BOOK_BAL_YTD_LCY, "+
              "DERIVATIVES_INCOME, "+
              "DERIVATIVES_INCOME_MTD_LCY, "+
              "DERIVATIVES_INCOME_YTD_AED, "+
              "DERIVATIVES_INCOME_YTD_LCY, "+
              "FEE_INCOME, "+
              "FEE_INCOME_MTD_LCY, "+
              "FEE_INCOME_YTD_AED, "+
              "FEE_INCOME_YTD_LCY, "+
              "FX_INCOME, "+
              "FX_INCOME_MTD_LCY, "+
              "FX_INCOME_YTD_AED, "+
              "FX_INCOME_YTD_LCY, "+
              "GROSS_INTEREST_EXPENSE, "+
              "GROSS_INTEREST_EXPENSE_MTD_LCY, "+
              "GROSS_INTEREST_EXPENSE_YTD_AED, "+
              "GROSS_INTEREST_EXPENSE_YTD_LCY, "+
              "GROSS_INTEREST_INCOME, "+
              "GROSS_INTEREST_INCOME_MTD_LCY, "+
              "GROSS_INTEREST_INCOME_YTD_AED, "+
              "GROSS_INTEREST_INCOME_YTD_LCY, "+
              "INTERBRANCH_EXPENSE, "+
              "INTERBRANCH_EXPENSE_MTD_LCY, "+
              "INTERBRANCH_EXPENSE_YTD_AED, "+
              "INTERBRANCH_EXPENSE_YTD_LCY, "+
              "INTERBRANCH_INCOME, "+
              "INTERBRANCH_INCOME_MTD_LCY, "+
              "INTERBRANCH_INCOME_YTD_AED, "+
              "INTERBRANCH_INCOME_YTD_LCY, "+
              "LIABILITY_COF, "+
              "LIABILITY_COF_MTD_LCY, "+
              "LIABILITY_COF_YTD_AED, "+
              "LIABILITY_COF_YTD_LCY, "+
              "LP_CHARGE, "+
              "LP_CHARGE_MTD_LCY, "+
              "LP_CHARGE_YTD_AED, "+
              "LP_CHARGE_YTD_LCY, "+
              "LP_CREDIT, "+
              "LP_CREDIT_MTD_LCY, "+
              "LP_CREDIT_YTD_AED, "+
              "LP_CREDIT_YTD_LCY, "+
              "NET_INT_MARGIN, "+
              "NET_INT_MARGIN_MTD_LCY, "+
              "NET_INT_MARGIN_YTD_AED, "+
              "NET_INT_MARGIN_YTD_LCY, "+
              "NET_INTEREST_INCOME, "+
              "NET_INTEREST_INCOME_MTD_LCY, "+
              "NET_INTEREST_INCOME_YTD_AED, "+
              "NET_INTEREST_INCOME_YTD_LCY, "+
              "NET_INVESTMENT_INCOME, "+
              "NET_INVESTMENT_INCOME_MTD_LCY, "+
              "NET_INVESTMENT_INCOME_YTD_AED, "+
              "NET_INVESTMENT_INCOME_YTD_LCY, "+
              "NET_PL, "+
              "NET_PL_MTD_LCY, "+
              "NET_PL_YTD_AED, "+
              "NET_PL_YTD_LCY, "+
              "OTH_INCOME, "+
              "OTH_INCOME_MTD_LCY OTHER_INCOME_MTD_LCY, "+
              "OTH_INCOME_YTD_AED OTHER_INCOME_YTD_AED, "+
              "OTH_INCOME_YTD_LCY OTHER_INCOME_YTD_LCY, "+
              "OUTSTANDING_BALANCE_MTD_AED OUTSTANDING_BALANCE_MTD, "+
              "OUTSTANDING_BALANCE_MTD_LCY, "+
              "OUTSTANDING_BALANCE_YTD_AED OUTSTANDING_BALANCE_YTD, "+
              "OUTSTANDING_BALANCE_YTD_LCY, "+
              "CASE WHEN ((DOM.L4_CODE = '511100') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '913716')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_CREDIT_RISK, "+
              "CASE WHEN ((DOM.L4_CODE = '511200') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '813714')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_MARKET_RISK, "+
              "CASE WHEN ((DOM.L4_CODE = '511300') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '813715')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_OPERATIONAL_RISK, "+
              "TOTAL_FTP_AMT_MTD_AED TOTAL_FTP_AMT, "+
              "TOTAL_FTP_AMT_MTD_LCY TOTAL_FTP_AMT_LCY, "+
              "TOTAL_FTP_AMT_YTD_AED TOTAL_FTP_AMT_YTD "+
              "FROM "+
              "OSX_DEAL_DATA AGG "+
              "LEFT OUTER JOIN OSX_DOM DOM ON DOM.DOMAIN_ID= AGG.DOMAIN_ID "+
              "LEFT OUTER JOIN OSX_ACC_FPA ACC ON ACC.GL_ACCOUNT_ID= AGG.GL_ACCOUNT_ID "+
              ") Q1) "+
              "GROUP BY AMOUNT_CLASS, BANKING_TYPE, BANKING_TYPE_CUSTOMER, BS_PL_FLAG, CURRENCY_CODE, CUSTOMER_NUMBER, "+
              "CUSTOMER_SEGMENT_CODE, DEPARTMENT_ID, DOMAIN_ID, FINAL_SEGMENT, GL_ACCOUNT_ID, LEGAL_ENTITY, PRODUCT_CODE, "+ 
              "PROFIT_CENTRE_CD, SOURCE_SYSTEM_ID, TIME_KEY"
          )
                    
          //cust_seg_data.show(5)
          //pw.println(process_sumx_agg.time+" "+"Customer aggregate loaded. Total Count" + cust_seg_data.count())
          //pw.println(process_sumx_agg.time+" "+ "OSX CUST - WRITING DATAFRAME TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_seg_cust_agg)
          
          /*
             cust_seg_data.repartition(5)
                          .write.mode("overwrite")
                          .option("compression","snappy")
                          .parquet (hdfs_cust_agg)
                  
             cust_seg_data.write
                          .mode("append")
                          .format("parquet")
                          .option("compression","snappy")
                          .insertInto("FIN_FCT_OSX_SEG_CUST_AGG_RPT")
                          
             cust_seg_data.write
                          .partitionBy("time_key")
                          .mode("append")
                          .format("parquet")
                          .option("compression","snappy")
                          .saveAsTable("fin_fct_osx_seg_cust_agg_rpt")
                       
                       
             cust_seg_data.write
                          .partitionBy("time_key")
                          
                          .mode("append")
                          .format("parquet")
                          .option("compression","snappy")
                          .save(hdfs_cust_agg_p)     
          */
          println (process_sumx_agg.time+" "+  "OSX CUST AGG - WRITING DATAFRAME TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_seg_cust_agg)
          cust_seg_data.repartition(200)
                       .write
                       .partitionBy("time_key", "last_refresh_flag", "version_id", "domain_id")
                       .mode("append")
                       .format("parquet")
                       .option("compression","snappy")
                       .save(hdfs_seg_cust_agg)
                       
          println(process_sumx_agg.time+" "+ "OSX CUST AGG - AGGREGATE WRITE COMPLETED")
         
       }

       
    }
  
}
