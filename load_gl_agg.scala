package sumx_aggregate

import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED

/* for writing logs w.r.t. data load process */
import java.io._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

import sys.process._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object load_gl_agg {
  
    
    def osx_seg_gl_agg_load (sc:SparkContext,
                             sqlContext:SQLContext, 
                             time_key:String,
                             sumx_data:DataFrame, 
                             sumx_data_lm1:DataFrame,
                             sumx_data_lm2:DataFrame,
                             sumx_data_lysm:DataFrame,
                             dom:DataFrame, 
                             acc:DataFrame,
                             pc:DataFrame,
                             le:DataFrame,
                             cov_geo:DataFrame,
                             prod:DataFrame,
                             dept:DataFrame, 
                             cust_seg:DataFrame,
                             hdfs_gl_agg:String,
                             overwrite_flag:Integer) {
      
      
        /**************************** CREATING GL LEVEL AGGREGATE FROM BASE LEVEL *********************************/
        
        println(process_sumx_agg.time+" "+ "OSX GL AGG - AGGREGATE LOAD INITIATED")
        //pw.println(process_sumx_agg.time+" "+ "OSX GL AGG - AGGREGATE LOAD INITIATED")
      
        val hdfs_seg_gl_agg_r = "/data/fin_onesumx/fin_fct_osx_segmental_gl_agg_rpt/time_key="+time_key
        val hdfs_seg_gl_agg   = "/data/fin_onesumx/fin_fct_osx_segmental_gl_agg_rpt"
        val hdfs_seg_temp     = "/data/fin_onesumx/temp"
        
        val fs= FileSystem.get(sc.hadoopConfiguration)
        var version_id=0
       
       //DIRECTORY EXISTS ALREADY. REMOVE THE EXISTING DIRECTORY AND THEN APPEND DATA FOR THE TIMEKEY
        /*println (process_sumx_agg.time+" "+"OSX GL AGG - SEG BASE DIRECTORY FLAG FOR PROCESSING TIME_KEY :"+ ("hadoop fs -test -d ".concat(hdfs_seg_gl_agg_r).!))
        if(("hadoop fs -test -d ".concat(hdfs_seg_gl_agg_r).!)==0)  
        {
           println (process_sumx_agg.time+" "+ "OSX GL AGG - DELETE EXISTING PARTITION FOR RELOAD")
           val remove_dir = "hadoop fs -rm -r ".concat(hdfs_seg_gl_agg_r).!
           print(remove_dir)
           if(remove_dir==0) {
             
             println("OSX GL AGG - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BRFORE GL AGGREGATE RELOAD. LOAD IN PROGRESS")
             //pw.println("OSX GL AGG - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BRFORE GL AGGREGATE RELOAD. LOAD IN PROGRESS")
           }
        } */
        
        println(process_sumx_agg.time+" "+"OSX GL AGG - DIMENSION TABLES REGISTERED FOR LOOKUP")
        //pw.println(process_sumx_agg.time+" "+"OSX GL AGG - DIMENSION TABLES INITIATED FOR LOOKUP")
       
        sumx_data.registerTempTable("FIN_FCT_OSX_SEG_GL_AGG_RPT")
        dom.registerTempTable("DIM_OSX_DOMAIN")
        acc.registerTempTable("DIM_OSX_GL_ACCOUNTS_FPA")
        sumx_data_lm1.registerTempTable("FIN_FCT_OSX_SEG_GL_AGG_RPT_LM1")
        sumx_data_lm2.registerTempTable("FIN_FCT_OSX_SEG_GL_AGG_RPT_LM2")
        sumx_data_lysm.registerTempTable("FIN_FCT_OSX_GL_AGG_RPT_LYSM")
        
        
        //pw.println(process_sumx_agg.time+" "+ "OSX GL AGG - GL AGGREGATE CURRENT MONTH DATAFRAME CREATED")
        
        val gl_agg_cm = sqlContext.sql(
            "SELECT "+
            "AMOUNT_CLASS, "+
            "BANKING_TYPE, "+
            "BANKING_TYPE_CUSTOMER, "+
            "CATEGORY_CODE, "+
            "CURRENCY_CODE, "+
            "CUSTOMER_SEGMENT_CODE, "+
            "DATASET, "+
            "DOMAIN_ID, "+
            "FINAL_SEGMENT, "+
            "GL_ACCOUNT_ID, "+
            "IS_INTERNAL_ACCOUNT, "+
            "LEGAL_ENTITY, "+
            "PROFIT_CENTRE, "+
            "SOURCE_SYSTEM_ID, "+
            "TIME_KEY, "+
            "SUM(ASSET_COF) ASSET_COF, "+
            "SUM(ASSET_COF_MTD_LCY) ASSET_COF_MTD_LCY, "+
            "SUM(ASSET_COF_YTD_AED) ASSET_COF_YTD_AED, "+
            "SUM(ASSET_COF_YTD_LCY) ASSET_COF_YTD_LCY, "+
            "SUM(AVG_BOOK_BAL) AVG_BOOK_BAL, "+
            "SUM(AVG_BOOK_BAL_MTD_LCY) AVG_BOOK_BAL_MTD_LCY, "+
            "SUM(AVG_BOOK_BAL_YTD_AED) AVG_BOOK_BAL_YTD_AED, "+
            "SUM(AVG_BOOK_BAL_YTD_LCY) AVG_BOOK_BAL_YTD_LCY, "+
            "SUM(CY_ED_BUDGET_MTD_AED) CY_ED_BUDGET_MTD_AED, "+
            "SUM(CY_ED_BUDGET_MTD_LCY) CY_ED_BUDGET_MTD_LCY, "+
            "SUM(CY_ED_BUDGET_YTD_AED) CY_ED_BUDGET_YTD_AED, "+
            "SUM(CY_ED_BUDGET_YTD_LCY) CY_ED_BUDGET_YTD_LCY, "+
            "SUM(DERIVATIVES_INCOME) DERIVATIVES_INCOME, "+
            "SUM(DERIVATIVES_INCOME_MTD_LCY) DERIVATIVES_INCOME_MTD_LCY, "+
            "SUM(DERIVATIVES_INCOME_YTD_AED) DERIVATIVES_INCOME_YTD_AED, "+
            "SUM(DERIVATIVES_INCOME_YTD_LCY) DERIVATIVES_INCOME_YTD_LCY, "+
            "SUM(FEE_INCOME) FEE_INCOME, "+
            "SUM(FEE_INCOME_MTD_LCY) FEE_INCOME_MTD_LCY, "+
            "SUM(FEE_INCOME_YTD_AED) FEE_INCOME_YTD_AED, "+
            "SUM(FEE_INCOME_YTD_LCY) FEE_INCOME_YTD_LCY, "+
            "SUM(FX_INCOME) FX_INCOME, "+
            "SUM(FX_INCOME_MTD_LCY) FX_INCOME_MTD_LCY, "+
            "SUM(FX_INCOME_YTD_AED) FX_INCOME_YTD_AED, "+
            "SUM(FX_INCOME_YTD_LCY) FX_INCOME_YTD_LCY, "+
            "SUM(GROSS_INTEREST_EXPENSE) GROSS_INTEREST_EXPENSE, "+
            "SUM(GROSS_INTEREST_EXPENSE_MTD_LCY) GROSS_INTEREST_EXPENSE_MTD_LCY, "+
            "SUM(GROSS_INTEREST_EXPENSE_YTD_AED) GROSS_INTEREST_EXPENSE_YTD_AED, "+
            "SUM(GROSS_INTEREST_EXPENSE_YTD_LCY) GROSS_INTEREST_EXPENSE_YTD_LCY, "+
            "SUM(GROSS_INTEREST_INCOME) GROSS_INTEREST_INCOME, "+
            "SUM(GROSS_INTEREST_INCOME_MTD_LCY) GROSS_INTEREST_INCOME_MTD_LCY, "+
            "SUM(GROSS_INTEREST_INCOME_YTD_AED) GROSS_INTEREST_INCOME_YTD_AED, "+
            "SUM(GROSS_INTEREST_INCOME_YTD_LCY) GROSS_INTEREST_INCOME_YTD_LCY, "+
            "SUM(INTERBRANCH_EXPENSE) INTERBRANCH_EXPENSE, "+
            "SUM(INTERBRANCH_EXPENSE_MTD_LCY) INTERBRANCH_EXPENSE_MTD_LCY, "+
            "SUM(INTERBRANCH_EXPENSE_YTD_AED) INTERBRANCH_EXPENSE_YTD_AED, "+
            "SUM(INTERBRANCH_EXPENSE_YTD_LCY) INTERBRANCH_EXPENSE_YTD_LCY, "+
            "SUM(INTERBRANCH_INCOME) INTERBRANCH_INCOME, "+
            "SUM(INTERBRANCH_INCOME_MTD_LCY) INTERBRANCH_INCOME_MTD_LCY, "+
            "SUM(INTERBRANCH_INCOME_YTD_AED) INTERBRANCH_INCOME_YTD_AED, "+
            "SUM(INTERBRANCH_INCOME_YTD_LCY) INTERBRANCH_INCOME_YTD_LCY, "+
            "SUM(LIABILITY_COF) LIABILITY_COF, "+
            "SUM(LIABILITY_COF_MTD_LCY) LIABILITY_COF_MTD_LCY, "+
            "SUM(LIABILITY_COF_YTD_AED) LIABILITY_COF_YTD_AED, "+
            "SUM(LIABILITY_COF_YTD_LCY) LIABILITY_COF_YTD_LCY, "+
            "SUM(LP_CHARGE) LP_CHARGE, "+
            "SUM(LP_CHARGE_MTD_LCY) LP_CHARGE_MTD_LCY, "+
            "SUM(LP_CHARGE_YTD_AED) LP_CHARGE_YTD_AED, "+
            "SUM(LP_CHARGE_YTD_LCY) LP_CHARGE_YTD_LCY, "+
            "SUM(LP_CREDIT) LP_CREDIT, "+
            "SUM(LP_CREDIT_MTD_LCY) LP_CREDIT_MTD_LCY, "+
            "SUM(LP_CREDIT_YTD_AED) LP_CREDIT_YTD_AED, "+
            "SUM(LP_CREDIT_YTD_LCY) LP_CREDIT_YTD_LCY, "+
            "SUM(MTD_AED) MTD_AED, "+
            "SUM(MTD_AED_ACTUAL) MTD_AED_ACTUAL, "+
            "SUM(MTD_AED_BUDGET) MTD_AED_BUDGET, "+
            "SUM(MTD_LCY) MTD_LCY, "+
            "SUM(MTD_LCY_ACTUAL) MTD_LCY_ACTUAL, "+
            "SUM(MTD_LCY_BUDGET) MTD_LCY_BUDGET, "+
            "SUM(NET_INT_MARGIN) NET_INT_MARGIN, "+
            "SUM(NET_INT_MARGIN_MTD_LCY) NET_INT_MARGIN_MTD_LCY, "+
            "SUM(NET_INT_MARGIN_YTD_AED) NET_INT_MARGIN_YTD_AED, "+
            "SUM(NET_INT_MARGIN_YTD_LCY) NET_INT_MARGIN_YTD_LCY, "+
            "SUM(NET_INTEREST_INCOME) NET_INTEREST_INCOME, "+
            "SUM(NET_INTEREST_INCOME_MTD_LCY) NET_INTEREST_INCOME_MTD_LCY, "+
            "SUM(NET_INTEREST_INCOME_YTD_AED) NET_INTEREST_INCOME_YTD_AED, "+
            "SUM(NET_INTEREST_INCOME_YTD_LCY) NET_INTEREST_INCOME_YTD_LCY, "+
            "SUM(NET_INVESTMENT_INCOME) NET_INVESTMENT_INCOME, "+
            "SUM(NET_INVESTMENT_INCOME_MTD_LCY) NET_INVESTMENT_INCOME_MTD_LCY, "+
            "SUM(NET_INVESTMENT_INCOME_YTD_AED) NET_INVESTMENT_INCOME_YTD_AED, "+
            "SUM(NET_INVESTMENT_INCOME_YTD_LCY) NET_INVESTMENT_INCOME_YTD_LCY, "+
            "SUM(NET_PL) NET_PL, "+
            "SUM(NET_PL_MTD_LCY) NET_PL_MTD_LCY, "+
            "SUM(NET_PL_YTD_AED) NET_PL_YTD_AED, "+
            "SUM(NET_PL_YTD_LCY) NET_PL_YTD_LCY, "+
            "SUM(OTHER_INCOME) OTHER_INCOME, "+
            "SUM(OTHER_INCOME_MTD_LCY) OTHER_INCOME_MTD_LCY, "+
            "SUM(OTHER_INCOME_YTD_AED) OTHER_INCOME_YTD_AED, "+
            "SUM(OTHER_INCOME_YTD_LCY) OTHER_INCOME_YTD_LCY, "+
            "SUM(RWA_CREDIT_RISK) RWA_CREDIT_RISK, "+
            "SUM(RWA_MARKET_RISK) RWA_MARKET_RISK, "+
            "SUM(RWA_OPERATIONAL_RISK) RWA_OPERATIONAL_RISK, "+
            "SUM(RWA_TOTAL) RWA_TOTAL, "+
            "SUM(TOTAL_FTP_AMT_MTD_AED) TOTAL_FTP_AMT_MTD_AED, "+
            "SUM(TOTAL_FTP_AMT_MTD_LCY) TOTAL_FTP_AMT_MTD_LCY, "+
            "SUM(TOTAL_FTP_AMT_YTD_AED) TOTAL_FTP_AMT_YTD_AED, "+
            "SUM(TOTAL_FTP_AMT_YTD_LCY) TOTAL_FTP_AMT_YTD_LCY, "+
            "SUM(YTD_AED) YTD_AED, "+
            "SUM(YTD_AED_ACTUAL) YTD_AED_ACTUAL, "+
            "SUM(YTD_AED_BUDGET) YTD_AED_BUDGET, "+
            "SUM(YTD_LCY) YTD_LCY, "+
            "SUM(YTD_LCY_ACTUAL) YTD_LCY_ACTUAL, "+
            "SUM(YTD_LCY_BUDGET) YTD_LCY_BUDGET "+
            "FROM "+
            "(SELECT (RWA_CREDIT_RISK+RWA_MARKET_RISK+RWA_OPERATIONAL_RISK) RWA_TOTAL, "+
            "Q1.* "+
            "FROM "+
            "(SELECT AMOUNT_CLASS, "+
            "BANKING_TYPE, "+
            "BANKING_TYPE_CUSTOMER, "+
            "CATEGORY_CODE, "+
            "CURRENCY_CODE, "+
            "CUSTOMER_SEGMENT_CODE, "+
            "'BASE' DATASET, "+
            "AGG.DOMAIN_ID, "+
            "FINAL_SEGMENT, "+
            "AGG.GL_ACCOUNT_ID, "+
            "AGG.IS_INTERNAL_ACCOUNT, "+
            "LEGAL_ENTITY, "+
            "PROFIT_CENTRE, "+
            "SOURCE_SYSTEM_ID, "+
            "TIME_KEY, "+          
            "ASSET_COF, "+
            "ASSET_COF_MTD_LCY, "+
            "ASSET_COF_YTD_AED, "+
            "ASSET_COF_YTD_LCY, "+
            "AVG_BOOK_BAL_MTD_AED AVG_BOOK_BAL, "+
            "AVG_BOOK_BAL_MTD_LCY, "+
            "AVG_BOOK_BAL_YTD_AED, "+
            "AVG_BOOK_BAL_YTD_LCY, "+
            "CASE WHEN DOM.L1_CODE = '400000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END CY_ED_BUDGET_MTD_AED, "+
            "CASE WHEN DOM.L1_CODE = '400000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END CY_ED_BUDGET_MTD_LCY, "+
            "CASE WHEN DOM.L1_CODE = '400000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END CY_ED_BUDGET_YTD_AED, "+
            "CASE WHEN DOM.L1_CODE = '400000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END CY_ED_BUDGET_YTD_LCY, "+
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
            "MTD_AED, "+
            "CASE WHEN DOM.L1_CODE = '100000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END MTD_AED_ACTUAL, "+
            "CASE WHEN DOM.L1_CODE = '200000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END MTD_AED_BUDGET, "+
            "MTD_LCY, "+
            "CASE WHEN DOM.L1_CODE = '100000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END MTD_LCY_ACTUAL, "+
            "CASE WHEN DOM.L1_CODE = '200000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END MTD_LCY_BUDGET, "+
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
            "OTH_INCOME OTHER_INCOME, "+
            "OTH_INCOME_MTD_LCY OTHER_INCOME_MTD_LCY, "+
            "OTH_INCOME_YTD_AED OTHER_INCOME_YTD_AED, "+
            "OTH_INCOME_YTD_LCY OTHER_INCOME_YTD_LCY, "+
            "CASE WHEN ((DOM.L4_CODE = '511100') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '913716')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_CREDIT_RISK, "+
            "CASE WHEN ((DOM.L4_CODE = '511200') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '813714')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_MARKET_RISK, "+
            "CASE WHEN ((DOM.L4_CODE = '511300') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '813715')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_OPERATIONAL_RISK, "+
            "TOTAL_FTP_AMT_MTD_AED, "+
            "TOTAL_FTP_AMT_MTD_LCY, "+
            "TOTAL_FTP_AMT_YTD_AED, "+
            "TOTAL_FTP_AMT_YTD_LCY, "+
            "NVL(OUTSTANDING_BALANCE_YTD_AED, 0 ) YTD_AED, "+
            "CASE WHEN DOM.L1_CODE = '100000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END YTD_AED_ACTUAL, "+
            "CASE WHEN DOM.L1_CODE = '200000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END YTD_AED_BUDGET, "+
            "NVL(OUTSTANDING_BALANCE_YTD_LCY, 0 ) YTD_LCY, "+
            "CASE WHEN DOM.L1_CODE = '100000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END YTD_LCY_ACTUAL, "+
            "CASE WHEN DOM.L1_CODE = '200000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END YTD_LCY_BUDGET "+
            "FROM "+
            "FIN_FCT_OSX_SEG_GL_AGG_RPT AGG "+ 
            "LEFT OUTER JOIN DIM_OSX_DOMAIN DOM ON (AGG.DOMAIN_ID = DOM.DOMAIN_ID) "+
            "LEFT OUTER JOIN DIM_OSX_GL_ACCOUNTS_FPA ACC ON (AGG.GL_ACCOUNT_ID = ACC.GL_ACCOUNT_ID) "+
            ")Q1 "+
            ") "+
            "GROUP BY AMOUNT_CLASS, BANKING_TYPE, BANKING_TYPE_CUSTOMER, CATEGORY_CODE, CURRENCY_CODE, CUSTOMER_SEGMENT_CODE, "+ 
            "DATASET, DOMAIN_ID, FINAL_SEGMENT, GL_ACCOUNT_ID, IS_INTERNAL_ACCOUNT, LEGAL_ENTITY, PROFIT_CENTRE, "+ 
            "SOURCE_SYSTEM_ID, TIME_KEY"
         )
         
         println(process_sumx_agg.time+" "+"OSX GL AGG - RUNNING MONTH GL BASE DATAFRAME CREATED")
          
         //pw.println(process_sumx_agg.time+" "+ "OSX GL AGG - GL AGGREGATE DATAFRAME FOR CURRENT-1 MONTH CREATED")
         
         //(CURR MONTH -1) GL AGGREGATE CREATION BASED ON REQUIRED FIELDS TO MERGE LM1 MTD
         val gl_agg_lm1= sqlContext.sql (
              "SELECT "+
              "LEGAL_ENTITY, "+
              "GL_ACCOUNT_ID, "+
              "PROFIT_CENTRE, "+
              "FINAL_SEGMENT, "+
              "CURRENCY_CODE, "+
              "CATEGORY_CODE, "+
              "CUSTOMER_SEGMENT_CODE, "+
              "SOURCE_SYSTEM_ID, "+
              "DOMAIN_ID, "+
              "IS_INTERNAL_ACCOUNT, "+
              "BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, "+
              "AMOUNT_CLASS, "+
              "SUM(MTD_AED_ACTUAL) MTD_AED_ACTUAL, "+
              "SUM(MTD_LCY_ACTUAL) MTD_LCY_ACTUAL "+
              "FROM FIN_FCT_OSX_SEG_GL_AGG_RPT_LM1 "+
              "GROUP BY LEGAL_ENTITY,GL_ACCOUNT_ID,PROFIT_CENTRE,FINAL_SEGMENT,CURRENCY_CODE,CATEGORY_CODE, "+
              "CUSTOMER_SEGMENT_CODE,SOURCE_SYSTEM_ID,DOMAIN_ID,IS_INTERNAL_ACCOUNT,BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, AMOUNT_CLASS")
              
         println(process_sumx_agg.time+" "+ "OSX GL AGG - GL AGGREGATE DATAFRAME FOR CURRENT-1 MONTH CREATED")
         
              
         //(CURR MONTH -2) GL AGGREGATE CREATION BASED ON REQUIRED FIELDS TO MERGE LM2 MTD
         val gl_agg_lm2= sqlContext.sql (
              "SELECT "+
              "LEGAL_ENTITY, "+
              "GL_ACCOUNT_ID, "+
              "PROFIT_CENTRE, "+
              "FINAL_SEGMENT, "+
              "CURRENCY_CODE, "+
              "CATEGORY_CODE, "+
              "CUSTOMER_SEGMENT_CODE, "+
              "SOURCE_SYSTEM_ID, "+
              "DOMAIN_ID, "+
              "IS_INTERNAL_ACCOUNT, "+
              "BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, "+
              "AMOUNT_CLASS, "+
              "SUM(MTD_AED_ACTUAL) MTD_AED_ACTUAL, "+
              "SUM(MTD_LCY_ACTUAL) MTD_LCY_ACTUAL "+
              "FROM FIN_FCT_OSX_SEG_GL_AGG_RPT_LM2 "+
              "GROUP BY LEGAL_ENTITY,GL_ACCOUNT_ID,PROFIT_CENTRE,FINAL_SEGMENT,CURRENCY_CODE,CATEGORY_CODE, "+
              "CUSTOMER_SEGMENT_CODE,SOURCE_SYSTEM_ID,DOMAIN_ID,IS_INTERNAL_ACCOUNT,BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, AMOUNT_CLASS")
          
          println(process_sumx_agg.time+" "+ "OSX GL AGG - GL AGGREGATE DATAFRAME FOR CURRENT-2 MONTHS CREATED")
          
          gl_agg_cm.registerTempTable("FIN_FCT_OSX_GL_AGG_CM")
          gl_agg_lm1.registerTempTable("FIN_FCT_OSX_GL_AGG_LM1")
          gl_agg_lm2.registerTempTable("FIN_FCT_OSX_GL_AGG_LM2")
          
          
          println(process_sumx_agg.time+" "+ "OSX GL AGG - TEMP TABLES CREATED FOR ALL THE DATAFRAMES")         
          
          
            
          //IF LAST YEAR SAME MONTH DATA AVAILABLE THEN CREATE AND REGISTER DATAFRAME  
          val gl_agg_lysm= sqlContext.sql (
              "SELECT "+
              "LEGAL_ENTITY, "+
              "GL_ACCOUNT_ID, "+
              "PROFIT_CENTRE, "+
              "FINAL_SEGMENT, "+
              "CURRENCY_CODE, "+
              "CATEGORY_CODE, "+
              "CUSTOMER_SEGMENT_CODE, "+
              "SOURCE_SYSTEM_ID, "+
              "DOMAIN_ID, "+
              "IS_INTERNAL_ACCOUNT, "+
              "BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, "+
              "AMOUNT_CLASS, "+
              "SUM(YTD_AED_ACTUAL) YTD_AED_ACTUAL, "+
              "SUM(YTD_LCY_ACTUAL) YTD_LCY_ACTUAL "+
              "FROM FIN_FCT_OSX_GL_AGG_RPT_LYSM "+
              "GROUP BY LEGAL_ENTITY,GL_ACCOUNT_ID,PROFIT_CENTRE,FINAL_SEGMENT,CURRENCY_CODE,CATEGORY_CODE, "+
              "CUSTOMER_SEGMENT_CODE,SOURCE_SYSTEM_ID,DOMAIN_ID,IS_INTERNAL_ACCOUNT,BANKING_TYPE, "+
              "BANKING_TYPE_CUSTOMER, AMOUNT_CLASS")
           
          gl_agg_lysm.registerTempTable("FIN_FCT_OSX_GL_AGG_LYSM")
          
          println(process_sumx_agg.time+" "+ "OSX GL AGG - BASED ON LYSM DATA AVAILABILITY DATAFRAME CREATED")
        
          val gl_agg_base= sqlContext.sql(
               "SELECT "+
                "A.AMOUNT_CLASS, "+
                "A.BANKING_TYPE, "+
                "A.BANKING_TYPE_CUSTOMER, "+
                "A.CATEGORY_CODE, "+
                "A.CURRENCY_CODE, "+
                "A.CUSTOMER_SEGMENT_CODE, "+
                "A.DATASET, "+
                "A.DOMAIN_ID, "+
                "A.FINAL_SEGMENT, "+
                "A.GL_ACCOUNT_ID, "+
                "A.IS_INTERNAL_ACCOUNT, "+
                "A.LEGAL_ENTITY, "+
                "A.PROFIT_CENTRE, "+
                "A.SOURCE_SYSTEM_ID, "+
                "A.TIME_KEY, "+
                "A.ASSET_COF, "+
                "A.ASSET_COF_MTD_LCY, "+
                "A.ASSET_COF_YTD_AED, "+
                "A.ASSET_COF_YTD_LCY, "+
                "A.AVG_BOOK_BAL, "+
                "A.AVG_BOOK_BAL_MTD_LCY, "+
                "A.AVG_BOOK_BAL_YTD_AED, "+
                "A.AVG_BOOK_BAL_YTD_LCY, "+
                "A.CY_ED_BUDGET_MTD_AED, "+
                "A.CY_ED_BUDGET_MTD_LCY, "+
                "A.CY_ED_BUDGET_YTD_AED, "+
                "A.CY_ED_BUDGET_YTD_LCY, "+
                "A.DERIVATIVES_INCOME, "+
                "A.DERIVATIVES_INCOME_MTD_LCY, "+
                "A.DERIVATIVES_INCOME_YTD_AED, "+
                "A.DERIVATIVES_INCOME_YTD_LCY, "+
                "A.FEE_INCOME, "+
                "A.FEE_INCOME_MTD_LCY, "+
                "A.FEE_INCOME_YTD_AED, "+
                "A.FEE_INCOME_YTD_LCY, "+
                "A.FX_INCOME, "+
                "A.FX_INCOME_MTD_LCY, "+
                "A.FX_INCOME_YTD_AED, "+
                "A.FX_INCOME_YTD_LCY, "+
                "A.GROSS_INTEREST_EXPENSE, "+
                "A.GROSS_INTEREST_EXPENSE_MTD_LCY, "+
                "A.GROSS_INTEREST_EXPENSE_YTD_AED, "+
                "A.GROSS_INTEREST_EXPENSE_YTD_LCY, "+
                "A.GROSS_INTEREST_INCOME, "+
                "A.GROSS_INTEREST_INCOME_MTD_LCY, "+
                "A.GROSS_INTEREST_INCOME_YTD_AED, "+
                "A.GROSS_INTEREST_INCOME_YTD_LCY, "+
                "A.INTERBRANCH_EXPENSE, "+
                "A.INTERBRANCH_EXPENSE_MTD_LCY, "+
                "A.INTERBRANCH_EXPENSE_YTD_AED, "+
                "A.INTERBRANCH_EXPENSE_YTD_LCY, "+
                "A.INTERBRANCH_INCOME, "+
                "A.INTERBRANCH_INCOME_MTD_LCY, "+
                "A.INTERBRANCH_INCOME_YTD_AED, "+
                "A.INTERBRANCH_INCOME_YTD_LCY, "+
                "A.LIABILITY_COF, "+
                "A.LIABILITY_COF_MTD_LCY, "+
                "A.LIABILITY_COF_YTD_AED, "+
                "A.LIABILITY_COF_YTD_LCY, "+
                "A.LP_CHARGE, "+
                "A.LP_CHARGE_MTD_LCY, "+
                "A.LP_CHARGE_YTD_AED, "+
                "A.LP_CHARGE_YTD_LCY, "+
                "A.LP_CREDIT, "+
                "A.LP_CREDIT_MTD_LCY, "+
                "A.LP_CREDIT_YTD_AED, "+
                "A.LP_CREDIT_YTD_LCY, "+
                "A.MTD_AED, "+
                "A.MTD_AED_ACTUAL, "+
                "A.MTD_AED_BUDGET, "+
                "A.MTD_LCY, "+
                "A.MTD_LCY_ACTUAL, "+
                "A.MTD_LCY_BUDGET, "+
                "A.NET_INT_MARGIN, "+
                "A.NET_INT_MARGIN_MTD_LCY, "+
                "A.NET_INT_MARGIN_YTD_AED, "+
                "A.NET_INT_MARGIN_YTD_LCY, "+
                "A.NET_INTEREST_INCOME, "+
                "A.NET_INTEREST_INCOME_MTD_LCY, "+
                "A.NET_INTEREST_INCOME_YTD_AED, "+
                "A.NET_INTEREST_INCOME_YTD_LCY, "+
                "A.NET_INVESTMENT_INCOME, "+
                "A.NET_INVESTMENT_INCOME_MTD_LCY, "+
                "A.NET_INVESTMENT_INCOME_YTD_AED, "+
                "A.NET_INVESTMENT_INCOME_YTD_LCY, "+
                "A.NET_PL, "+
                "A.NET_PL_MTD_LCY, "+
                "A.NET_PL_YTD_AED, "+
                "A.NET_PL_YTD_LCY, "+
                "A.OTHER_INCOME, "+
                "A.OTHER_INCOME_MTD_LCY, "+
                "A.OTHER_INCOME_YTD_AED, "+
                "A.OTHER_INCOME_YTD_LCY, "+
                "A.RWA_CREDIT_RISK, "+
                "A.RWA_MARKET_RISK, "+
                "A.RWA_OPERATIONAL_RISK, "+
                "A.RWA_TOTAL, "+
                "A.TOTAL_FTP_AMT_MTD_AED, "+
                "A.TOTAL_FTP_AMT_MTD_LCY, "+
                "A.TOTAL_FTP_AMT_YTD_AED, "+
                "A.TOTAL_FTP_AMT_YTD_LCY, "+
                "A.YTD_AED, "+
                "A.YTD_AED_ACTUAL, "+
                "A.YTD_AED_BUDGET, "+
                "A.YTD_LCY, "+
                "A.YTD_LCY_ACTUAL, "+
                "A.YTD_LCY_BUDGET, "+
                "NVL(D.YTD_AED_ACTUAL,0) PREV_YR_CURR_MTH_YTD_AED, "+
                "NVL(D.YTD_LCY_ACTUAL,0) PREV_YR_CURR_MTH_YTD_LCY, "+
                "B.MTD_AED_ACTUAL AS PREV_MTH1_MTD_AED, "+
                "B.MTD_LCY_ACTUAL AS PREV_MTH1_MTD_LCY, "+
                "C.MTD_AED_ACTUAL AS PREV_MTH2_MTD_AED, "+
                "C.MTD_LCY_ACTUAL AS PREV_MTH2_MTD_LCY "+
               "FROM FIN_FCT_OSX_GL_AGG_CM A "+
               "LEFT OUTER JOIN FIN_FCT_OSX_GL_AGG_LM1 B "+
               "ON  A.LEGAL_ENTITY          = B.LEGAL_ENTITY "+
               "AND A.GL_ACCOUNT_ID         = B.GL_ACCOUNT_ID "+
               "AND A.PROFIT_CENTRE         = B.PROFIT_CENTRE "+
               "AND A.FINAL_SEGMENT         = B.FINAL_SEGMENT "+
               "AND A.CURRENCY_CODE         = B.CURRENCY_CODE "+
               "AND A.CATEGORY_CODE         = B.CATEGORY_CODE "+
               "AND A.CUSTOMER_SEGMENT_CODE = B.CUSTOMER_SEGMENT_CODE "+
               "AND A.SOURCE_SYSTEM_ID      = B.SOURCE_SYSTEM_ID "+
               "AND A.DOMAIN_ID             = B.DOMAIN_ID "+
               "AND A.IS_INTERNAL_ACCOUNT   = B.IS_INTERNAL_ACCOUNT "+
               "AND A.BANKING_TYPE          = B.BANKING_TYPE "+
               "AND A.BANKING_TYPE_CUSTOMER = B.BANKING_TYPE_CUSTOMER "+
               "AND A.AMOUNT_CLASS          = B.AMOUNT_CLASS "+
               "LEFT OUTER JOIN FIN_FCT_OSX_GL_AGG_LM2 C "+
               "ON  A.LEGAL_ENTITY          = C.LEGAL_ENTITY "+
               "AND A.GL_ACCOUNT_ID         = C.GL_ACCOUNT_ID "+
               "AND A.PROFIT_CENTRE         = C.PROFIT_CENTRE "+
               "AND A.FINAL_SEGMENT         = C.FINAL_SEGMENT "+
               "AND A.CURRENCY_CODE         = C.CURRENCY_CODE "+
               "AND A.CATEGORY_CODE         = C.CATEGORY_CODE "+
               "AND A.CUSTOMER_SEGMENT_CODE = C.CUSTOMER_SEGMENT_CODE "+
               "AND A.SOURCE_SYSTEM_ID      = C.SOURCE_SYSTEM_ID "+
               "AND A.DOMAIN_ID             = C.DOMAIN_ID "+
               "AND A.IS_INTERNAL_ACCOUNT   = C.IS_INTERNAL_ACCOUNT "+
               "AND A.BANKING_TYPE          = C.BANKING_TYPE "+
               "AND A.BANKING_TYPE_CUSTOMER = C.BANKING_TYPE_CUSTOMER "+
               "AND A.AMOUNT_CLASS          = C.AMOUNT_CLASS "+
               "LEFT OUTER JOIN FIN_FCT_OSX_GL_AGG_LYSM D "+
               "ON  A.LEGAL_ENTITY          = D.LEGAL_ENTITY "+
               "AND A.GL_ACCOUNT_ID         = D.GL_ACCOUNT_ID "+
               "AND A.PROFIT_CENTRE         = D.PROFIT_CENTRE "+
               "AND A.FINAL_SEGMENT         = D.FINAL_SEGMENT "+
               "AND A.CURRENCY_CODE         = D.CURRENCY_CODE "+
               "AND A.CATEGORY_CODE         = D.CATEGORY_CODE "+
               "AND A.CUSTOMER_SEGMENT_CODE = D.CUSTOMER_SEGMENT_CODE "+
               "AND A.SOURCE_SYSTEM_ID      = D.SOURCE_SYSTEM_ID "+
               "AND A.DOMAIN_ID             = D.DOMAIN_ID "+
               "AND A.IS_INTERNAL_ACCOUNT   = D.IS_INTERNAL_ACCOUNT "+
               "AND A.BANKING_TYPE          = D.BANKING_TYPE "+
               "AND A.BANKING_TYPE_CUSTOMER = D.BANKING_TYPE_CUSTOMER "+
               "AND A.AMOUNT_CLASS          = D.AMOUNT_CLASS")
               
          val gl_agg_tot= load_total_lines.osx_seg_gl_tot_lines_load(sqlContext, gl_agg_base, dom, acc, pc, hdfs_gl_agg)              
            
          val gl_agg_base_f= patch_gl_agg.osx_seg_gl_patch(sqlContext, gl_agg_base, dom, acc, pc, le, cov_geo, prod, dept, cust_seg) 
             
            
          gl_agg_tot.registerTempTable("FIN_FCT_OSX_SEG_GL_AGG_TOT")
          gl_agg_base_f.registerTempTable("FIN_FCT_OSX_SEG_GL_AGG_BASE")
          
          println(process_sumx_agg.time+" "+ "OSX GL AGG - COMPLETE GL AGG DATAFRAME WITH TOTAL LINES AND PATCH SET CREATED")
          //pw.println(process_sumx_agg.time+" "+ "OSX GL AGG - COMPLETE GL AGG DATAFRAME WITH TOTAL LINES AND PATCH SET CREATED")
          
          if (overwrite_flag==0 && ("hadoop fs -test -d ".concat(hdfs_seg_gl_agg_r).!)==0)
          {
              
              println (process_sumx_agg.time+" "+  "OSX GL AGG - PROCESSING ADDITIONAL VERSION LOAD OF GL DATASET. APPLICABLE ONLY FOR MONTHLY LOAD SCENARIO" )
                        
              /* LOOKUP THE DATASET IN THE LAST_REFRESH_FLAG=Y */
              fs.listStatus(new Path(s"$hdfs_seg_gl_agg/time_key=$time_key/last_refresh_flag=Y"))
                .filter(_.isDir)
                .map(_.getPath)
                .foreach(x=> {
                 version_id+=x.toString.substring(x.toString.lastIndexOf("=")+1).toInt
              })
              
              /* CHECK IF THERE IS FILE AVAILABLE THEN MOVE THE FILE TO LAST_REFRESH_FLAG=N */
              println(process_sumx_agg.time+" "+"OSX GL AGG - MOVING THE LATEST VERSION FILE FROM LAST REFRESH FLAG Y TO N")
              if (version_id > 0) {
                 
                 val exist_dir = "hadoop fs -test -d ".concat(s"$hdfs_seg_gl_agg/time_key=$time_key/last_refresh_flag=N").!                     
                 if (exist_dir==1) {
                      "hadoop fs -mkdir ".concat(s"$hdfs_seg_gl_agg/time_key=$time_key/last_refresh_flag=N").!
                 }
                 
                 val move_last_dir = (s"hadoop fs -mv $hdfs_seg_gl_agg/time_key=$time_key/last_refresh_flag=Y/version_id=$version_id"+" "+
                                      s"$hdfs_seg_gl_agg/time_key=$time_key/last_refresh_flag=N/version_id=$version_id").!
                                      
                 if (move_last_dir==0){
                    println(process_sumx_agg.time+" "+"OSX GL AGG - FILE MOVED SUCCESSFULLY")
                 }
                 else {
                    println(process_sumx_agg.time+" "+"OSX GL AGG - FILE DID NOT MOVE SUCCESSFULLY. TERMINATING THE PROCESS")
                 }
                 
              }
              else {
                    println(process_sumx_agg.time+" "+"OSX GL AGG - THERE WERE NO FILE IN LAST REFRESH FLAG FOLDER PATH. THIS IS AN EXCEPTION. TERMINATING PROCESS")
              }
                        
              /* CREATING A TEMPORARY LOOKUP TABLE WITH LATEST VERSION ID FOR BELOW OPERATIONS */
              println(process_sumx_agg.time+" "+"OSX GL AGG - CREATING DUMMY TEMP VIEW WITH PREVIOUS VERSION ID FOR CURRENT VERSION LOAD")
              val last_version= sqlContext.sql (s"SELECT $version_id AS VERSION_ID")
              last_version.registerTempTable("LKP_LAST_VERSION")             
          
              val gl_agg_data= sqlContext.sql(
                  "SELECT "+
                  "A.TIME_KEY, "+
                  "(CAST(LKP.VERSION_ID AS INT) + 1) AS VERSION_ID, "+
                  "A.AMOUNT_CLASS, "+
                  "A.BANKING_TYPE, "+
                  "A.BANKING_TYPE_CUSTOMER, "+
                  "A.CATEGORY_CODE, "+
                  "A.CURRENCY_CODE, "+
                  "A.CUSTOMER_SEGMENT_CODE, "+
                  "A.DATASET, "+
                  "A.DOMAIN_ID, "+
                  "A.FINAL_SEGMENT, "+
                  "A.GL_ACCOUNT_ID, "+
                  "A.IS_INTERNAL_ACCOUNT, "+
                  "A.LEGAL_ENTITY, "+
                  "A.PROFIT_CENTRE, "+
                  "A.SOURCE_SYSTEM_ID, "+
                  "A.ASSET_COF, "+
                  "A.ASSET_COF_MTD_LCY, "+
                  "A.ASSET_COF_YTD_AED, "+
                  "A.ASSET_COF_YTD_LCY, "+
                  "A.AVG_BOOK_BAL, "+
                  "A.AVG_BOOK_BAL_MTD_LCY, "+
                  "A.AVG_BOOK_BAL_YTD_AED, "+
                  "A.AVG_BOOK_BAL_YTD_LCY, "+
                  "A.CY_ED_BUDGET_MTD_AED, "+
                  "A.CY_ED_BUDGET_MTD_LCY, "+
                  "A.CY_ED_BUDGET_YTD_AED, "+
                  "A.CY_ED_BUDGET_YTD_LCY, "+
                  "A.DERIVATIVES_INCOME, "+
                  "A.DERIVATIVES_INCOME_MTD_LCY, "+
                  "A.DERIVATIVES_INCOME_YTD_AED, "+
                  "A.DERIVATIVES_INCOME_YTD_LCY, "+
                  "A.FEE_INCOME, "+
                  "A.FEE_INCOME_MTD_LCY, "+
                  "A.FEE_INCOME_YTD_AED, "+
                  "A.FEE_INCOME_YTD_LCY, "+
                  "A.FX_INCOME, "+
                  "A.FX_INCOME_MTD_LCY, "+
                  "A.FX_INCOME_YTD_AED, "+
                  "A.FX_INCOME_YTD_LCY, "+
                  "A.GROSS_INTEREST_EXPENSE, "+
                  "A.GROSS_INTEREST_EXPENSE_MTD_LCY, "+
                  "A.GROSS_INTEREST_EXPENSE_YTD_AED, "+
                  "A.GROSS_INTEREST_EXPENSE_YTD_LCY, "+
                  "A.GROSS_INTEREST_INCOME, "+
                  "A.GROSS_INTEREST_INCOME_MTD_LCY, "+
                  "A.GROSS_INTEREST_INCOME_YTD_AED, "+
                  "A.GROSS_INTEREST_INCOME_YTD_LCY, "+
                  "A.INTERBRANCH_EXPENSE, "+
                  "A.INTERBRANCH_EXPENSE_MTD_LCY, "+
                  "A.INTERBRANCH_EXPENSE_YTD_AED, "+
                  "A.INTERBRANCH_EXPENSE_YTD_LCY, "+
                  "A.INTERBRANCH_INCOME, "+
                  "A.INTERBRANCH_INCOME_MTD_LCY, "+
                  "A.INTERBRANCH_INCOME_YTD_AED, "+
                  "A.INTERBRANCH_INCOME_YTD_LCY, "+
                  "A.LIABILITY_COF, "+
                  "A.LIABILITY_COF_MTD_LCY, "+
                  "A.LIABILITY_COF_YTD_AED, "+
                  "A.LIABILITY_COF_YTD_LCY, "+
                  "A.LP_CHARGE, "+
                  "A.LP_CHARGE_MTD_LCY, "+
                  "A.LP_CHARGE_YTD_AED, "+
                  "A.LP_CHARGE_YTD_LCY, "+
                  "A.LP_CREDIT, "+
                  "A.LP_CREDIT_MTD_LCY, "+
                  "A.LP_CREDIT_YTD_AED, "+
                  "A.LP_CREDIT_YTD_LCY, "+
                  "A.MTD_AED, "+
                  "A.MTD_AED_ACTUAL, "+
                  "A.MTD_AED_BUDGET, "+
                  "A.MTD_LCY, "+
                  "A.MTD_LCY_ACTUAL, "+
                  "A.MTD_LCY_BUDGET, "+
                  "A.NET_INT_MARGIN, "+
                  "A.NET_INT_MARGIN_MTD_LCY, "+
                  "A.NET_INT_MARGIN_YTD_AED, "+
                  "A.NET_INT_MARGIN_YTD_LCY, "+
                  "A.NET_INTEREST_INCOME, "+
                  "A.NET_INTEREST_INCOME_MTD_LCY, "+
                  "A.NET_INTEREST_INCOME_YTD_AED, "+
                  "A.NET_INTEREST_INCOME_YTD_LCY, "+
                  "A.NET_INVESTMENT_INCOME, "+
                  "A.NET_INVESTMENT_INCOME_MTD_LCY, "+
                  "A.NET_INVESTMENT_INCOME_YTD_AED, "+
                  "A.NET_INVESTMENT_INCOME_YTD_LCY, "+
                  "A.NET_PL, "+
                  "A.NET_PL_MTD_LCY, "+
                  "A.NET_PL_YTD_AED, "+
                  "A.NET_PL_YTD_LCY, "+
                  "A.OTHER_INCOME, "+
                  "A.OTHER_INCOME_MTD_LCY, "+
                  "A.OTHER_INCOME_YTD_AED, "+
                  "A.OTHER_INCOME_YTD_LCY, "+
                  "A.RWA_CREDIT_RISK, "+
                  "A.RWA_MARKET_RISK, "+
                  "A.RWA_OPERATIONAL_RISK, "+
                  "A.RWA_TOTAL, "+
                  "A.TOTAL_FTP_AMT_MTD_AED, "+
                  "A.TOTAL_FTP_AMT_MTD_LCY, "+
                  "A.TOTAL_FTP_AMT_YTD_AED, "+
                  "A.TOTAL_FTP_AMT_YTD_LCY, "+
                  "A.YTD_AED, "+
                  "A.YTD_AED_ACTUAL, "+
                  "A.YTD_AED_BUDGET, "+
                  "A.YTD_LCY, "+
                  "A.YTD_LCY_ACTUAL, "+
                  "A.YTD_LCY_BUDGET, "+
                  "A.PREV_YR_CURR_MTH_YTD_AED, "+
                  "A.PREV_YR_CURR_MTH_YTD_LCY, "+
                  "A.PREV_MTH1_MTD_AED, "+
                  "A.PREV_MTH1_MTD_LCY, "+
                  "A.PREV_MTH2_MTD_AED, "+
                  "A.PREV_MTH2_MTD_LCY, "+
                  "A.GM_STMT_DERIVATIVES, "+
                  "A.GM_OTHER, "+
                  "'Y' LAST_REFRESH_FLAG "+
                  "FROM ( "+
                  "SELECT * "+
                  "FROM FIN_FCT_OSX_SEG_GL_AGG_BASE "+
                  "UNION ALL "+
                  "SELECT * "+
                  "FROM FIN_FCT_OSX_SEG_GL_AGG_TOT) A "+
                  "CROSS JOIN LKP_LAST_VERSION LKP "
                 )
              
              println(process_sumx_agg.time+" "+ "OSX GL AGG - WRITING DATAFRAME TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_gl_agg)
              //pw.println(process_sumx_agg.time+" "+ "OSX GL AGG - WRITING DATAFRAME TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_gl_agg)
                            
              gl_agg_data.repartition(200)
                         .write
                         .partitionBy("time_key", "last_refresh_flag", "version_id", "domain_id")
                         .mode("append")
                         .format("parquet")
                         .option("compression","snappy")
                         .save(hdfs_gl_agg)
              
              println(process_sumx_agg.time+" "+ "OSX GL AGG - AGGREGATE LOAD COMPLETED")
             
          }
          else {
            
            if(("hadoop fs -test -d ".concat(hdfs_seg_gl_agg_r).!)==0)  
            {
                println (process_sumx_agg.time+" "+  "OSX CUST AGG - DELETING EXISTING DIRECTORY ")
                val remove_dir = "hadoop fs -rm -r ".concat(hdfs_seg_gl_agg_r).!
                print(remove_dir)
                if(remove_dir==0) {
                 
                  println (process_sumx_agg.time+" "+  "OSX GL AGG - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE GL AGG RELOAD ")
                  //pw.println ("OSX BASE - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE BASE AGG RELOAD. LOAD IN PROGRESS")
                }
                println (process_sumx_agg.time+" "+  "OSX GL AGG - PROCESSING VERSION OVERWRITE OF GL AGG DATASET. APPLICABLE ONLY FOR DAILY LOAD SCENARIO" )
             }
             else {
               
                println (process_sumx_agg.time+" "+  "OSX GL AGG - PROCESSING FIRST VERSION OF GL AGG DATASET. APPLICABLE FOR BOTH DAILY AND MONTHLY LOAD SCENARIO" )
             }
             
             val gl_agg_data= sqlContext.sql(
                  "SELECT "+
                  "A.TIME_KEY, "+
                  "CAST(1 AS INT) AS VERSION_ID, "+
                  "A.AMOUNT_CLASS, "+
                  "A.BANKING_TYPE, "+
                  "A.BANKING_TYPE_CUSTOMER, "+
                  "A.CATEGORY_CODE, "+
                  "A.CURRENCY_CODE, "+
                  "A.CUSTOMER_SEGMENT_CODE, "+
                  "A.DATASET, "+
                  "A.DOMAIN_ID, "+
                  "A.FINAL_SEGMENT, "+
                  "A.GL_ACCOUNT_ID, "+
                  "A.IS_INTERNAL_ACCOUNT, "+
                  "A.LEGAL_ENTITY, "+
                  "A.PROFIT_CENTRE, "+
                  "A.SOURCE_SYSTEM_ID, "+
                  "A.ASSET_COF, "+
                  "A.ASSET_COF_MTD_LCY, "+
                  "A.ASSET_COF_YTD_AED, "+
                  "A.ASSET_COF_YTD_LCY, "+
                  "A.AVG_BOOK_BAL, "+
                  "A.AVG_BOOK_BAL_MTD_LCY, "+
                  "A.AVG_BOOK_BAL_YTD_AED, "+
                  "A.AVG_BOOK_BAL_YTD_LCY, "+
                  "A.CY_ED_BUDGET_MTD_AED, "+
                  "A.CY_ED_BUDGET_MTD_LCY, "+
                  "A.CY_ED_BUDGET_YTD_AED, "+
                  "A.CY_ED_BUDGET_YTD_LCY, "+
                  "A.DERIVATIVES_INCOME, "+
                  "A.DERIVATIVES_INCOME_MTD_LCY, "+
                  "A.DERIVATIVES_INCOME_YTD_AED, "+
                  "A.DERIVATIVES_INCOME_YTD_LCY, "+
                  "A.FEE_INCOME, "+
                  "A.FEE_INCOME_MTD_LCY, "+
                  "A.FEE_INCOME_YTD_AED, "+
                  "A.FEE_INCOME_YTD_LCY, "+
                  "A.FX_INCOME, "+
                  "A.FX_INCOME_MTD_LCY, "+
                  "A.FX_INCOME_YTD_AED, "+
                  "A.FX_INCOME_YTD_LCY, "+
                  "A.GROSS_INTEREST_EXPENSE, "+
                  "A.GROSS_INTEREST_EXPENSE_MTD_LCY, "+
                  "A.GROSS_INTEREST_EXPENSE_YTD_AED, "+
                  "A.GROSS_INTEREST_EXPENSE_YTD_LCY, "+
                  "A.GROSS_INTEREST_INCOME, "+
                  "A.GROSS_INTEREST_INCOME_MTD_LCY, "+
                  "A.GROSS_INTEREST_INCOME_YTD_AED, "+
                  "A.GROSS_INTEREST_INCOME_YTD_LCY, "+
                  "A.INTERBRANCH_EXPENSE, "+
                  "A.INTERBRANCH_EXPENSE_MTD_LCY, "+
                  "A.INTERBRANCH_EXPENSE_YTD_AED, "+
                  "A.INTERBRANCH_EXPENSE_YTD_LCY, "+
                  "A.INTERBRANCH_INCOME, "+
                  "A.INTERBRANCH_INCOME_MTD_LCY, "+
                  "A.INTERBRANCH_INCOME_YTD_AED, "+
                  "A.INTERBRANCH_INCOME_YTD_LCY, "+
                  "A.LIABILITY_COF, "+
                  "A.LIABILITY_COF_MTD_LCY, "+
                  "A.LIABILITY_COF_YTD_AED, "+
                  "A.LIABILITY_COF_YTD_LCY, "+
                  "A.LP_CHARGE, "+
                  "A.LP_CHARGE_MTD_LCY, "+
                  "A.LP_CHARGE_YTD_AED, "+
                  "A.LP_CHARGE_YTD_LCY, "+
                  "A.LP_CREDIT, "+
                  "A.LP_CREDIT_MTD_LCY, "+
                  "A.LP_CREDIT_YTD_AED, "+
                  "A.LP_CREDIT_YTD_LCY, "+
                  "A.MTD_AED, "+
                  "A.MTD_AED_ACTUAL, "+
                  "A.MTD_AED_BUDGET, "+
                  "A.MTD_LCY, "+
                  "A.MTD_LCY_ACTUAL, "+
                  "A.MTD_LCY_BUDGET, "+
                  "A.NET_INT_MARGIN, "+
                  "A.NET_INT_MARGIN_MTD_LCY, "+
                  "A.NET_INT_MARGIN_YTD_AED, "+
                  "A.NET_INT_MARGIN_YTD_LCY, "+
                  "A.NET_INTEREST_INCOME, "+
                  "A.NET_INTEREST_INCOME_MTD_LCY, "+
                  "A.NET_INTEREST_INCOME_YTD_AED, "+
                  "A.NET_INTEREST_INCOME_YTD_LCY, "+
                  "A.NET_INVESTMENT_INCOME, "+
                  "A.NET_INVESTMENT_INCOME_MTD_LCY, "+
                  "A.NET_INVESTMENT_INCOME_YTD_AED, "+
                  "A.NET_INVESTMENT_INCOME_YTD_LCY, "+
                  "A.NET_PL, "+
                  "A.NET_PL_MTD_LCY, "+
                  "A.NET_PL_YTD_AED, "+
                  "A.NET_PL_YTD_LCY, "+
                  "A.OTHER_INCOME, "+
                  "A.OTHER_INCOME_MTD_LCY, "+
                  "A.OTHER_INCOME_YTD_AED, "+
                  "A.OTHER_INCOME_YTD_LCY, "+
                  "A.RWA_CREDIT_RISK, "+
                  "A.RWA_MARKET_RISK, "+
                  "A.RWA_OPERATIONAL_RISK, "+
                  "A.RWA_TOTAL, "+
                  "A.TOTAL_FTP_AMT_MTD_AED, "+
                  "A.TOTAL_FTP_AMT_MTD_LCY, "+
                  "A.TOTAL_FTP_AMT_YTD_AED, "+
                  "A.TOTAL_FTP_AMT_YTD_LCY, "+
                  "A.YTD_AED, "+
                  "A.YTD_AED_ACTUAL, "+
                  "A.YTD_AED_BUDGET, "+
                  "A.YTD_LCY, "+
                  "A.YTD_LCY_ACTUAL, "+
                  "A.YTD_LCY_BUDGET, "+
                  "A.PREV_YR_CURR_MTH_YTD_AED, "+
                  "A.PREV_YR_CURR_MTH_YTD_LCY, "+
                  "A.PREV_MTH1_MTD_AED, "+
                  "A.PREV_MTH1_MTD_LCY, "+
                  "A.PREV_MTH2_MTD_AED, "+
                  "A.PREV_MTH2_MTD_LCY, "+
                  "A.GM_STMT_DERIVATIVES, "+
                  "A.GM_OTHER, "+
                  "'Y' LAST_REFRESH_FLAG "+
                  "FROM ( "+
                  "SELECT * "+
                  "FROM FIN_FCT_OSX_SEG_GL_AGG_BASE "+
                  "UNION ALL "+
                  "SELECT * "+
                  "FROM FIN_FCT_OSX_SEG_GL_AGG_TOT) A "
              )
              
              println(process_sumx_agg.time+" "+ "OSX GL AGG - WRITING DATAFRAME TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_gl_agg)
              //pw.println(process_sumx_agg.time+" "+ "OSX GL AGG - WRITING DATAFRAME TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_gl_agg)
                            
              gl_agg_data.repartition(200)
                         .write
                         .partitionBy("time_key", "last_refresh_flag", "version_id", "domain_id")
                         .mode("append")
                         .format("parquet")
                         .option("compression","snappy")
                         .save(hdfs_gl_agg)
              
              println(process_sumx_agg.time+" "+ "OSX GL AGG - AGGREGATE LOAD COMPLETED")
            
          }
  
               
      
    }
  
}
