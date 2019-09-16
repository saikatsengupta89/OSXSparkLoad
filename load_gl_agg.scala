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

import sys.process._

object load_gl_agg {
  
    def today():String= {
       return new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date())
    }
    
    def osx_seg_gl_agg_load (sqlContext:SQLContext, 
                             time_key:String,
                             sumx_data:DataFrame, 
                             sumx_data_lm1:DataFrame,
                             sumx_data_lm2:DataFrame,
                             dom:DataFrame, 
                             acc:DataFrame,
                             pc:DataFrame,
                             le:DataFrame,
                             cov_geo:DataFrame,
                             prod:DataFrame,
                             dept:DataFrame, 
                             cust_seg:DataFrame,
                             pw:PrintWriter, 
                             hdfs_gl_agg:String) {
      
      
        /**************************** CREATING GL LEVEL AGGREGATE FROM BASE LEVEL *********************************/
      
        pw.println(today()+ " OSX GL - AGGREGATE LOAD INITIATED")
      
        val hdfs_seg_gl_agg_r= hdfs_gl_agg+"/time_key="+time_key
       
       //DIRECTORY EXISTS ALREADY. REMOVE THE EXISTING DIRECTORY AND THEN APPEND DATA FOR THE TIMEKEY
        if(("hadoop fs -test -d ".concat(hdfs_seg_gl_agg_r).!)==0)  
        {
           println ("Delete existing directory")
           val remove_dir = "hadoop fs -rm -r ".concat(hdfs_seg_gl_agg_r).!
           print(remove_dir)
           if(remove_dir==0) {
             pw.println(" OSX GL - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BRFORE GL AGGREGATE RELOAD. LOAD IN PROGRESS")
           }
        }
        
        pw.println(today()+" OSX GL - DIMENSION TABLES INITIATED FOR LOOKUP")
       
        sumx_data.registerTempTable("OSX_DEAL_DATA")
        dom.registerTempTable("OSX_DOM")
        acc.registerTempTable("OSX_ACC_FPA")
        sumx_data_lm1.registerTempTable("OSX_DEAL_DATA_LM1")
        sumx_data_lm2.registerTempTable("OSX_DEAL_DATA_LM2")
        
        
        pw.println(today()+ " OSX GL - GL AGGREGATE CURRENT MONTH DATAFRAME CREATED")
        
        
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
            "CASE WHEN DOM.LVL1_CODE = '400000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END CY_ED_BUDGET_MTD_AED, "+
            "CASE WHEN DOM.LVL1_CODE = '400000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END CY_ED_BUDGET_MTD_LCY, "+
            "CASE WHEN DOM.LVL1_CODE = '400000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END CY_ED_BUDGET_YTD_AED, "+
            "CASE WHEN DOM.LVL1_CODE = '400000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END CY_ED_BUDGET_YTD_LCY, "+
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
            "CASE WHEN DOM.LVL1_CODE = '100000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END MTD_AED_ACTUAL, "+
            "CASE WHEN DOM.LVL1_CODE = '200000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END MTD_AED_BUDGET, "+
            "MTD_LCY, "+
            "CASE WHEN DOM.LVL1_CODE = '100000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END MTD_LCY_ACTUAL, "+
            "CASE WHEN DOM.LVL1_CODE = '200000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END MTD_LCY_BUDGET, "+
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
            "CASE WHEN ((DOM.LVL4_CODE = '511100') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '913716')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_CREDIT_RISK, "+
            "CASE WHEN ((DOM.LVL4_CODE = '511200') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '813714')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_MARKET_RISK, "+
            "CASE WHEN ((DOM.LVL4_CODE = '511300') OR (DOM.DOMAIN_ID IN ('50','51','52') AND ACC.GL_ACCOUNT_ID = '813715')) THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END RWA_OPERATIONAL_RISK, "+
            "TOTAL_FTP_AMT_MTD_AED, "+
            "TOTAL_FTP_AMT_MTD_LCY, "+
            "TOTAL_FTP_AMT_YTD_AED, "+
            "TOTAL_FTP_AMT_YTD_LCY, "+
            "NVL(OUTSTANDING_BALANCE_YTD_AED, 0 ) YTD_AED, "+
            "CASE WHEN DOM.LVL1_CODE = '100000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END YTD_AED_ACTUAL, "+
            "CASE WHEN DOM.LVL1_CODE = '200000' THEN OUTSTANDING_BALANCE_YTD_AED ELSE 0 END YTD_AED_BUDGET, "+
            "NVL(OUTSTANDING_BALANCE_YTD_LCY, 0 ) YTD_LCY, "+
            "CASE WHEN DOM.LVL1_CODE = '100000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END YTD_LCY_ACTUAL, "+
            "CASE WHEN DOM.LVL1_CODE = '200000' THEN OUTSTANDING_BALANCE_YTD_LCY ELSE 0 END YTD_LCY_BUDGET "+
            "FROM "+
            "OSX_DEAL_DATA AGG "+ 
            "LEFT OUTER JOIN OSX_DOM DOM ON (AGG.DOMAIN_ID = DOM.DOMAIN_ID) "+
            "LEFT OUTER JOIN OSX_ACC_FPA ACC ON (AGG.GL_ACCOUNT_ID = ACC.GL_ACCOUNT_ID) "+
            ")Q1 "+
            ") "+
            "GROUP BY AMOUNT_CLASS, BANKING_TYPE, BANKING_TYPE_CUSTOMER, CATEGORY_CODE, CURRENCY_CODE, CUSTOMER_SEGMENT_CODE, "+ 
            "DATASET, DOMAIN_ID, FINAL_SEGMENT, GL_ACCOUNT_ID, IS_INTERNAL_ACCOUNT, LEGAL_ENTITY, PROFIT_CENTRE, "+ 
            "SOURCE_SYSTEM_ID, TIME_KEY"
         )
          
         
         pw.println(today()+ " OSX GL - GL AGGREGATE DATAFRAME FOR CURRENT-1 MONTH CREATED")
         
         //(CURR MONTH -1) GL AGGREGATE CREATION BASED ON REQUIRED FIELDS TO MERGE LM1 MTD
         val gl_agg_lm1= sqlContext.sql (
              "SELECT "+
              "A.LEGAL_ENTITY, "+
              "A.GL_ACCOUNT_ID, "+
              "A.PROFIT_CENTRE, "+
              "A.FINAL_SEGMENT, "+
              "A.CURRENCY_CODE, "+
              "A.CATEGORY_CODE, "+
              "A.CUSTOMER_SEGMENT_CODE, "+
              "A.SOURCE_SYSTEM_ID, "+
              "A.DOMAIN_ID, "+
              "A.IS_INTERNAL_ACCOUNT, "+
              "A.BANKING_TYPE, "+
              "A.BANKING_TYPE_CUSTOMER, "+
              "SUM(CASE WHEN B.LVL1_CODE = '100000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END) MTD_AED_ACTUAL, "+
              "SUM(CASE WHEN B.LVL1_CODE = '100000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END) MTD_LCY_ACTUAL "+
              "FROM OSX_DEAL_DATA_LM1 A "+
              "INNER JOIN OSX_DOM B ON A.DOMAIN_ID = B.DOMAIN_ID "+
              "GROUP BY A.LEGAL_ENTITY,A.GL_ACCOUNT_ID,A.PROFIT_CENTRE,A.FINAL_SEGMENT,A.CURRENCY_CODE,A.CATEGORY_CODE, "+
              "A.CUSTOMER_SEGMENT_CODE,A.SOURCE_SYSTEM_ID,A.DOMAIN_ID,A.IS_INTERNAL_ACCOUNT,A.BANKING_TYPE, "+
              "A.BANKING_TYPE_CUSTOMER")
              
         
         pw.println(today()+ " OSX GL - GL AGGREGATE DATAFRAME FOR CURRENT-2 MONTHS CREATED")
              
         //(CURR MONTH -2) GL AGGREGATE CREATION BASED ON REQUIRED FIELDS TO MERGE LM2 MTD
         val gl_agg_lm2= sqlContext.sql (
              "SELECT "+
              "A.LEGAL_ENTITY, "+
              "A.GL_ACCOUNT_ID, "+
              "A.PROFIT_CENTRE, "+
              "A.FINAL_SEGMENT, "+
              "A.CURRENCY_CODE, "+
              "A.CATEGORY_CODE, "+
              "A.CUSTOMER_SEGMENT_CODE, "+
              "A.SOURCE_SYSTEM_ID, "+
              "A.DOMAIN_ID, "+
              "A.IS_INTERNAL_ACCOUNT, "+
              "A.BANKING_TYPE, "+
              "A.BANKING_TYPE_CUSTOMER, "+
              "SUM(CASE WHEN B.LVL1_CODE = '100000' THEN OUTSTANDING_BALANCE_MTD_AED ELSE 0 END) MTD_AED_ACTUAL, "+
              "SUM(CASE WHEN B.LVL1_CODE = '100000' THEN OUTSTANDING_BALANCE_MTD_LCY ELSE 0 END) MTD_LCY_ACTUAL "+
              "FROM OSX_DEAL_DATA_LM2 A "+
              "INNER JOIN OSX_DOM B ON A.DOMAIN_ID = B.DOMAIN_ID "+
              "GROUP BY A.LEGAL_ENTITY,A.GL_ACCOUNT_ID,A.PROFIT_CENTRE,A.FINAL_SEGMENT,A.CURRENCY_CODE,A.CATEGORY_CODE, "+
              "A.CUSTOMER_SEGMENT_CODE,A.SOURCE_SYSTEM_ID,A.DOMAIN_ID,A.IS_INTERNAL_ACCOUNT,A.BANKING_TYPE, "+
              "A.BANKING_TYPE_CUSTOMER")
              
          
          gl_agg_cm.registerTempTable("GL_AGG_CM")
          gl_agg_lm1.registerTempTable("GL_AGG_LM1")
          gl_agg_lm2.registerTempTable("GL_AGG_LM2")
          
          pw.println(today()+ " OSX GL - GL AGGREGATE DATAFRAME CREATED WITH RELEVANT FIELDS FROM LAST TWO MONTHS")
          
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
                "CAST(0 AS DOUBLE) PREV_YR_CURR_MTH_YTD_AED, "+
                "CAST(0 AS DOUBLE) PREV_YR_CURR_MTH_YTD_LCY, "+
                "B.MTD_AED_ACTUAL AS PREV_MTH1_MTD_AED, "+
                "B.MTD_LCY_ACTUAL AS PREV_MTH1_MTD_LCY, "+
                "C.MTD_AED_ACTUAL AS PREV_MTH2_MTD_AED, "+
                "C.MTD_LCY_ACTUAL AS PREV_MTH2_MTD_LCY "+
               "FROM GL_AGG_CM A "+
               "LEFT OUTER JOIN GL_AGG_LM1 B "+
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
               "LEFT OUTER JOIN GL_AGG_LM2 C "+
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
               "AND A.BANKING_TYPE_CUSTOMER = C.BANKING_TYPE_CUSTOMER")
               
              
              val gl_agg_tot= load_total_lines.osx_seg_gl_tot_lines_load(sqlContext, gl_agg_base, dom, acc, pc, pw, hdfs_gl_agg)              
              
              val gl_agg_base_f= patch_gl_agg.osx_seg_gl_patch(sqlContext, gl_agg_base, dom, acc, pc, le, cov_geo, prod, dept, pw, cust_seg) 
               
              
              gl_agg_tot.registerTempTable("OSX_SEG_GL_AGG_TOT")
              gl_agg_base_f.registerTempTable("OSX_SEG_GL_AGG_BASE")
              
              pw.println(today()+ " OSX GL - COMPLETE GL AGG DATAFRAME WITH TOTAL LINES AND PATCH SET CREATED")
              
              val gl_agg_data= sqlContext.sql(
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
                  "A.PREV_YR_CURR_MTH_YTD_AED, "+
                  "A.PREV_YR_CURR_MTH_YTD_LCY, "+
                  "A.PREV_MTH1_MTD_AED, "+
                  "A.PREV_MTH1_MTD_LCY, "+
                  "A.PREV_MTH2_MTD_AED, "+
                  "A.PREV_MTH2_MTD_LCY, "+
                  "A.GM_STMT_DERIVATIVES, "+
                  "A.GM_OTHER "+
                  "FROM ( "+
                  "SELECT * "+
                  "FROM OSX_SEG_GL_AGG_BASE "+
                  "UNION ALL "+
                  "SELECT * "+
                  "FROM OSX_SEG_GL_AGG_TOT) A"
                 )
              
              pw.println(today()+ " OSX GL - WRITING DATAFRAME TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :", hdfs_gl_agg)
                            
              gl_agg_data.repartition(200)
                         .write
                         .partitionBy("time_key")
                         .mode("append")
                         .format("parquet")
                         .option("compression","snappy")
                         .save(hdfs_gl_agg)
                                 
              pw.println(today()+ " OSX GL - AGGREGATE LOAD COMPLETED")
              
      
    }
  
}