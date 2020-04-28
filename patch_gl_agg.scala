package sumx_aggregate

import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED

/* for writing logs w.r.t. data load process */
import java.io._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.spark.sql.SQLContext

object patch_gl_agg {
  
   def today():String= {
        return new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date())
    }
   
   def osx_seg_gl_patch (sqlContext:SQLContext,
                         gl_agg_base:DataFrame,
                         dom:DataFrame, 
                         acc:DataFrame, 
                         pc:DataFrame, 
                         le:DataFrame, 
                         cov_geo:DataFrame, 
                         prod:DataFrame, 
                         dept:DataFrame, 
                         cust_seg:DataFrame): DataFrame= {
      
      //pw.println(process_sumx_agg.time+" "+"OSX GL AGG- PATCH SET INITIATED TO TAG GM STMT AND GM OTHERS INFORMATION") 
     
      gl_agg_base.registerTempTable("OSX_SEG_GL_AGG")
      dom.registerTempTable("OSX_DOM")
      acc.registerTempTable("OSX_ACC_FPA")
      pc.registerTempTable("OSX_PC")
      le.registerTempTable("OSX_LE")
      cov_geo.registerTempTable("OSX_COV_GEO")
      prod.registerTempTable("OSX_PROD")
      dept.registerTempTable("OSX_DEPT")
      cust_seg.registerTempTable("OSX_CUST_SEG")
            
      println(process_sumx_agg.time+" "+"OSX GL AGG- PATCH SET DATAFRAME CREATED")
      //pw.println(process_sumx_agg.time+" "+"OSX GL AGG- PATCH SET DATAFRAME CREATED")
      
      val patch_dataset= sqlContext.sql (
          "SELECT  "+
          "GL_RPT.AMOUNT_CLASS, "+
          "GL_RPT.BANKING_TYPE, "+
          "GL_RPT.BANKING_TYPE_CUSTOMER, "+
          "GL_RPT.CATEGORY_CODE, "+
          "GL_RPT.CURRENCY_CODE, "+
          "GL_RPT.CUSTOMER_SEGMENT_CODE, "+
          "GL_RPT.DOMAIN_ID, "+
          "GL_RPT.FINAL_SEGMENT, "+
          "GL_RPT.GL_ACCOUNT_ID, "+
          "GL_RPT.LEGAL_ENTITY, "+
          "GL_RPT.PROFIT_CENTRE, "+
          "GL_RPT.SOURCE_SYSTEM_ID, "+
          "GL_RPT.TIME_KEY, "+
          "CASE WHEN DEPT.L4_CODE = '103070' AND GL_FPA.GL_ACCOUNT_LEVL3_CODE = '60260000'  "+
          "AND PROD.PRODUCT_LEVL2_CODE IN ('00050','00055','00060','00070','00075','00080','00085','00090','00095','00130', "+
          "'01000','02000','20000','21000','30000','35000','36000','37000')  "+
          "THEN CASE WHEN GL_FPA.GL_ACCOUNT_LEVL6_CODE IN ('642507','728717','728718','728719','728720','728721','728722','728723', "+
          "'728724','728725','728726','728727','728728','728729','728730','728731','728732','728733','728734') THEN 'CMC' "+
          "WHEN GL_FPA.GL_ACCOUNT_LEVL6_CODE = '642508' THEN 'PC Sales'  "+
          "WHEN GL_FPA.GL_ACCOUNT_LEVL6_CODE = '642509' THEN 'PC Non Sales'  "+
          "WHEN GL_FPA.GL_ACCOUNT_LEVL6_CODE IN ('728695','728742') THEN 'GM Sales Others'  "+
          "ELSE ' GM Sales Others'  "+
          "END   "+
          "WHEN DEPT.L4_CODE = '103070' AND GL_FPA.GL_ACCOUNT_LEVL3_CODE = '60260000'  "+
          "AND NOT PROD.PRODUCT_LEVL2_CODE IN ('00050','00055','00060','00070','00075','00080','00085','00090','00095','00130','01000', "+
          "'02000','20000','21000','30000','35000','36000','37000') THEN 'Others'  "+
          "WHEN GL_RPT.LEGAL_ENTITY <> '620' AND GL_FPA.GL_ACCOUNT_LEVL2_CODE = '60100000'   "+
          "AND DEPT.L3_CODE = '100030' AND DOM.L2_CODE = '120000'   "+
          "AND C_SEG.L1_CODE IN ('5000','7000') AND NOT GL_FPA.GL_ACCOUNT_LEVL6_CODE IN ('619021','713071','713072')  "+
          "AND PROD.PRODUCT_CODE IN ('21020','21024','21029','9591','9593','9611') AND NOT GL_RPT.SOURCE_SYSTEM_ID IN ('AEI','AEIFMD') THEN 'NIM'  "+
          "ELSE 'GM Others'  "+
          "END GM_OTHER,     "+
          "CASE WHEN DEPT.L4_CODE = '103070' AND GL_FPA.GL_ACCOUNT_LEVL3_CODE = '60260000'  "+
          "THEN CASE WHEN PROD.PRODUCT_LEVL2_CODE = '00075' THEN 'FX'  "+
          "WHEN PROD.PRODUCT_LEVL2_CODE IN ('00055','01000','02000','20000','21000','30000','35000','36000','37000') THEN 'MM'  "+
          "WHEN PROD.PRODUCT_LEVL2_CODE IN ('00050','00085','00095','00130') THEN 'Securities'  "+
          "WHEN PROD.PRODUCT_LEVL2_CODE IN ('00060','00070','00080','00090') THEN 'Derivatives'  "+
          "ELSE 'Derivatives'  "+
          "END   "+
          "WHEN GL_RPT.LEGAL_ENTITY <> '620' AND GL_FPA.GL_ACCOUNT_LEVL2_CODE = '60100000'  "+
          "AND DEPT.L3_CODE = '100030' AND DOM.L2_CODE = '120000'  "+
          "AND C_SEG.L1_CODE IN ('5000','7000') AND NOT GL_FPA.GL_ACCOUNT_LEVL6_CODE IN ('619021','713071','713072')  "+
          "AND PROD.PRODUCT_CODE IN ('21020','21024','21029','9591','9593','9611') AND NOT GL_RPT.SOURCE_SYSTEM_ID IN ('AEI','AEIFMD') THEN 'MM'  "+
          "ELSE 'GM Others'  "+
          "END AS GM_STMT_DERIVATIVES "+
          "FROM  "+
          "OSX_SEG_GL_AGG GL_RPT "+
          "INNER JOIN OSX_COV_GEO CV_GEO ON GL_RPT.CUSTOMER_SEGMENT_CODE = CV_GEO.COVERAGE_GEOGRAPHY "+
          "INNER JOIN OSX_DOM DOM ON DOM.DOMAIN_ID = GL_RPT.DOMAIN_ID "+
          "INNER JOIN OSX_PROD PROD ON PROD.PRODUCT_CODE = GL_RPT.CATEGORY_CODE "+
          "INNER JOIN OSX_LE LE ON LE.LEGAL_ENTITY = GL_RPT.LEGAL_ENTITY "+
          "INNER JOIN OSX_ACC_FPA GL_FPA ON GL_FPA.GL_ACCOUNT_ID = GL_RPT.GL_ACCOUNT_ID "+
          "INNER JOIN OSX_DEPT DEPT ON DEPT.DEPARTMENT_ID = GL_RPT.FINAL_SEGMENT "+
          "INNER JOIN OSX_CUST_SEG C_SEG ON C_SEG.CUSTOMER_SEGMENT_CODE = GL_RPT.CUSTOMER_SEGMENT_CODE "+
          "WHERE "+
          "(DEPT.L1_CODE = '100000'  "+
          "AND DEPT.L2_CODE = '100003'  "+
          "AND GL_FPA.GL_ACCOUNT_LEVL1_CODE = '60000000'  "+
          "AND GL_RPT.CUSTOMER_SEGMENT_CODE = CV_GEO.COVERAGE_GEOGRAPHY  "+
          "AND (DOM.L2_CODE = '120000' OR GL_RPT.DOMAIN_ID IN ('26','37','7'))  "+
          "AND (CASE C_SEG.L2_CODE WHEN '5005' THEN 'IBG'  "+
          "WHEN '5010' THEN 'CBG'  "+
          "WHEN '5015' THEN 'PCG'  "+
          "WHEN '5020' THEN 'CMB'  "+
          "WHEN '5030' THEN 'MNC'  "+
          "WHEN '5025' THEN 'FIG'  "+
          "ELSE 'OTHERS'  "+
          "END IN ('CBG','CMB','FIG','IBG','MNC','OTHERS','PCG'))  "+
          "AND (CASE WHEN CV_GEO.L2_CODE = '10105' THEN 'AD'  "+
          "WHEN CV_GEO.L2_CODE = '10107' THEN 'DXB'  "+
          "WHEN LE.LEGAL_ENTITY<> '100' AND CV_GEO.L2_CODE = '20100' THEN 'APACINTL'  "+
          "WHEN LE.LEGAL_ENTITY = '100' AND CV_GEO.L2_CODE = '20100' THEN 'APACUAE'  "+
          "WHEN LE.LEGAL_ENTITY<> '100' AND CV_GEO.L2_CODE = '20300' THEN 'EAMEAINTL'  "+
          "WHEN LE.LEGAL_ENTITY = '100' AND CV_GEO.L2_CODE = '20300' THEN 'EAMEAUAE'  "+
          "END  "+
          "IN ('AD','APACINTL','APACUAE','DXB','EAMEAINTL','EAMEAUAE'))  "+
          "AND C_SEG.L1_CODE IN ('5000','0009','0001','6000','UnClass') "+
          "AND GL_RPT.GL_ACCOUNT_ID NOT LIKE '-%')  "+
          "GROUP BY  "+
          "GL_RPT.AMOUNT_CLASS, "+
          "GL_RPT.BANKING_TYPE, "+
          "GL_RPT.BANKING_TYPE_CUSTOMER, "+
          "GL_RPT.CATEGORY_CODE, "+
          "GL_RPT.CURRENCY_CODE, "+
          "GL_RPT.CUSTOMER_SEGMENT_CODE, "+
          "GL_RPT.DOMAIN_ID, "+
          "GL_RPT.FINAL_SEGMENT, "+
          "GL_RPT.GL_ACCOUNT_ID, "+
          "GL_RPT.LEGAL_ENTITY, "+
          "GL_RPT.PROFIT_CENTRE, "+
          "GL_RPT.SOURCE_SYSTEM_ID, "+
          "GL_RPT.TIME_KEY, "+
          "CASE WHEN DEPT.L4_CODE = '103070' AND GL_FPA.GL_ACCOUNT_LEVL3_CODE = '60260000' AND PROD.PRODUCT_LEVL2_CODE IN ('00050','00055', "+
          "'00060','00070','00075','00080','00085','00090','00095','00130','01000','02000','20000','21000','30000','35000','36000','37000')  "+
          "THEN CASE WHEN GL_FPA.GL_ACCOUNT_LEVL6_CODE IN ('642507','728717','728718','728719','728720','728721','728722','728723','728724', "+
          "'728725','728726','728727','728728','728729','728730','728731','728732','728733','728734') THEN 'CMC'  "+
          "WHEN GL_FPA.GL_ACCOUNT_LEVL6_CODE = '642508' THEN 'PC Sales'  "+
          "WHEN GL_FPA.GL_ACCOUNT_LEVL6_CODE = '642509' THEN 'PC Non Sales'  "+
          "WHEN GL_FPA.GL_ACCOUNT_LEVL6_CODE IN ('728695','728742') THEN 'GM Sales Others'  "+
          "ELSE ' GM Sales Others'  "+
          "END   "+
          "WHEN DEPT.L4_CODE = '103070' AND GL_FPA.GL_ACCOUNT_LEVL3_CODE = '60260000'  "+
          "AND NOT PROD.PRODUCT_LEVL2_CODE IN ('00050','00055','00060','00070','00075','00080','00085','00090','00095','00130','01000','02000', "+
          "'20000','21000','30000','35000','36000','37000') THEN 'Others'  "+
          "WHEN GL_RPT.LEGAL_ENTITY <> '620'  "+
          "AND GL_FPA.GL_ACCOUNT_LEVL2_CODE = '60100000'  "+
          "AND DEPT.L3_CODE = '100030' AND DOM.L2_CODE = '120000'  "+
          "AND C_SEG.L1_CODE IN ('5000','7000') AND NOT GL_FPA.GL_ACCOUNT_LEVL6_CODE IN ('619021','713071','713072')  "+
          "AND PROD.PRODUCT_CODE IN ('21020','21024','21029','9591','9593','9611') AND NOT GL_RPT.SOURCE_SYSTEM_ID IN ('AEI','AEIFMD') THEN 'NIM'  "+
          "ELSE 'GM Others'  "+
          "END, "+
          "CASE WHEN DEPT.L4_CODE = '103070' AND GL_FPA.GL_ACCOUNT_LEVL3_CODE = '60260000'  "+
          "THEN CASE WHEN PROD.PRODUCT_LEVL2_CODE = '00075' THEN 'FX'  "+
          "WHEN PROD.PRODUCT_LEVL2_CODE IN ('00055','01000','02000','20000','21000','30000','35000','36000','37000') THEN 'MM'  "+
          "WHEN PROD.PRODUCT_LEVL2_CODE IN ('00050','00085','00095','00130') THEN 'Securities'  "+
          "WHEN PROD.PRODUCT_LEVL2_CODE IN ('00060','00070','00080','00090') THEN 'Derivatives'  "+
          "ELSE 'Derivatives'  "+
          "END   "+
          "WHEN GL_RPT.LEGAL_ENTITY <> '620' AND GL_FPA.GL_ACCOUNT_LEVL2_CODE = '60100000'  "+
          "AND DEPT.L3_CODE = '100030' AND DOM.L2_CODE = '120000'  "+
          "AND C_SEG.L1_CODE IN ('5000','7000') AND NOT GL_FPA.GL_ACCOUNT_LEVL6_CODE IN ('619021','713071','713072')  "+
          "AND PROD.PRODUCT_CODE IN ('21020','21024','21029','9591','9593','9611') AND NOT GL_RPT.SOURCE_SYSTEM_ID IN ('AEI','AEIFMD') THEN 'MM'  "+
          "ELSE 'GM Others'  "+
          "END ")
      
      patch_dataset.registerTempTable("OSX_SEG_GL_AGG_PATCH_SET")
      
      println(process_sumx_agg.time+" "+"OSX GL AGG - FINAL PATCH SET DATAFRAME CREATED")
      //pw.println(process_sumx_agg.time+" "+"OSX GL AGG - FINAL PATCH SET DATAFRAME CREATED")
          
      val gl_agg_base_f= sqlContext.sql (
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
          "B.GM_STMT_DERIVATIVES, "+
          "B.GM_OTHER "+
          "FROM OSX_SEG_GL_AGG A "+
          "LEFT OUTER JOIN OSX_SEG_GL_AGG_PATCH_SET B "+
          "ON A.AMOUNT_CLASS= B.AMOUNT_CLASS "+
          "AND A.BANKING_TYPE= B.BANKING_TYPE "+
          "AND A.BANKING_TYPE_CUSTOMER= B.BANKING_TYPE_CUSTOMER "+
          "AND A.CATEGORY_CODE= B.CATEGORY_CODE "+
          "AND A.CURRENCY_CODE= B.CURRENCY_CODE "+
          "AND A.CUSTOMER_SEGMENT_CODE= B.CUSTOMER_SEGMENT_CODE "+
          "AND A.DOMAIN_ID= B.DOMAIN_ID "+
          "AND A.FINAL_SEGMENT= B.FINAL_SEGMENT "+
          "AND A.GL_ACCOUNT_ID= B.GL_ACCOUNT_ID "+
          "AND A.LEGAL_ENTITY= B.LEGAL_ENTITY "+
          "AND A.PROFIT_CENTRE= B.PROFIT_CENTRE "+
          "AND A.SOURCE_SYSTEM_ID= B.SOURCE_SYSTEM_ID ")
          
          return gl_agg_base_f
   }
  
}
