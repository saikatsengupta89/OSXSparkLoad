package sumx_aggregate
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

object process_budget_dummyDataSet {
  
    def get_dummy_set (time_key:Integer, sqlContext: SQLContext) : DataFrame = {
               
         val dummy_dataset= sqlContext.sql (
            "SELECT "+ 
            "'NA' BANKING_TYPE, "+
            "'NA' BANKING_TYPE_CUSTOMER, "+
            "'-1' CATEGORY_CODE, "+
            "'-1' CURRENCY_CODE, "+
            "'-1' CUSTOMER_SEGMENT_CODE, "+
            "'DUMMY' DATASET, "+
            "'-1' FINAL_SEGMENT, "+
            "'-1' GL_ACCOUNT_ID, "+
            "'-1' IS_INTERNAL_ACCOUNT, "+
            "'-1' LEGAL_ENTITY, "+
            "'-1' PROFIT_CENTRE, "+
            "'NA' SOURCE_SYSTEM_ID, "+
            "CAST(0 AS DOUBLE) ASSET_COF, "+
            "CAST(0 AS DOUBLE) ASSET_COF_MTD_LCY, "+
            "CAST(0 AS DOUBLE) ASSET_COF_YTD_AED, "+
            "CAST(0 AS DOUBLE) ASSET_COF_YTD_LCY, "+
            "CAST(0 AS DOUBLE) AVG_BOOK_BAL, "+
            "CAST(0 AS DOUBLE) AVG_BOOK_BAL_MTD_LCY, "+
            "CAST(0 AS DOUBLE) AVG_BOOK_BAL_YTD_AED, "+
            "CAST(0 AS DOUBLE) AVG_BOOK_BAL_YTD_LCY , "+
            "CAST(0 AS DOUBLE) CY_ED_BUDGET_MTD_AED, "+
            "CAST(0 AS DOUBLE) CY_ED_BUDGET_MTD_LCY, "+
            "CAST(0 AS DOUBLE) CY_ED_BUDGET_YTD_AED, "+
            "CAST(0 AS DOUBLE) CY_ED_BUDGET_YTD_LCY, "+
            "CAST(0 AS DOUBLE) DERIVATIVES_INCOME, "+
            "CAST(0 AS DOUBLE) DERIVATIVES_INCOME_MTD_LCY, "+
            "CAST(0 AS DOUBLE) DERIVATIVES_INCOME_YTD_AED, "+
            "CAST(0 AS DOUBLE) DERIVATIVES_INCOME_YTD_LCY, "+
            "CAST(0 AS DOUBLE) FEE_INCOME, "+
            "CAST(0 AS DOUBLE) FEE_INCOME_MTD_LCY, "+
            "CAST(0 AS DOUBLE) FEE_INCOME_YTD_AED, "+
            "CAST(0 AS DOUBLE) FEE_INCOME_YTD_LCY, "+
            "CAST(0 AS DOUBLE) FX_INCOME, "+
            "CAST(0 AS DOUBLE) FX_INCOME_MTD_LCY, "+
            "CAST(0 AS DOUBLE) FX_INCOME_YTD_AED, "+
            "CAST(0 AS DOUBLE) FX_INCOME_YTD_LCY, "+
            "CAST(0 AS DOUBLE) GROSS_INTEREST_EXPENSE, "+
            "CAST(0 AS DOUBLE) GROSS_INTEREST_EXPENSE_MTD_LCY, "+
            "CAST(0 AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_AED, "+
            "CAST(0 AS DOUBLE) GROSS_INTEREST_EXPENSE_YTD_LCY, "+
            "CAST(0 AS DOUBLE) GROSS_INTEREST_INCOME, "+
            "CAST(0 AS DOUBLE) GROSS_INTEREST_INCOME_MTD_LCY, "+
            "CAST(0 AS DOUBLE) GROSS_INTEREST_INCOME_YTD_AED, "+
            "CAST(0 AS DOUBLE) GROSS_INTEREST_INCOME_YTD_LCY, "+
            "CAST(0 AS DOUBLE) INTERBRANCH_EXPENSE, "+
            "CAST(0 AS DOUBLE) INTERBRANCH_EXPENSE_MTD_LCY, "+
            "CAST(0 AS DOUBLE) INTERBRANCH_EXPENSE_YTD_AED, "+
            "CAST(0 AS DOUBLE) INTERBRANCH_EXPENSE_YTD_LCY, "+
            "CAST(0 AS DOUBLE) INTERBRANCH_INCOME, "+
            "CAST(0 AS DOUBLE) INTERBRANCH_INCOME_MTD_LCY, "+
            "CAST(0 AS DOUBLE) INTERBRANCH_INCOME_YTD_AED, "+
            "CAST(0 AS DOUBLE) INTERBRANCH_INCOME_YTD_LCY, "+
            "CAST(0 AS DOUBLE) LIABILITY_COF, "+
            "CAST(0 AS DOUBLE) LIABILITY_COF_MTD_LCY, "+
            "CAST(0 AS DOUBLE) LIABILITY_COF_YTD_AED, "+
            "CAST(0 AS DOUBLE) LIABILITY_COF_YTD_LCY, "+
            "CAST(0 AS DOUBLE) LP_CHARGE, "+
            "CAST(0 AS DOUBLE) LP_CHARGE_MTD_LCY, "+
            "CAST(0 AS DOUBLE) LP_CHARGE_YTD_AED, "+
            "CAST(0 AS DOUBLE) LP_CHARGE_YTD_LCY, "+
            "CAST(0 AS DOUBLE) LP_CREDIT, "+
            "CAST(0 AS DOUBLE) LP_CREDIT_MTD_LCY, "+
            "CAST(0 AS DOUBLE) LP_CREDIT_YTD_AED, "+
            "CAST(0 AS DOUBLE) LP_CREDIT_YTD_LCY, "+
            "CAST(0 AS DOUBLE) MTD_AED, "+
            "CAST(0 AS DOUBLE) MTD_AED_ACTUAL, "+
            "CAST(0 AS DOUBLE) MTD_AED_BUDGET, "+
            "CAST(0 AS DOUBLE) MTD_LCY, "+
            "CAST(0 AS DOUBLE) MTD_LCY_ACTUAL, "+
            "CAST(0 AS DOUBLE) MTD_LCY_BUDGET, "+
            "CAST(0 AS DOUBLE) NET_INT_MARGIN, "+
            "CAST(0 AS DOUBLE) NET_INT_MARGIN_MTD_LCY, "+
            "CAST(0 AS DOUBLE) NET_INT_MARGIN_YTD_AED, "+
            "CAST(0 AS DOUBLE) NET_INT_MARGIN_YTD_LCY, "+
            "CAST(0 AS DOUBLE) NET_INTEREST_INCOME, "+
            "CAST(0 AS DOUBLE) NET_INTEREST_INCOME_MTD_LCY, "+
            "CAST(0 AS DOUBLE) NET_INTEREST_INCOME_YTD_AED, "+
            "CAST(0 AS DOUBLE) NET_INTEREST_INCOME_YTD_LCY, "+
            "CAST(0 AS DOUBLE) NET_INVESTMENT_INCOME, "+
            "CAST(0 AS DOUBLE) NET_INVESTMENT_INCOME_MTD_LCY, "+
            "CAST(0 AS DOUBLE) NET_INVESTMENT_INCOME_YTD_AED, "+
            "CAST(0 AS DOUBLE) NET_INVESTMENT_INCOME_YTD_LCY, "+
            "CAST(0 AS DOUBLE) NET_PL, "+
            "CAST(0 AS DOUBLE) NET_PL_MTD_LCY, "+
            "CAST(0 AS DOUBLE) NET_PL_YTD_AED, "+
            "CAST(0 AS DOUBLE) NET_PL_YTD_LCY, "+
            "CAST(0 AS DOUBLE) OTHER_INCOME, "+
            "CAST(0 AS DOUBLE) OTHER_INCOME_MTD_LCY, "+
            "CAST(0 AS DOUBLE) OTHER_INCOME_YTD_AED, "+
            "CAST(0 AS DOUBLE) OTHER_INCOME_YTD_LCY, "+
            "CAST(0 AS DOUBLE) RWA_CREDIT_RISK, "+
            "CAST(0 AS DOUBLE) RWA_MARKET_RISK, "+
            "CAST(0 AS DOUBLE) RWA_OPERATIONAL_RISK, "+
            "CAST(0 AS DOUBLE) RWA_TOTAL, "+
            "CAST(0 AS DOUBLE) TOTAL_FTP_AMT_MTD_AED, "+
            "CAST(0 AS DOUBLE) TOTAL_FTP_AMT_MTD_LCY, "+
            "CAST(0 AS DOUBLE) TOTAL_FTP_AMT_YTD_AED, "+
            "CAST(0 AS DOUBLE) TOTAL_FTP_AMT_YTD_LCY, "+
            "CAST(0 AS DOUBLE) YTD_AED, "+
            "CAST(0 AS DOUBLE) YTD_AED_ACTUAL, "+
            "CAST(0 AS DOUBLE) YTD_AED_BUDGET, "+
            "CAST(0 AS DOUBLE) YTD_LCY, "+
            "CAST(0 AS DOUBLE) YTD_LCY_ACTUAL, "+
            "CAST(0 AS DOUBLE) YTD_LCY_BUDGET, "+
            "CAST(0 AS DOUBLE) PREV_YR_CURR_MTH_YTD_AED, "+
            "CAST(0 AS DOUBLE) PREV_YR_CURR_MTH_YTD_LCY, "+
            "CAST(0 AS DOUBLE) PREV_MTH1_MTD_AED, "+
            "CAST(0 AS DOUBLE) PREV_MTH1_MTD_LCY, "+
            "CAST(0 AS DOUBLE) PREV_MTH2_MTD_AED, "+
            "CAST(0 AS DOUBLE) PREV_MTH2_MTD_LCY, "+
            s"$time_key TIME_KEY, "+
            "'Y' LAST_REFRESH_FLAG, "+
            "'1' VERSION_ID, "+
            "'0' DOMAIN_ID"
         )
         
         dummy_dataset.registerTempTable("TMP_DUMMY_SET")
         
         val amt_class= sqlContext.sql("SELECT 'MGTBGT' AS AMOUNT_CLASS "+
                                       "UNION ALL SELECT 'MGTACT' AS AMOUNT_CLASS "+
                                       "UNION ALL SELECT 'FINACT' AS AMOUNT_CLASS ")
         amt_class.registerTempTable("TMP_AMT_CLASS")
         
         val dummy_set= sqlContext.sql (
             "SELECT "+
             "B.AMOUNT_CLASS, "+
             "A.* "+
             "FROM TMP_DUMMY_SET A CROSS JOIN TMP_AMT_CLASS B"
         )
         
         return dummy_set
  }
  
}
