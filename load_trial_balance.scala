package sumx_aggregate

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter
import sys.process._

object load_trial_balance {
  
  def osx_seg_trial_balance_load (sc:SparkContext,
                                  sqlContext:SQLContext,
                                  time_key:String,
                                  dom:DataFrame, 
                                  acc:DataFrame, 
                                  le: DataFrame,
                                  osx_cust: DataFrame,
                                  osx_cont: DataFrame,
                                  osx_seg_base: DataFrame,
                                  overwrite_flag:Integer)
  {
      
      val hdfs_seg_trial_bal_r    = "/data/fin_onesumx/fin_fct_osx_trial_balance/time_key="+time_key
      val hdfs_seg_trial_bal      = "/data/fin_onesumx/fin_fct_osx_trial_balance"
      val hdfs_seg_temp           = "/data/fin_onesumx/temp"
      
      val fs= FileSystem.get(sc.hadoopConfiguration)
      var version_id = 0
      
      osx_cont.registerTempTable("DIM_OSX_CONTRACTS")
      osx_cust.registerTempTable("DIM_OSX_CUSTOMER")
      
//      println (process_sumx_agg.time+" "+"OSX TRIAL BAL - DIRECTORY FLAG FOR PROCESSING TIME_KEY :"+ ("hadoop fs -test -d ".concat(hdfs_seg_trial_bal_r).!))
//      if(("hadoop fs -test -d ".concat(hdfs_seg_trial_bal_r).!)==0)  
//      {
//         println (process_sumx_agg.time+" "+ "OSX TRIAL BAL - DELETE EXISTING PARTITION FOR RELOAD")
//         val remove_dir = "hadoop fs -rm -r ".concat(hdfs_seg_trial_bal_r).!
//         if(remove_dir==0) {
//           
//           println (process_sumx_agg.time+" "+"OSX TRIAL BAL - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE TRIAL BAL RELOAD. LOAD IN PROGRESS")
//           //pw.println (process_sumx_agg.time+" "+"OSX BASE - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE BASE AGG RELOAD. LOAD IN PROGRESS")
//         }
//      }
      
      osx_seg_base.registerTempTable("FIN_FCT_OSX_SEGMENTAL_AGG_RPT")
      
      val lkp_timekey= sqlContext.sql("SELECT TIME_KEY FROM FIN_FCT_OSX_SEGMENTAL_AGG_RPT LIMIT 1")
      lkp_timekey.registerTempTable("LKP_TIME_KEY")
      
      if (overwrite_flag==0 && ("hadoop fs -test -d ".concat(hdfs_seg_trial_bal_r).!)==0)
      {

          println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - PROCESSING ADDITIONAL VERSION LOAD OF BASE DATASET. APPLICABLE ONLY FOR MONTHLY LOAD SCENARIO" )
          
          /* LOOKUP THE DATASET IN THE LAST_REFRESH_FLAG=Y */
          fs.listStatus(new Path(s"$hdfs_seg_trial_bal/time_key=$time_key/last_refresh_flag=Y"))
            .filter(_.isDir)
            .map(_.getPath)
            .foreach(x=> {
             version_id+=x.toString.substring(x.toString.lastIndexOf("=")+1).toInt
          })
          
          /* CHECK IF THERE IS FILE AVAILABLE THEN MOVE THE FILE TO LAST_REFRESH_FLAG=N */
          println(process_sumx_agg.time+" "+"OSX TRIAL BAL - MOVING THE LATEST VERSION FILE FROM LAST REFRESH FLAG Y TO N")
          if (version_id > 0) {
            
             val exist_dir = "hadoop fs -test -d ".concat(s"$hdfs_seg_trial_bal/time_key=$time_key/last_refresh_flag=N").!                     
             if (exist_dir==1) {
                  "hadoop fs -mkdir ".concat(s"$hdfs_seg_trial_bal/time_key=$time_key/last_refresh_flag=N").!
             }
             
             val move_last_dir = (s"hadoop fs -mv $hdfs_seg_trial_bal/time_key=$time_key/last_refresh_flag=Y/version_id=$version_id"+" "+
                                  s"$hdfs_seg_trial_bal/time_key=$time_key/last_refresh_flag=N/version_id=$version_id").!
             if (move_last_dir==0){
                println(process_sumx_agg.time+" "+"OSX TRIAL BAL - FILE MOVED SUCCESSFULLY")
             }
             else {
                println(process_sumx_agg.time+" "+"OSX TRIAL BAL - FILE DID NOT MOVE SUCCESSFULLY. TERMINATING THE PROCESS")
             }
             
          }
          else {
                println(process_sumx_agg.time+" "+"OSX TRIAL BAL - THERE WERE NO FILE IN LAST REFRESH FLAG FOLDER PATH. THIS IS AN EXCEPTION. TERMINATING PROCESS")
          }
                    
          /* CREATING A TEMPORARY LOOKUP TABLE WITH LATEST VERSION ID FOR BELOW OPERATIONS */
          println(process_sumx_agg.time+" "+"OSX TRIAL BAL - CREATING DUMMY TEMP VIEW WITH PREVIOUS VERSION ID FOR CURRENT VERSION LOAD")
          val last_version= sqlContext.sql (s"SELECT $version_id AS VERSION_ID")
          last_version.registerTempTable("LKP_LAST_VERSION")
          
          val fct_trial_balance= sqlContext.sql (
          "WITH OSX_SEGMENTAL_AGG AS "+
          "(SELECT COMMON_KEY, "+
          "        FCT.TIME_KEY, "+
          "        (CAST(LKP.VERSION_ID AS INT) + 1) AS VERSION_ID, "+
          "        FCT.SOURCE_SYSTEM_ID, "+
          "        FCT.CUSTOMER_NUMBER, "+
          "        NVL(FCT.CONTRACT_ID, '0000000') CONTRACT_ID, "+
          "        FCT.CATEGORY_CODE, "+
          "        FCT.CURRENCY_CODE, "+
          "        FCT.BANKING_TYPE, "+
          "        FCT.GL_ACCOUNT_ID, "+
          "        FCT.LEGAL_ENTITY, "+
          "        FCT.PROFIT_CENTRE, "+
          "        FCT.BALANCE_SHEET_TYPE, "+ 
          "        NVL((FCT.OUTSTANDING_BALANCE_MTD_ACY), 0) OUTSTANDING_BALANCE_MTD_ACY, "+
          "        NVL((FCT.OUTSTANDING_BALANCE_MTD_LCY), 0) OUTSTANDING_BALANCE_MTD_LCY, "+
          "        NVL((FCT.OUTSTANDING_BALANCE_MTD_AED), 0) OUTSTANDING_BALANCE_MTD_AED, "+
          "        NVL((FCT.OUTSTANDING_BALANCE_YTD_ACY), 0) OUTSTANDING_BALANCE_YTD_ACY, "+
          "        NVL((FCT.OUTSTANDING_BALANCE_YTD_LCY), 0) OUTSTANDING_BALANCE_YTD_LCY, "+
          "        NVL((FCT.OUTSTANDING_BALANCE_YTD_AED), 0) OUTSTANDING_BALANCE_YTD_AED, "+
          "        FCT.BS_PL_FLAG, "+
          "        NVL((FCT.AVG_BOOK_BAL_LTD_ACY), 0) AVG_BOOK_BAL_LTD_ACY, "+
          "        NVL((FCT.AVG_BOOK_BAL_LTD_LCY), 0) AVG_BOOK_BAL_LTD_LCY, "+
          "        NVL((FCT.AVG_BOOK_BAL_LTD_AED), 0) AVG_BOOK_BAL_LTD_AED, "+
          "        NVL((FCT.AVG_BOOK_BAL_MTD_ACY), 0) AVG_BOOK_BAL_MTD_ACY, "+
          "        NVL((FCT.AVG_BOOK_BAL_MTD_LCY), 0) AVG_BOOK_BAL_MTD_LCY, "+
          "        NVL((FCT.AVG_BOOK_BAL_MTD_AED), 0) AVG_BOOK_BAL_MTD_AED, "+
          "        NVL((FCT.AVG_BOOK_BAL_YTD_ACY), 0) AVG_BOOK_BAL_YTD_ACY, "+
          "        NVL((FCT.AVG_BOOK_BAL_YTD_LCY), 0) AVG_BOOK_BAL_YTD_LCY, "+
          "        NVL((FCT.AVG_BOOK_BAL_YTD_AED), 0) AVG_BOOK_BAL_YTD_AED, "+
          "        FCT.FINAL_SEGMENT, "+
          "        FCT.CUSTOMER_SEGMENT_CODE, "+
          "        FCT.DOMAIN_ID, "+
          "        FCT.IS_INTERNAL_ACCOUNT, "+
          "        NVL((FCT.MTD_AED), 0) MTD_AED, "+
          "        NVL((FCT.MTD_LCY), 0) MTD_LCY, "+
          "        NVL((FCT.MTD_ACY), 0) MTD_ACY, "+
          "        FCT.BANKING_TYPE_CUSTOMER, "+
          "        AMOUNT_CLASS, "+
          "        CAST(cust.start_time_key AS INT) CUST_START_TIME_KEY, "+
          "        CAST(cust.end_time_key AS INT) CUST_END_TIME_KEY, "+
          "        CONCAT(cust.CUSTOMER_NUMBER, cust.BANKING_TYPE, CAST(cust.start_time_key AS STRING)) CUST_DERIVED_KEY "+
          "   FROM FIN_FCT_OSX_SEGMENTAL_AGG_RPT FCT "+
          "   INNER JOIN DIM_OSX_LEGAL_ENTITY LE ON LE.legal_entity = FCT.legal_entity "+
          "   CROSS JOIN LKP_LAST_VERSION LKP "+
          "   LEFT OUTER JOIN "+
          "   (SELECT customer_number, "+ 
          "           banking_type, "+
          "           CAST(start_time_key AS INT) start_time_key, "+
          "           CAST(end_time_key AS INT) end_time_key, "+
          "           row_number() over(partition by customer_number, banking_type order by start_time_key desc, end_time_key desc) RN "+
          "     FROM DIM_OSX_CUSTOMER CUS "+
          "     INNER JOIN LKP_TIME_KEY TK ON TK.TIME_KEY BETWEEN CUS.START_TIME_KEY AND CUS.END_TIME_KEY"+
          "    ) CUST  "+
          "    ON  TRIM(CUST.customer_number) = TRIM(FCT.customer_number) "+
          "    AND TRIM(CUST.banking_type)   = TRIM(FCT.banking_type_customer) "+
          "    AND CUST.RN =1 "+
          "    WHERE FCT.balance_sheet_type != 'BS-PL INSERT' "+
          "    AND LE.legal_entity_levl1_desc = 'FAB Group' "+
          "), "+
          "OSX_CONTRACTS AS "+
          "(SELECT CONTRACT_ID, "+ 
          "        BANKING_TYPE, "+
          "        SOURCE_SYSTEM, "+
          "        START_VALIDITY_DATE, "+
          "        END_VALIDITY_DATE, "+
          "        CAST(from_unixtime(unix_timestamp(start_validity_date, 'dd-MMM-yyyy'), 'yyyyMMdd') AS INT) START_VALIDITY_DATE_KEY, "+
          "        CAST(from_unixtime(unix_timestamp(end_validity_date, 'dd-MMM-yyyy'), 'yyyyMMdd') AS INT) END_VALIDITY_DATE_KEY, "+
          "        ROW_NUMBER() OVER (PARTITION BY CONTRACT_ID, BANKING_TYPE, SOURCE_SYSTEM ORDER BY START_VALIDITY_DATE DESC, END_VALIDITY_DATE DESC) RN "+
          "        FROM DIM_OSX_CONTRACTS CONT "+
          "        INNER JOIN LKP_TIME_KEY TK ON TK.TIME_KEY BETWEEN CAST(from_unixtime(unix_timestamp(CONT.start_validity_date, 'dd-MMM-yyyy'), 'yyyyMMdd') AS INT) "+
          "        AND CAST(from_unixtime(unix_timestamp(CONT.end_validity_date, 'dd-MMM-yyyy'), 'yyyyMMdd') AS INT) "+
          ") "+
          "SELECT "+ 
          "AGG.*, "+
          "DEAL.start_validity_date deal_start_validity_date, "+
          "DEAL.end_validity_date deal_end_validity_date, "+
          "CONCAT(AGG.contract_id, AGG.banking_type, CAST(deal.start_validity_date_key AS STRING)) contract_derived_key, "+
          "CONCAT(CAST(AGG.TIME_KEY AS STRING), AGG.CONTRACT_ID, AGG.PROFIT_CENTRE, AGG.GL_ACCOUNT_ID, AGG.CURRENCY_CODE, "+ 
          "       AGG.CUSTOMER_NUMBER, AGG.SOURCE_SYSTEM_ID, NVL(CATEGORY_CODE, '00000')) ftp_derived_key, "+
          "'Y' AS LAST_REFRESH_FLAG "+
          "FROM OSX_SEGMENTAL_AGG AGG "+
          "LEFT OUTER JOIN OSX_CONTRACTS DEAL "+
          "ON  TRIM(AGG.CONTRACT_ID)     = TRIM(DEAL.CONTRACT_ID) "+ 
          "AND TRIM(AGG.SOURCE_SYSTEM_ID)= TRIM(DEAL.SOURCE_SYSTEM) "+
          "AND TRIM(AGG.BANKING_TYPE)    = TRIM(DEAL.BANKING_TYPE) "+
          "WHERE AGG.CONTRACT_ID='0000000' "+
          "UNION ALL "+
          "SELECT "+ 
          "AGG.*, "+
          "DEAL.start_validity_date deal_start_validity_date, "+
          "DEAL.end_validity_date deal_end_validity_date, "+
          "CONCAT(AGG.contract_id, AGG.banking_type, CAST(deal.start_validity_date_key AS STRING)) contract_derived_key, "+
          "CONCAT(CAST(AGG.TIME_KEY AS STRING), AGG.CONTRACT_ID, AGG.PROFIT_CENTRE, AGG.GL_ACCOUNT_ID, AGG.CURRENCY_CODE, "+ 
          "       AGG.CUSTOMER_NUMBER, AGG.SOURCE_SYSTEM_ID, NVL(CATEGORY_CODE, '00000')) ftp_derived_key, "+ 
          "'Y' AS LAST_REFRESH_FLAG "+
          "FROM OSX_SEGMENTAL_AGG AGG "+
          "LEFT OUTER JOIN (SELECT * FROM OSX_CONTRACTS WHERE RN =1) DEAL "+
          "ON  TRIM(AGG.CONTRACT_ID) = TRIM(DEAL.CONTRACT_ID) "+ 
          "AND TRIM(AGG.BANKING_TYPE)= TRIM(DEAL.BANKING_TYPE) "+
          "WHERE AGG.CONTRACT_ID NOT IN ('0000000')"
          )
          
          println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - WRITING DATAFRAME TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_seg_trial_bal)
          fct_trial_balance.repartition(200)
                            .write
                            .partitionBy("time_key", "last_refresh_flag", "version_id", "domain_id")
                            .mode("append")
                            .format("parquet")
                            .option("compression","snappy")
                            .save(hdfs_seg_trial_bal)
                            
                            
          println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - AGGREGATE WRITE COMPLETED")
      }
      else {
        
        if(("hadoop fs -test -d ".concat(hdfs_seg_trial_bal_r).!)==0)  
        {
            println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - DELETING EXISTING DIRECTORY ")
            val remove_dir = "hadoop fs -rm -r ".concat(hdfs_seg_trial_bal_r).!
            print(remove_dir)
            if(remove_dir==0) {
             
              println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE TRIAL BAL RELOAD ")
              //pw.println ("OSX BASE - EXISTING DIRECTORY FOR "+time_key+" IS DELETED BEFORE BASE AGG RELOAD. LOAD IN PROGRESS")
            }
            println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - PROCESSING VERSION OVERWRITE OF TRIAL BAL DATASET. APPLICABLE ONLY FOR DAILY LOAD SCENARIO" )
         }
         else {
           
            println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - PROCESSING FIRST VERSION OF TRIAL BAL DATASET. APPLICABLE FOR BOTH DAILY AND MONTHLY LOAD SCENARIO" )
        }
        
        val fct_trial_balance= sqlContext.sql (
          "WITH OSX_SEGMENTAL_AGG AS "+
          "(SELECT COMMON_KEY, "+
          "        FCT.TIME_KEY, "+
          "        CAST(1 AS INT) VERSION_ID, "+
          "        FCT.SOURCE_SYSTEM_ID, "+
          "        FCT.CUSTOMER_NUMBER, "+
          "        NVL(FCT.CONTRACT_ID, '0000000') CONTRACT_ID, "+
          "        FCT.CATEGORY_CODE, "+
          "        FCT.CURRENCY_CODE, "+
          "        FCT.BANKING_TYPE, "+
          "        FCT.GL_ACCOUNT_ID, "+
          "        FCT.LEGAL_ENTITY, "+
          "        FCT.PROFIT_CENTRE, "+
          "        FCT.BALANCE_SHEET_TYPE, "+ 
          "        NVL((FCT.OUTSTANDING_BALANCE_MTD_ACY), 0) OUTSTANDING_BALANCE_MTD_ACY, "+
          "        NVL((FCT.OUTSTANDING_BALANCE_MTD_LCY), 0) OUTSTANDING_BALANCE_MTD_LCY, "+
          "        NVL((FCT.OUTSTANDING_BALANCE_MTD_AED), 0) OUTSTANDING_BALANCE_MTD_AED, "+
          "        NVL((FCT.OUTSTANDING_BALANCE_YTD_ACY), 0) OUTSTANDING_BALANCE_YTD_ACY, "+
          "        NVL((FCT.OUTSTANDING_BALANCE_YTD_LCY), 0) OUTSTANDING_BALANCE_YTD_LCY, "+
          "        NVL((FCT.OUTSTANDING_BALANCE_YTD_AED), 0) OUTSTANDING_BALANCE_YTD_AED, "+
          "        FCT.BS_PL_FLAG, "+
          "        NVL((FCT.AVG_BOOK_BAL_LTD_ACY), 0) AVG_BOOK_BAL_LTD_ACY, "+
          "        NVL((FCT.AVG_BOOK_BAL_LTD_LCY), 0) AVG_BOOK_BAL_LTD_LCY, "+
          "        NVL((FCT.AVG_BOOK_BAL_LTD_AED), 0) AVG_BOOK_BAL_LTD_AED, "+
          "        NVL((FCT.AVG_BOOK_BAL_MTD_ACY), 0) AVG_BOOK_BAL_MTD_ACY, "+
          "        NVL((FCT.AVG_BOOK_BAL_MTD_LCY), 0) AVG_BOOK_BAL_MTD_LCY, "+
          "        NVL((FCT.AVG_BOOK_BAL_MTD_AED), 0) AVG_BOOK_BAL_MTD_AED, "+
          "        NVL((FCT.AVG_BOOK_BAL_YTD_ACY), 0) AVG_BOOK_BAL_YTD_ACY, "+
          "        NVL((FCT.AVG_BOOK_BAL_YTD_LCY), 0) AVG_BOOK_BAL_YTD_LCY, "+
          "        NVL((FCT.AVG_BOOK_BAL_YTD_AED), 0) AVG_BOOK_BAL_YTD_AED, "+
          "        FCT.FINAL_SEGMENT, "+
          "        FCT.CUSTOMER_SEGMENT_CODE, "+
          "        FCT.DOMAIN_ID, "+
          "        FCT.IS_INTERNAL_ACCOUNT, "+
          "        NVL((FCT.MTD_AED), 0) MTD_AED, "+
          "        NVL((FCT.MTD_LCY), 0) MTD_LCY, "+
          "        NVL((FCT.MTD_ACY), 0) MTD_ACY, "+
          "        FCT.BANKING_TYPE_CUSTOMER, "+
          "        AMOUNT_CLASS, "+
          "        CAST(cust.start_time_key AS INT) CUST_START_TIME_KEY, "+
          "        CAST(cust.end_time_key AS INT) CUST_END_TIME_KEY, "+
          "        CONCAT(cust.CUSTOMER_NUMBER, cust.BANKING_TYPE, CAST(cust.start_time_key AS STRING)) CUST_DERIVED_KEY "+
          "   FROM FIN_FCT_OSX_SEGMENTAL_AGG_RPT FCT "+
          "   INNER JOIN DIM_OSX_LEGAL_ENTITY LE ON LE.legal_entity = FCT.legal_entity "+
          "   LEFT OUTER JOIN "+
          "   (SELECT customer_number, "+ 
          "           banking_type, "+
          "           CAST(start_time_key AS INT) start_time_key, "+
          "           CAST(end_time_key AS INT) end_time_key, "+
          "           row_number() over(partition by customer_number, banking_type order by start_time_key desc, end_time_key desc) RN "+
          "     FROM DIM_OSX_CUSTOMER CUS "+
          "     INNER JOIN LKP_TIME_KEY TK ON TK.TIME_KEY BETWEEN CUS.START_TIME_KEY AND CUS.END_TIME_KEY"+
          "    ) CUST  "+
          "    ON  TRIM(CUST.customer_number) = TRIM(FCT.customer_number) "+
          "    AND TRIM(CUST.banking_type)   = TRIM(FCT.banking_type_customer) "+
          "    AND CUST.RN =1 "+
          "    WHERE FCT.balance_sheet_type != 'BS-PL INSERT' "+
          "    AND LE.legal_entity_levl1_desc = 'FAB Group' "+
          "), "+
          "OSX_CONTRACTS AS "+
          "(SELECT CONTRACT_ID, "+ 
          "        BANKING_TYPE, "+
          "        SOURCE_SYSTEM, "+
          "        START_VALIDITY_DATE, "+
          "        END_VALIDITY_DATE, "+
          "        CAST(from_unixtime(unix_timestamp(start_validity_date, 'dd-MMM-yyyy'), 'yyyyMMdd') AS INT) START_VALIDITY_DATE_KEY, "+
          "        CAST(from_unixtime(unix_timestamp(end_validity_date, 'dd-MMM-yyyy'), 'yyyyMMdd') AS INT) END_VALIDITY_DATE_KEY, "+
          "        ROW_NUMBER() OVER (PARTITION BY CONTRACT_ID, BANKING_TYPE, SOURCE_SYSTEM ORDER BY START_VALIDITY_DATE DESC, END_VALIDITY_DATE DESC) RN "+
          "        FROM DIM_OSX_CONTRACTS CONT "+
          "        INNER JOIN LKP_TIME_KEY TK ON TK.TIME_KEY BETWEEN CAST(from_unixtime(unix_timestamp(CONT.start_validity_date, 'dd-MMM-yyyy'), 'yyyyMMdd') AS INT) "+
          "        AND CAST(from_unixtime(unix_timestamp(CONT.end_validity_date, 'dd-MMM-yyyy'), 'yyyyMMdd') AS INT) "+
          ") "+
          "SELECT "+ 
          "AGG.*, "+
          "DEAL.start_validity_date deal_start_validity_date, "+
          "DEAL.end_validity_date deal_end_validity_date, "+
          "CONCAT(AGG.contract_id, AGG.banking_type, CAST(deal.start_validity_date_key AS STRING)) contract_derived_key, "+
          "CONCAT(CAST(AGG.TIME_KEY AS STRING), AGG.CONTRACT_ID, AGG.PROFIT_CENTRE, AGG.GL_ACCOUNT_ID, AGG.CURRENCY_CODE, "+ 
          "       AGG.CUSTOMER_NUMBER, AGG.SOURCE_SYSTEM_ID, NVL(CATEGORY_CODE, '00000')) ftp_derived_key, "+
          "'Y' AS LAST_REFRESH_FLAG "+
          "FROM OSX_SEGMENTAL_AGG AGG "+
          "LEFT OUTER JOIN OSX_CONTRACTS DEAL "+
          "ON  TRIM(AGG.CONTRACT_ID)     = TRIM(DEAL.CONTRACT_ID) "+ 
          "AND TRIM(AGG.SOURCE_SYSTEM_ID)= TRIM(DEAL.SOURCE_SYSTEM) "+
          "AND TRIM(AGG.BANKING_TYPE)    = TRIM(DEAL.BANKING_TYPE) "+
          "WHERE AGG.CONTRACT_ID='0000000' "+
          "UNION ALL "+
          "SELECT "+ 
          "AGG.*, "+
          "DEAL.start_validity_date deal_start_validity_date, "+
          "DEAL.end_validity_date deal_end_validity_date, "+
          "CONCAT(AGG.contract_id, AGG.banking_type, CAST(deal.start_validity_date_key AS STRING)) contract_derived_key, "+
          "CONCAT(CAST(AGG.TIME_KEY AS STRING), AGG.CONTRACT_ID, AGG.PROFIT_CENTRE, AGG.GL_ACCOUNT_ID, AGG.CURRENCY_CODE, "+ 
          "       AGG.CUSTOMER_NUMBER, AGG.SOURCE_SYSTEM_ID, NVL(CATEGORY_CODE, '00000')) ftp_derived_key, "+
          "'Y' AS LAST_REFRESH_FLAG "+
          "FROM OSX_SEGMENTAL_AGG AGG "+
          "LEFT OUTER JOIN (SELECT * FROM OSX_CONTRACTS WHERE RN =1) DEAL "+
          "ON  TRIM(AGG.CONTRACT_ID) = TRIM(DEAL.CONTRACT_ID) "+ 
          "AND TRIM(AGG.BANKING_TYPE)= TRIM(DEAL.BANKING_TYPE) "+
          "WHERE AGG.CONTRACT_ID NOT IN ('0000000')"
          )
          
          println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - WRITING DATAFRAME TO HDFS USING SNAPPPY COMPRESSION IN PARQUET FORMAT. WRITE PATH :"+ hdfs_seg_trial_bal)
          fct_trial_balance.repartition(200)
                            .write
                            .partitionBy("time_key", "last_refresh_flag", "version_id", "domain_id")
                            .mode("append")
                            .format("parquet")
                            .option("compression","snappy")
                            .save(hdfs_seg_trial_bal)
                            
                            
          println (process_sumx_agg.time+" "+  "OSX TRIAL BAL - AGGREGATE WRITE COMPLETED")
        
      }
      
  }
  
}
