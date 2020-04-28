package load_dimension
import java.sql.DriverManager
import java.util.Properties

import org.apache.log4j._
import org.apache.spark.SparkConf;
import org.apache.spark._  
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{StringType ,IntegerType, DoubleType}

import java.net.Authenticator
import org.apache.hadoop.conf.Configuration

/* for writing logs w.r.t. data load process */
import java.io._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.YearMonth
import org.apache.spark.sql.SaveMode

object load_osx_contracts {
  
  def time:String={
    
    /* GET CURRENT DATESTAMP FOR LOG WRITING TO YARN */
    val timeformat= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    return timeformat.format(new Date()).toString()
  }
  
  def sparkConfig():SparkConf = {
    
    val conf = new SparkConf().
                            setMaster("yarn").
                            setAppName("ProcessSumx - DimensionLoad").
                            set("spark.hadoop.fs.defaultFS","hdfs://dev1node01.fgb.ae:8020").
                            set("spark.hadoop.yarn.resourcemanager.address", "dev1node04.fgb.ae:8032").
                            set("spark.hadoop.yarn.resourcemanager.hostname","dev1node04.fgb.ae").
//                            set("spark.yarn.jars","hdfs://dev1node01.fgb.ae:8020/user/spark/share/lib/*.jar").
//                            set("spark.hadoop.yarn.application.classpath", 
//                                "$HADOOP_CLIENT_CONF_DIR,"
//                               +"$HADOOP_CONF_DIR,"
//                               +"$HADOOP_COMMON_HOME/*,"
//                               +"$HADOOP_COMMON_HOME/lib/*,"
//                               +"$HADOOP_HDFS_HOME/*,"
//                               +"$HADOOP_HDFS_HOME/lib/*,"
//                               +"$HADOOP_YARN_HOME/*,"
//                               +"$HADOOP_YARN_HOME/lib/*").
//                            set("spark.sql.warehouse.dir","hdfs://bda1node01.fgb.ae:8020/user/hive/warehouse").
//                            set("hive.metastore.uris","thrift://bda1node01.fgb.ae:9083").
//                            set("spark.local.dir","/tmp/spark_temp").
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
  
  def main (args:Array[String]) {
    
    val sparkConf= sparkConfig()
    val sparkContext= new SparkContext(sparkConf)
    val sqlContext= new SQLContext(sparkContext)
    
    val hdfs_osx_contracts_raw = "/raw/onesumx/daily/T_OUTBOUND_TRN_CONTRACT_FGB.DAT"
    val hdfs_osx_contracts     = "/data/fin_onesumx/dim_osx_contracts"
   
    val osx_contract_schema= StructType (Array (
                                StructField ("source_system", StringType, true),
                                StructField ("entity", StringType, true),
                                StructField ("deal_id", StringType, true),
                                StructField ("tfi_id", StringType, true),
                                StructField ("deal_name", StringType, true),
                                StructField ("start_validity_date", StringType, true),
                                StructField ("end_validity_date", StringType, true),
                                StructField ("customer_nr", StringType, true),
                                StructField ("profit_centre", StringType, true),
                                StructField ("book_code", StringType, true),
                                StructField ("measurement_category", StringType, true),
                                StructField ("record_id", StringType, true),
                                StructField ("deal_type", StringType, true),
                                StructField ("deal_subtype", StringType, true),
                                StructField ("deal_date", StringType, true),
                                StructField ("value_date", StringType, true),
                                StructField ("maturity_date", StringType, true),
                                StructField ("reversal_date", StringType, true),
                                StructField ("currency", StringType, true),
                                StructField ("principal_amount", DoubleType, true),
                                StructField ("interest_rate_type", StringType, true),
                                StructField ("interest_rate", DoubleType, true),
                                StructField ("base_rate", DoubleType, true),
                                StructField ("margin", DoubleType, true),
                                StructField ("next_repayment_date", StringType, true),
                                StructField ("repayment_freq", StringType, true),
                                StructField ("repayment_day_nr", DoubleType, true),
                                StructField ("local_industry_code", StringType, true),
                                StructField ("country_of_risk", StringType, true),
                                StructField ("account_officer", StringType, true),
                                StructField ("rollover_date", StringType, true),
                                StructField ("next_rollover_date", StringType, true),
                                StructField ("rollover_freq", StringType, true),
                                StructField ("rollover_day_nr", DoubleType, true),
                                StructField ("interest_basis", StringType, true),
                                StructField ("interest_payment_freq", StringType, true),
                                StructField ("total_interest", DoubleType, true),
                                StructField ("next_interest_pay_date", StringType, true),
                                StructField ("interest_base_date", StringType, true),
                                StructField ("funding_rate", DoubleType, true),
                                StructField ("matched_opportunity_rate", DoubleType, true),
                                StructField ("accr_funding_amount", DoubleType, true),
                                StructField ("funding_centre", StringType, true),
                                StructField ("related_deal_id", StringType, true),
                                StructField ("original_entry_date", StringType, true),
                                StructField ("settlement_currency", StringType, true),
                                StructField ("in_currency", StringType, true),
                                StructField ("is_subordinated", DoubleType, true),
                                StructField ("rest_period", StringType, true),
                                StructField ("next_rest_date", StringType, true),
                                StructField ("is_int_sttld", DoubleType, true),
                                StructField ("char_cust_element1", StringType, true),
                                StructField ("char_cust_element2", StringType, true),
                                StructField ("char_cust_element3", StringType, true),
                                StructField ("char_cust_element4", StringType, true),
                                StructField ("char_cust_element5", StringType, true),
                                StructField ("char_cust_element6", StringType, true),
                                StructField ("char_cust_element7", StringType, true),
                                StructField ("char_cust_element8", StringType, true),
                                StructField ("char_cust_element9", StringType, true),
                                StructField ("num_cust_element1", DoubleType, true),
                                StructField ("num_cust_element2", DoubleType, true),
                                StructField ("num_cust_element3", DoubleType, true),
                                StructField ("num_cust_element4", DoubleType, true),
                                StructField ("num_cust_element5", DoubleType, true),
                                StructField ("num_cust_element6", DoubleType, true),
                                StructField ("reference_curve_code", StringType, true),
                                StructField ("reference_curve_point", StringType, true),
                                StructField ("buy_sell_ind", StringType, true),
                                StructField ("cof_vof", DoubleType, true),
                                StructField ("related_facility_id", DoubleType, true),
                                StructField ("related_collateral_id", DoubleType, true),
                                StructField ("underlying_asset_id", StringType, true),
                                StructField ("fx_rate", DoubleType, true),
                                StructField ("buy_sell_currency", StringType, true),
                                StructField ("deal_description", StringType, true),
                                StructField ("trade_price", DoubleType, true),
                                StructField ("quantity", DoubleType, true),
                                StructField ("strike_price", DoubleType, true),
                                StructField ("cap_rate", DoubleType, true),
                                StructField ("profit_centre_source", StringType, true),
                                StructField ("book_code_source", StringType, true),
                                StructField ("deal_type_source", StringType, true),
                                StructField ("from_gl_code", StringType, true),
                                StructField ("to_gl_code", StringType, true),
                                StructField ("number_of_period", DoubleType, true),
                                StructField ("adjustment_id", DoubleType, true),
                                StructField ("last_modified", StringType, true),
                                StructField ("modified_by", StringType, true)
                              ))
     
     println(time+" "+"DIM_OSX_CONTRACTS - SCHEMA INITIALIZED")
     
     val osx_contract_base= sqlContext.read.format("com.databricks.spark.csv")
                                           .option("header", "true")
                                           .option("delimiter","~")
                                           .schema(osx_contract_schema)
                                           .load (hdfs_osx_contracts_raw)
                               

     osx_contract_base.registerTempTable("DIM_OSX_CONTRACTS_INTERIM")
     
     println(time+" "+"DIM_OSX_CONTRACTS - LOAD INITIATED")
    
     val dim_osx_contracts = sqlContext.sql(
          "SELECT "+
          "CONTRACT_ID, "+
          "CUSTOMER_NUMBER, "+
          "BANKING_TYPE, "+
          "JOB_ID, "+
          "ACCOUNT_OFFICER, "+
          "CATEGORY, "+
          "CONTRACT_SUB_TYPE, "+
          "BOOKING_DATE, "+
          "MAT_DATE, "+
          "MEASUREMENT_CATEGORY, "+
          "REPAYMENT_DAY_NR, "+
          "CONTRACT_AMOUNT, "+
          "ACCOUNT_NUMBER, "+
          "ROLLOVER_DAY_NR, "+
          "CURRENCY, "+
          "RELATED_DEAL_ID, "+
          "REST_PERIOD, "+
          "IS_INT_STTLD, "+
          "CHAR_CUST_ELEMENT8, "+
          "BOOK_CODE, "+
          "CONTRACT_STATUS, "+
          "CHAR_CUST_ELEMENT9, "+
          "VALUE_DATE, "+
          "DEPT_CODE, "+
          "NUM_CUST_ELEMENT1, "+
          "FLEXIBLE_CONTRACTS_FLAG, "+
          "LOAN_TYPE_DESC, "+
          "LIMIT_REF, "+
          "NUM_CUST_ELEMENT6, "+
          "REFERENCE_CURVE_CODE, "+
          "PRINCIPAL, "+
          "REFERENCE_CURVE_POINT, "+
          "BUY_SELL_IND, "+
          "UNDERLYING_ASSET_ID, "+
          "TRADE_PRICE, "+
          "QUANTITY, "+
          "ENTITY, "+
          "ACCR_FUNDING_AMOUNT, "+
          "BASE_RATE, "+
          "BUY_SELL_CURRENCY, "+
          "REPAYMENT_TYPE, "+
          "IBA_FLAG, "+
          "MX_GID, "+
          "LOAN_PURPOSE, "+
          "ORIGINATION_GEOGRAPHY, "+
          "COF_VOF, "+
          "COUNTRY_OF_RISK, "+
          "DEAL_DESCRIPTION, "+
          "DEAL_NAME, "+
          "END_VALIDITY_DATE, "+
          "FUNDING_CENTRE, "+
          "FUNDING_RATE, "+
          "FX_RATE, "+
          "IN_CURRENCY, "+
          "INTEREST_BASE_DATE, "+
          "INTEREST_BASIS, "+
          "INTEREST_PAYMENT_FREQ, "+
          "INTEREST_RATE, "+
          "IS_SUBORDINATED, "+
          "LOCAL_INDUSTRY_CODE, "+
          "MARGIN, "+
          "MATCHED_OPPORTUNITY_RATE, "+
          "NEXT_INTEREST_PAY_DATE, "+
          "NEXT_REPAYMENT_DATE, "+
          "NEXT_REST_DATE, "+
          "NEXT_ROLLOVER_DATE, "+
          "RESTRUCTURING_FLAG, "+
          "TOPUP_FLAG, "+
          "GLCMS_COMMITMENT_FLAG, "+
          "GLCMS_REVOLVING_FLAG, "+
          "ORIGINAL_ENTRY_DATE, "+
          "BRANCH, "+
          "REPAYMENT_FREQ, "+
          "REVERSAL_DATE, "+
          "ROLLOVER_DATE, "+
          "ROLLOVER_FREQ, "+
          "SETTLEMENT_CURRENCY, "+
          "SOURCE_SYSTEM, "+
          "START_VALIDITY_DATE, "+
          "TFI_ID, "+
          "TOTAL_INTEREST, "+
          "STRIKE_PRICE, "+
          "CAP_RATE, "+
          "PROFIT_CENTRE_SOURCE, "+
          "BOOK_CODE_SOURCE, "+
          "DEAL_TYPE_SOURCE, "+
          "FROM_GL_CODE, "+
          "TO_GL_CODE, "+
          "NUMBER_OF_PERIOD, "+
          "ADJUSTMENT_ID, "+
          "RELATED_FACILITY_ID, "+
          "RELATED_COLLATERAL_ID, "+
          "LATEST_FLAG, "+
          "CONCAT(CONTRACT_ID, BANKING_TYPE, CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(START_VALIDITY_DATE),'yyyyMMdd') AS STRING)) CONTRACT_KEY "+
          "FROM ( "+
          "SELECT Q1.*, "+
          "ROW_NUMBER() OVER (PARTITION BY SOURCE_SYSTEM,BANKING_TYPE,CONTRACT_ID,START_VALIDITY_DATE,END_VALIDITY_DATE ORDER BY CONTRACT_ID) AS RN "+
          "FROM ("+
          "SELECT "+
          "NVL((SUBSTR(DEAL_ID,(INSTR(DEAL_ID,'#')+1))),DEAL_ID) CONTRACT_ID, "+
          "NVL((SUBSTR(CUSTOMER_NR,(INSTR(CUSTOMER_NR,'#')+1))),CUSTOMER_NR) CUSTOMER_NUMBER, "+
          "CASE "+
          "WHEN INSTR(DEAL_ID, '#') <> 0 "+
          "THEN SUBSTR(DEAL_ID, 0, INSTR(DEAL_ID,'#')-1) "+
          "ELSE 'NA' "+
          "END BANKING_TYPE, "+
          "FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_TIMESTAMP),'yyyyMMddHHmmss')  JOB_ID, "+
          "ACCOUNT_OFFICER, "+
          "DEAL_TYPE CATEGORY, "+
          "DEAL_SUBTYPE CONTRACT_SUB_TYPE, "+
          "DEAL_DATE BOOKING_DATE, "+
          "MATURITY_DATE MAT_DATE, "+
          "MEASUREMENT_CATEGORY, "+
          "CAST(REPAYMENT_DAY_NR AS INT) REPAYMENT_DAY_NR, "+
          "CAST(PRINCIPAL_AMOUNT AS DOUBLE) CONTRACT_AMOUNT, "+
          "NVL((SUBSTR(DEAL_ID,(INSTR(DEAL_ID,'#')+1))),DEAL_ID) ACCOUNT_NUMBER, "+
          "CAST(ROLLOVER_DAY_NR AS INT) ROLLOVER_DAY_NR, "+
          "CURRENCY, "+
          "RELATED_DEAL_ID, "+
          "REST_PERIOD, "+
          "CAST(IS_INT_STTLD AS INT) IS_INT_STTLD, "+
          "CHAR_CUST_ELEMENT8, "+
          "BOOK_CODE, "+
          "RECORD_ID CONTRACT_STATUS, "+
          "CHAR_CUST_ELEMENT9, "+
          "VALUE_DATE, "+
          "BOOK_CODE DEPT_CODE, "+
          "CAST(NUM_CUST_ELEMENT1 AS INT) NUM_CUST_ELEMENT1, "+
          "CAST(INTEREST_RATE_TYPE AS INT) FLEXIBLE_CONTRACTS_FLAG, "+
          "CHAR_CUST_ELEMENT7 LOAN_TYPE_DESC, "+
          "CHAR_CUST_ELEMENT2 LIMIT_REF, "+
          "CAST(NUM_CUST_ELEMENT6 AS INT) NUM_CUST_ELEMENT6, "+
          "REFERENCE_CURVE_CODE, "+
          "CAST(PRINCIPAL_AMOUNT AS DOUBLE) PRINCIPAL, "+
          "REFERENCE_CURVE_POINT, "+
          "BUY_SELL_IND, "+
          "UNDERLYING_ASSET_ID, "+
          "CAST(TRADE_PRICE AS DOUBLE) TRADE_PRICE, "+
          "CAST(QUANTITY AS DOUBLE) QUANTITY, "+
          "ENTITY ENTITY, "+
          "CAST(ACCR_FUNDING_AMOUNT AS DOUBLE) ACCR_FUNDING_AMOUNT, "+
          "CAST(BASE_RATE AS DOUBLE) BASE_RATE, "+
          "BUY_SELL_CURRENCY, "+
          "CHAR_CUST_ELEMENT1 REPAYMENT_TYPE, "+
          "CHAR_CUST_ELEMENT3 IBA_FLAG, "+
          "CHAR_CUST_ELEMENT4 MX_GID, "+
          "CHAR_CUST_ELEMENT5 LOAN_PURPOSE, "+
          "CHAR_CUST_ELEMENT6 ORIGINATION_GEOGRAPHY, "+
          "CAST(COF_VOF AS DOUBLE) COF_VOF, "+
          "COUNTRY_OF_RISK, "+
          "DEAL_DESCRIPTION, "+
          "DEAL_NAME, "+
          "END_VALIDITY_DATE, "+
          "FUNDING_CENTRE, "+
          "CAST(FUNDING_RATE AS DOUBLE) FUNDING_RATE, "+
          "CAST(FX_RATE AS DOUBLE) FX_RATE, "+
          "IN_CURRENCY, "+
          "INTEREST_BASE_DATE, "+
          "INTEREST_BASIS, "+
          "INTEREST_PAYMENT_FREQ, "+
          "CAST(INTEREST_RATE AS DOUBLE) INTEREST_RATE, "+
          "CAST(IS_SUBORDINATED AS INT) IS_SUBORDINATED, "+
          "LOCAL_INDUSTRY_CODE, "+
          "CAST(MARGIN AS DOUBLE) MARGIN, "+
          "CAST(MATCHED_OPPORTUNITY_RATE AS DOUBLE) MATCHED_OPPORTUNITY_RATE, "+
          "NEXT_INTEREST_PAY_DATE, "+
          "NEXT_REPAYMENT_DATE, "+
          "NEXT_REST_DATE, "+
          "NEXT_ROLLOVER_DATE, "+
          "CAST(NUM_CUST_ELEMENT2 AS INT) RESTRUCTURING_FLAG, "+
          "CAST(NUM_CUST_ELEMENT3 AS INT) TOPUP_FLAG, "+
          "CAST(NUM_CUST_ELEMENT4 AS INT) GLCMS_COMMITMENT_FLAG, "+
          "CAST(NUM_CUST_ELEMENT5 AS INT) GLCMS_REVOLVING_FLAG, "+
          "ORIGINAL_ENTRY_DATE, "+
          "PROFIT_CENTRE BRANCH, "+
          "CAST(REPAYMENT_FREQ AS INT) REPAYMENT_FREQ, "+
          "REVERSAL_DATE, "+
          "ROLLOVER_DATE, "+
          "ROLLOVER_FREQ, "+
          "SETTLEMENT_CURRENCY, "+
          "SOURCE_SYSTEM, "+
          "START_VALIDITY_DATE, "+
          "TFI_ID, "+
          "CAST(TOTAL_INTEREST AS DOUBLE) TOTAL_INTEREST, "+
          "CAST(STRIKE_PRICE AS DOUBLE) STRIKE_PRICE, "+
          "CAST(CAP_RATE AS DOUBLE) CAP_RATE, "+
          "PROFIT_CENTRE_SOURCE, "+
          "BOOK_CODE_SOURCE, "+
          "DEAL_TYPE_SOURCE, "+
          "FROM_GL_CODE, "+
          "TO_GL_CODE, "+
          "CAST(NUMBER_OF_PERIOD AS INT) NUMBER_OF_PERIOD, "+
          "CAST(ADJUSTMENT_ID AS INT) ADJUSTMENT_ID, "+
          "CAST(RELATED_FACILITY_ID AS INT) RELATED_FACILITY_ID, "+
          "CAST(RELATED_COLLATERAL_ID AS INT) RELATED_COLLATERAL_ID, "+
          "CASE WHEN TO_DATE(CAST(UNIX_TIMESTAMP(END_VALIDITY_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP))='9999-12-31' THEN 'Y' ELSE 'N' END LATEST_FLAG "+
          "FROM ( "+
          "SELECT "+
          " SOURCE_SYSTEM, "+
          " DEAL_ID, "+
          " TFI_ID, "+
          " DEAL_NAME, "+
          " CUSTOMER_NR, "+
          " PROFIT_CENTRE, "+
          " BOOK_CODE, "+
          " MEASUREMENT_CATEGORY, "+
          " RECORD_ID, "+
          " DEAL_TYPE, "+
          " DEAL_SUBTYPE, "+
          " CAST(UNIX_TIMESTAMP(DEAL_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) DEAL_DATE, "+
          " CAST(UNIX_TIMESTAMP(VALUE_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) VALUE_DATE, "+
          " CAST(UNIX_TIMESTAMP(MATURITY_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) MATURITY_DATE, "+
          " CAST(UNIX_TIMESTAMP(REVERSAL_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) REVERSAL_DATE, "+
          " CURRENCY, "+
          " PRINCIPAL_AMOUNT, "+
          " INTEREST_RATE, "+
          " BASE_RATE, "+
          " MARGIN, "+
          " CAST(UNIX_TIMESTAMP(NEXT_REPAYMENT_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) NEXT_REPAYMENT_DATE, "+
          " REPAYMENT_FREQ, "+
          " REPAYMENT_DAY_NR, "+
          " LOCAL_INDUSTRY_CODE, "+
          " COUNTRY_OF_RISK, "+
          " ACCOUNT_OFFICER, "+
          " CAST(UNIX_TIMESTAMP(ROLLOVER_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) ROLLOVER_DATE, "+
          " CAST(UNIX_TIMESTAMP(NEXT_ROLLOVER_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) NEXT_ROLLOVER_DATE, "+
          " ROLLOVER_FREQ, "+
          " ROLLOVER_DAY_NR, "+
          " INTEREST_BASIS, "+
          " INTEREST_PAYMENT_FREQ, "+
          " TOTAL_INTEREST, "+
          " CAST(UNIX_TIMESTAMP(NEXT_INTEREST_PAY_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) NEXT_INTEREST_PAY_DATE, "+
          " CAST(UNIX_TIMESTAMP(INTEREST_BASE_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) INTEREST_BASE_DATE, "+
          " FUNDING_RATE, "+
          " MATCHED_OPPORTUNITY_RATE, "+
          " ACCR_FUNDING_AMOUNT, "+
          " FUNDING_CENTRE, "+
          " RELATED_DEAL_ID, "+
          " CAST(UNIX_TIMESTAMP(ORIGINAL_ENTRY_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) ORIGINAL_ENTRY_DATE, "+
          " SETTLEMENT_CURRENCY, "+
          " IN_CURRENCY, "+
          " IS_SUBORDINATED, "+
          " REST_PERIOD, "+
          " CAST(UNIX_TIMESTAMP(NEXT_REST_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) NEXT_REST_DATE, "+
          " IS_INT_STTLD, "+
          " CHAR_CUST_ELEMENT1, "+
          " CHAR_CUST_ELEMENT2, "+
          " CHAR_CUST_ELEMENT3, "+
          " CHAR_CUST_ELEMENT4, "+
          " CHAR_CUST_ELEMENT5, "+
          " CHAR_CUST_ELEMENT6, "+
          " CHAR_CUST_ELEMENT7, "+
          " CHAR_CUST_ELEMENT8, "+
          " CHAR_CUST_ELEMENT9, "+
          " NUM_CUST_ELEMENT1, "+
          " NUM_CUST_ELEMENT2, "+
          " NUM_CUST_ELEMENT3, "+
          " NUM_CUST_ELEMENT4, "+
          " NUM_CUST_ELEMENT5, "+
          " NUM_CUST_ELEMENT6, "+
          " CAST(UNIX_TIMESTAMP(LAST_MODIFIED,'MMM dd yyyy HH:mm') AS TIMESTAMP) LAST_MODIFIED, "+
          " MODIFIED_BY, "+
          " REFERENCE_CURVE_CODE, "+
          " REFERENCE_CURVE_POINT, "+
          " BUY_SELL_IND, "+
          " COF_VOF, "+
          " INTEREST_RATE_TYPE, "+
          " UNDERLYING_ASSET_ID, "+
          " FX_RATE, "+
          " BUY_SELL_CURRENCY, "+
          " DEAL_DESCRIPTION, "+
          " TRADE_PRICE, "+
          " QUANTITY, "+
          " STRIKE_PRICE, "+
          " CAP_RATE, "+
          " PROFIT_CENTRE_SOURCE, "+
          " BOOK_CODE_SOURCE, "+
          " DEAL_TYPE_SOURCE, "+
          " CASE WHEN LENGTH(FROM_GL_CODE)=1 THEN NULL ELSE FROM_GL_CODE END FROM_GL_CODE, "+
          " CASE WHEN LENGTH(TO_GL_CODE)=1 THEN NULL ELSE TO_GL_CODE END TO_GL_CODE, "+
          " NUMBER_OF_PERIOD, "+
          " ADJUSTMENT_ID, "+
          " ENTITY, "+
          " CAST(UNIX_TIMESTAMP(START_VALIDITY_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) START_VALIDITY_DATE, "+
          " CAST(UNIX_TIMESTAMP(END_VALIDITY_DATE,'MMM dd yyyy HH:mm') AS TIMESTAMP) END_VALIDITY_DATE, "+
          " RELATED_FACILITY_ID, "+
          " RELATED_COLLATERAL_ID "+
          "FROM DIM_OSX_CONTRACTS_INTERIM))Q1 ) WHERE RN=1 "
    )
    
    println(time+" "+"DIM_OSX_CONTRACTS - TRANSFORMED DATASET FOR CONTRACTS WITH DUPLICATE REMOVAL CREATED") 
   
    println(time+" "+"DIM_OSX_CONTRACTS - WRITE STARTED TO PATH: "+ hdfs_osx_contracts)
          
    dim_osx_contracts.repartition(200)
                     .write
                     .mode(SaveMode.Overwrite)
                     .option("format","parquet")
                     .option("compression","snappy")
                     .save(hdfs_osx_contracts)
                     
    println(time+" "+"DIM_OSX_CONTRACTS - WRITE COMPLETED")
    
  }
  
}
