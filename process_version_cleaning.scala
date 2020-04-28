package sumx_aggregate
import java.util.Date
import java.util.Calendar
import java.text.DateFormat
import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer
import sys.process._

object process_version_cleaning {
  
  def time():String={
    
    /* GET CURRENT DATESTAMP FOR LOG WRITING TO YARN */
    val timeformat= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    return timeformat.format(new Date()).toString()
  }
  
  
  def version_cleaning(sc:SparkContext,
                       time_key:String,
                       hdfs_agg_path:String,
                       overwrite_flag:Integer)  {
    
      val sparkContext= sc
    
      var version_list= new ArrayBuffer[Integer]()
      val fs= FileSystem.get(sparkContext.hadoopConfiguration)
      
      
      if (overwrite_flag == 0)
      {
          val initPath        = hdfs_agg_path+"/time_key="+time_key+"/last_refresh_flag=N"
          val checkPathExists = "hadoop fs -test -d ".concat(initPath).!
          
          if (checkPathExists == 0)
          {
              fs.listStatus(new Path(s"$hdfs_agg_path/time_key=$time_key/last_refresh_flag=N"))
                .filter(_.isDir)
                .map(_.getPath)
                .foreach(x=> {
                 version_list+=x.toString.substring(x.toString.lastIndexOf("=")+1).toInt
              })
              
              val max_version  = version_list.max
              val upper_bound  = (max_version - 4).toInt
              val lower_bound  = version_list.min.toInt
              
              if (max_version >= 5) {
                
                  var remove_list= new ArrayBuffer[Integer]()
                  for (i <- lower_bound to upper_bound) {
                  remove_list += i
                  }
                  
                  
                  //printing out all the version which are supposed to be deleted
                  remove_list.foreach(x=> {
                          val path = hdfs_agg_path+"/time_key="+time_key+"/last_refresh_flag=N/version_id="+x.toString
                          //println(path)
                          if (("hadoop fs -test -d ".concat(path).!)==0)
                          {
                              val remove_dir = "hadoop fs -rm -r ".concat(path).!
                              if (remove_dir ==0) {
                                println(time+" "+"VERSION CLEANING - VERSION HAS BEEN REMOVED FROM PATH: "+ path)
                              }
                              
                          }
                  
                  })
                  
               }
               else {
                  
                   println (time+" "+"VERSION CLEANING - MAX VERSION IS STILL <= 5 VERSIONS. NO VERSION CLEANING REQUIRED")
                
               }
            
          }
          else {
            
              println (time+" "+"VERSION CLEANING - MAX VERSION IS STILL <= 5 VERSIONS. NO VERSION CLEANING REQUIRED")
          }
        
      }
      else {
        
        println (time+" "+"VERSION CLEANING - NOT A MONTH END REFRESH. NOT APPLICABLE" )
        
      }
      
      
  }
  
}
