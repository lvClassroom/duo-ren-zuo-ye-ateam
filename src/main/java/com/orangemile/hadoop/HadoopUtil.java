package com.orangemile.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;

public class HadoopUtil {

	
	/**
	 * Performs a HAR (Hadoop Archive) unarchive - i.e. copy of files from archive into target path 
	 * @param hdfs
	 * @param harArchivePath
	 * @param target
	 * @param mapReduceQueue
	 * @throws Exception 
	 * @see hadoop distcp -Dmapreduce.job.queuename=myqueue har://hdfs-orange:8020/har/orange.har hdfs://orange:8020/target/
	 */
	public static void unarchive(FileSystem hdfs, Path harArchivePath, Path target, String mapReduceQueue ) throws Exception 
	{
		Configuration conf = hdfs.getConf();		
		if ( ! harArchivePath.toString().startsWith("har://") ) {
			throw new RuntimeException("Invalid Har-Archive-Path! Expected format:  'har://hdfs-<url>/name.har'");
		}	
		
		if ( mapReduceQueue != null ) {
			conf.set("mapreduce.job.queuename", mapReduceQueue);
		}
		
		DistCp dcp = new DistCp(conf, null);
		
		String [] args = new String [] { harArchivePath.toString(), target.toString() };
		ToolRunner.run(conf, dcp, args);		
	}


	/**
	 * 
	 * @param hdfs
	 * @param pathToArchive
	 * @param archiveFileName
	 * @param archiveTargetDir
	 * @param mapReduceQueue
	 * @return
	 * @throws Exception
	 * @see hadoop -Dmapreduce.job.queuename=myqueue archive -archiveName orange.har -p hdfs://orange:8020/src hdfs://orange:8020/har/
	 */
	public static Path archive(FileSystem hdfs, Path pathToArchive, String archiveFileName, Path archiveTargetDir, String mapReduceQueue ) throws Exception 
	{
		Path archivePath = new Path(archiveTargetDir, archiveFileName);
		if ( hdfs.exists(archivePath) ) {
			throw new RuntimeException("Hadoop Archive (HAR) already exists at path - " + archivePath );
		}
		
		Configuration conf = hdfs.getConf();		
		if ( mapReduceQueue != null ) {
			conf.set("mapreduce.job.queuename", mapReduceQueue);
		}
		
		// har command
		HadoopArchives archives = new HadoopArchives(conf);
		String args [] = new String [] {
			"-archiveName", archiveFileName, 
			"-p", pathToArchive.toString(),
			archiveTargetDir.toString()
		};
		
		// run
		archives.run(args);
				
		if ( ! hdfs.exists(archivePath) ) {
			throw new RuntimeException("Unable to create Hadoop Archive (HAR)!");
		}
		
		return archivePath;
	}
	
}
