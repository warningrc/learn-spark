package com.warningrc.test.spark.hellospark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.AccessControlException
import org.apache.commons.io.IOUtils

object HdfsTest {
    def main(args: Array[String]): Unit = {
        val fileSystem = FileSystem.get(configuration)
        listFiles(fileSystem,fileSystem.getFileStatus(new Path("/")))
        println("*"*20)
        readFile(fileSystem,"/user/hadoop/testdata/core-site.xml")
        println("*"*20)
        writeAndRenameFile(fileSystem)
    }
    def listFiles(fileSystem: FileSystem, file: FileStatus): Unit = {
        println( (if(file.isFile())"f:"else"d:")+file.getPath)
        if (file.isDirectory()) {
            try {
                fileSystem.listStatus(file.getPath).foreach(listFiles(fileSystem, _))
            } catch {
                case e: AccessControlException => {}
            }
        }
    }
    
    def readFile(fileSystem: FileSystem, file: String){
        println( IOUtils.toString(fileSystem.open(new Path(file))))
    }
    
    def writeAndRenameFile(fileSystem:FileSystem){
        val file = "/user/hadoop/testdata/log4j.properties"
        val fileRename = "/user/hadoop/testdata/log4j-rename.properties"
        val newFileInput = fileSystem.create(new Path(file))
        IOUtils.copy(getClass.getResourceAsStream("/log4j.properties"), newFileInput)
        newFileInput.hsync()
        newFileInput.close()
        readFile(fileSystem,file)
        fileSystem.rename(new Path(file), new Path(fileRename))
        println("*"*20)
        readFile(fileSystem,fileRename)
//        fileSystem.delete(new Path(file),false)
        fileSystem.delete(new Path(fileRename),false)
    }

    def configuration = {
        val conf = new Configuration()
        conf.set("fs.default.name", "hdfs://w.hadoop1:9000")
        conf.set("fs.defaultFS", "hdfs://w.hadoop1:9000")
        conf
    }
}