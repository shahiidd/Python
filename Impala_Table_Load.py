# This script will upload the txt file from local temp folder to impala under adhoc database.
 
__author__ = 'Shahid'
import sys,os,re,fnmatch
from ConfigParser import ConfigParser
import csv,argparse,subprocess
from datetime import timedelta,datetime

def deleteHeader(file_with_header,subDirHdfsPath,subDirMntPath,inputFileWithOutSemAndExtension, file_type):  
    
    command = "sed '1d' " +file_with_header + ">" + subDirMntPath + "/"+inputFileWithOutSemAndExtension +"_1." + file_type
    
    print "command_header====",command
    os.system(command)
    
    cmd1 = "hadoop fs -rm " + subDirHdfsPath +"/"+ inputFileWithOutSemAndExtension + "." + file_type
    print cmd1
    os.system(cmd1)
    cmd = "hadoop fs -mv " + subDirHdfsPath +"/"+ inputFileWithOutSemAndExtension + "_1."  + file_type + " " + subDirHdfsPath + "/" + inputFileWithOutSemAndExtension + "." + file_type
    
    print 'cmd mv========', cmd
    ExitCode = os.system(cmd)
    print cmd
    if ExitCode == 0:
        return True
    else:
        print 'fatal error in moving file from hdfs to hdfs in header deletion'
        return False

def get_DDL(Impalatablename,column,sql_file_name,dilimitedFlag):
    o = open(sql_file_loc + sql_file_name,'w')
    print "CREATE TABLE  " + dbname + ".`" + Impalatablename
    print >> o, "SELECT' " + dbname + "." + Impalatablename + "';"
    print >> o, ("DROP TABLE IF EXISTS " + dbname + ".`" + Impalatablename + "`;")
    print  >> o, ("CREATE EXTERNAL TABLE IF NOT EXISTS "+  dbname + ".`" + Impalatablename + "` ( ")
    for i in range(len(column)):
        #print i
        print  >> o , "`"+column[i].strip().replace("\"","") + "` " ,
        print >> o, dataType ,
        if i < len(column)-1:
            print >> o , ","
        
    print >> o, ")"
    print >> o, "COMMENT '"+  dbname +"." + Impalatablename + "'"                        
    print >> o, "row format delimited "
    if dilimitedFlag == 't':
        print >> o, "fields terminated by '\\t' "
    if dilimitedFlag == 'p':
        print >> o, "fields terminated by '|' "
    if dilimitedFlag == 'c':
        print >> o, "fields terminated by ',' "
    print >> o, "STORED AS textfile "
    locString = "LOCATION '" + subDirHdfsPath + "';"
    print >> o, locString                  
    o.close()
        
        
def moveFileFromLocaltoHDFS(local_source , hdfs_destination, inputFileWithOutSem):
    #command = "cp " + source + " " + destination
    cmd = "hadoop fs -rm " + hdfs_destination +"/"+ inputFileWithOutSem
    print "cmd--======",cmd
    os.system(cmd)
    command = "hadoop fs -copyFromLocal " + local_source + " " + hdfs_destination
    print "command_cp=", command
    ExitCode = os.system(command)
    print 'exitcode=' ,ExitCode
    if ExitCode != 0:
        print "FATAL ERROR: File movement to HDFS from local failed"  
        return False
    else:
        return True
        
def createDir(subDirMntPath):
    print "creating directory for " + subDirMntPath 
    #directory = subDirHdfsPath
    print "directory===", subDirMntPath
    try:
        os.stat(subDirMntPath)
        print 'directory exist'
    except:
        os.mkdir(subDirMntPath)
        print 'directory created'
        
def validate_table(sql_file_name,Impalatablename):
    o = open(sql_file_loc+"validate_"+sql_file_name,'w')
    print "CREATE VALIDATE TABLE FOR " + dbname + ".`" + Impalatablename
    print >> o, "SELECT * from " + dbname + "." + Impalatablename + " limit 1;"                
    o.close()
    
def compute_stats_table(sql_file_name,Impalatablename):
    o = open(sql_file_loc+"compute_stats_"+sql_file_name,'w')
    print "CREATE compute_stats TABLE FOR " + dbname + ".`" + Impalatablename
    print >> o, "COMPUTE STATS " + dbname + "." + Impalatablename + ";"                
    o.close()
    
def refresh_table(sql_file_name,Impalatablename):
    o = open(sql_file_loc+"refresh_"+sql_file_name,'w')
    print "CREATE refresh command FOR " + dbname + ".`" + Impalatablename
    print >> o, "refresh " + dbname + "." + Impalatablename + ";"                
    o.close()

    
def check(local_path,subDirHdfsPath, file_name,file_typev):
    count = 0
    with open(impala_log_path+file_name+'_validate.log') as f:
        for line in f:
            if "Fetched 1" in line:
                print "file uploaded"
                count = 1
                
                moveFileHdfsToLocal(local_path,subDirHdfsPath, file_name,file_type)
                #moveFileHdfsToHdfs(hdfs_path_incomimg,hdfs_path_uploaded, file_name)
        if count != 1:
            print "error in file upload to impala"
                

    
                
def moveFileHdfsToLocal(destination, source , file_name,file_type):
    
    cmd = "rm -rf " + destination + file_name + "." + file_type
    print "cmd to remove old file from local--======",cmd
    os.system(cmd)
    command = "hadoop fs -get " + source + "/"+file_name + "." + file_type+ " " + destination+file_name+ "." +file_type
    print "command_cp_hdfs_to_local=", command   
    ExitCode = os.system(command)
    print 'exitcode=' ,ExitCode
    if ExitCode != 0:
        print "FATAL ERROR: File movement to local from hdfs failed"  
        return False
    else:
    ##############remove sem file from local#########
        rm_semfile_commamd = "rm " + temp_dir + semFileName
        print "rm_semfile_commamd",rm_semfile_commamd
        os.system(rm_semfile_commamd)
    ###################remove main file############
        rm_main_commamd = "rm " + temp_dir + inputFileWithOutSem
        print "rm_main_commamd",rm_main_commamd
        os.system(rm_main_commamd)
        return True

        
if __name__ == "__main__":
    
    temp_dir = "/data/Incoming_to_Impala/"
    #temp_dir = "/home/mohd.shahid2/Impala/"
    temp_dir1 = "/data/Uploaded_to_Impala/"
    hdfs_url_uploaded = "hdfs://172.20.17.133:8020/projects/CVSC0014/raw-data/Uploaded_to_Impala/"
    
    hdfs_url_incoming = "hdfs://172.20.17.133:8020/projects/CVSC0014/raw-data/Incoming_to_Impala/"
    hdfs_mnt_incoming = "/mnt/hdfs/projects/CVSC0014/raw-data/Incoming_to_Impala/"
    sql_file_loc = "/home/mohd.shahid2/CVSC0013-2/trunk/Impala_sql/"
    impala_log_path = "/home/mohd.shahid2/CVSC0013-2/trunk/Impala_log/"
    impalad_host = "CVS-PROD-HS3.ny.os.local:21000"
    
    dbname = "cvs_temp_tables"
    dataType = "string"
    command0 = 'ls ' +  temp_dir + '*.sem' 
    print command0
    cmd = os.system(command0)
    print cmd
    if cmd !=0:
        date_today= datetime.now()
        print date_today
        sys.exit(0);
    else:
        proc = subprocess.Popen(command0, stdout=subprocess.PIPE, shell=True)
        proc.wait()
        ls_lines = proc.stdout.readlines()
        #print ls_lines
        for file in ls_lines:
            semFileName = file.split("/")[-1]
            print "semFileName",semFileName
            inputFileWithOutSem=semFileName.replace(".sem","").strip().replace("\"","")
            print "inputFileWithOutSem",inputFileWithOutSem
            
            file_name_with_path_local= temp_dir+inputFileWithOutSem
            print "file_name_with_path_local",file_name_with_path_local
            
            if not os.path.exists(file_name_with_path_local):
                print "data file     "+ file_name_with_path_local + "    not exist"
                ##############remove this sem file from local#########
                rm_semfile_commamd = "rm " + temp_dir + semFileName
                print "rm_semfile_commamd",rm_semfile_commamd
                os.system(rm_semfile_commamd)
                continue;
                
            inputFileWithOutSemAndExtension = inputFileWithOutSem.rsplit(".", 1)[0]
            print "inputFileWithOutSemAndExtension==", inputFileWithOutSemAndExtension
            
            subDirMntPath = hdfs_mnt_incoming + inputFileWithOutSemAndExtension
            print 'subDirMntPath', subDirMntPath
            
            subDirHdfsPath = hdfs_url_incoming + inputFileWithOutSemAndExtension
            print 'subDirHdfsPath', subDirHdfsPath
            createDir(subDirMntPath)
            
            file_type = inputFileWithOutSem.rsplit("." ,1)[1]
            print "file_type=", file_type
            print "It is " + file_type + " file"
                      
            moveFileFromLocaltoHDFS(file_name_with_path_local,subDirHdfsPath,inputFileWithOutSem )
            
            input_file_to_impala = subDirMntPath + "/" + inputFileWithOutSem
            
            print "input_file_to_impala==",input_file_to_impala, len(input_file_to_impala)
            os.system('head %s'%input_file_to_impala)
            csvfile=open(input_file_to_impala, 'rb')
            
            output=[]
            for i,row in enumerate(open(input_file_to_impala, 'rb')):
                output.append(row.strip())
                if i==0:
                    break
            print output
           
            header=output[0]
            print header 
######################################check here for pipe and tab####################             
            if header.count('|')>0:
                print 'pipe dilimited'          
                column = header.split('|')
                dilimitedFlag = 'p'
                
                no_of_column = len(column)
                print no_of_column
            
            elif header.count('\t')>0:
                print 'tab delimited'
                dilimitedFlag = 't'
                column = header.split('\t')
                no_of_column = len(column)
                print no_of_column
                
            elif header.count(',')>0:
                print 'comma delimited'
                dilimitedFlag = 'c'
                column = header.split(',')
                no_of_column = len(column)
                print no_of_column
                
            else :
                print 'one column'
                dilimitedFlag = 't'
                column = header.split('\t')
                no_of_column = len(column)
                print no_of_column
                
            
            sql_file_name=inputFileWithOutSemAndExtension + '.sql'
            #sql_file= sql_file_name+ '.sql'
            #Impalatablename = inputFileWithOutSemAndExtension
            print sql_file_name
            deleteHeader(input_file_to_impala,subDirHdfsPath,subDirMntPath,inputFileWithOutSemAndExtension,file_type)
            get_DDL(inputFileWithOutSemAndExtension,column,sql_file_name,dilimitedFlag)
            
            impala_cmd="impala-shell -l --auth_creds_ok_in_clear --impalad=" + impalad_host + " --ldap_password_cmd='cat /home/mohd.shahid2/auto_impala.pwd' --query_file=" + sql_file_loc + sql_file_name + " -B >"+impala_log_path+inputFileWithOutSemAndExtension+ ".log 2>&1"
            print "impala_cmd==", impala_cmd
            os.system(impala_cmd)
            
            print 'impala_cmd executed'
            validate_table(sql_file_name,inputFileWithOutSemAndExtension)
            validate_cmd="impala-shell -l --auth_creds_ok_in_clear --impalad=" + impalad_host + " --ldap_password_cmd='cat /home/mohd.shahid2/auto_impala.pwd' --query_file=" + sql_file_loc + "validate_"+sql_file_name + " -B >"+impala_log_path+inputFileWithOutSemAndExtension+"_validate.log 2>&1"
    
            print "validate_cmd==", validate_cmd
            os.system(validate_cmd)
            print "validate_cmd executed"
            
            compute_stats_table(sql_file_name,inputFileWithOutSemAndExtension)
            compute_stats_cmd="impala-shell -l --auth_creds_ok_in_clear --impalad=" + impalad_host + " --ldap_password_cmd='cat /home/mohd.shahid2/auto_impala.pwd' --query_file=" + sql_file_loc + "compute_stats_"+sql_file_name + " -B >"+impala_log_path+inputFileWithOutSemAndExtension+"_compute_stats.log 2>&1"
    
            print "compute_stats==", compute_stats_cmd
            os.system(compute_stats_cmd)
            print "compute_stats_cmd executed"
            
            refresh_table(sql_file_name,inputFileWithOutSemAndExtension)
            refresh_cmd="impala-shell -l --auth_creds_ok_in_clear --impalad=" + impalad_host + " --ldap_password_cmd='cat /home/mohd.shahid2/auto_impala.pwd' --query_file=" + sql_file_loc + "refresh_"+sql_file_name + " -B >"+impala_log_path+inputFileWithOutSemAndExtension+"_refresh.log 2>&1"
    
            print "refresh_cmd==", compute_stats_cmd
            os.system(refresh_cmd)
            print "refresh_cmd executed"
            
            
            
            check(temp_dir1,subDirHdfsPath,inputFileWithOutSemAndExtension,file_type)