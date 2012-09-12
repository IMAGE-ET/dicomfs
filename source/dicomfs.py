#!/usr/bin/env python2.7
# -*- coding: utf8 -*- 

#
#     Copyright 2010 - 2012 David Volgyes
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Lesser General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public License 
#    along with this program.  If not, see <https://www.gnu.org/licenses/lgpl.html>.
#

import os, stat, errno, commands, sys, re,time,threading
from errno import *
from stat import *
import fuse,fcntl
from fuse import Fuse
import gdcm
import hashlib
import datetime
from subprocess import call

if gdcm.Version.GetMajorVersion()<2 or (gdcm.Version.GetMajorVersion()==2 and gdcm.Version.GetMinorVersion()<2):
    print
    print "DicomFS requires version 2.2 or higher from libgdcm/python-gdcm."
    print "Current version: %s" % gdcm.Version.GetVersion()
    sys.exit(-1)

if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."

globaloptions=dict()
globaloptions['cachedir']="ORIGINAL"
fuse.fuse_python_api = (0, 2)


def uploadFile(filename):
    cnf_upload = gdcm.CompositeNetworkFunctions()
    cnf_filename = gdcm.FilenamesType()
    cnf_filename.append(filename)
    cnf_upload.CStore(globaloptions['server'],int(globaloptions['remoteport']),cnf_filename,globaloptions['aet'],globaloptions['aec']) 

def flag2mode(flags):
    md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)
        pass
    return m

class XmpFile(object):

        def __init__(self, path, flags, *mode):
            stripped=path.strip("/")
            if stripped in fileaccessCache: 
                self.filename=fileaccessCache[stripped]
            else:
                self.filename=globaloptions['cachedir']+"/"+stripped
            self.file = os.fdopen(os.open(self.filename, flags, *mode),flag2mode(flags))
            self.fd = self.file.fileno()
            self.upload = False
            self.direct_io=1
            self.keep_cache=1            

        def read(self, length, offset):
            self.file.seek(offset)
            return self.file.read(length)

        def write(self, buf, offset):
            self.file.seek(offset)
            self.file.write(buf)
            self.upload = True
            return len(buf)

        def release(self, flags):
            self.file.close()
            if self.upload: 
                uploadFile(self.filename)
            self.upload = False

        def _fflush(self):
            if 'w' in self.file.mode or 'a' in self.file.mode:
                self.file.flush()

        def fsync(self, isfsyncfile):
            self._fflush()
            if isfsyncfile and hasattr(os, 'fdatasync'):
                os.fdatasync(self.fd)
            else:
                os.fsync(self.fd)

        def flush(self):
            self._fflush()
            os.close(os.dup(self.fd))

        def fgetattr(self):
            return os.fstat(self.fd)

        def ftruncate(self, len):
            self.file.truncate(len)

        def lock(self, cmd, owner, **kw):
            op = { fcntl.F_UNLCK : fcntl.LOCK_UN,
                   fcntl.F_RDLCK : fcntl.LOCK_SH,
                   fcntl.F_WRLCK : fcntl.LOCK_EX }[kw['l_type']]
            if cmd == fcntl.F_GETLK:
                return -EOPNOTSUPP
            elif cmd == fcntl.F_SETLK:
                if op != fcntl.LOCK_UN:
                    op |= fcntl.LOCK_NB
            elif cmd == fcntl.F_SETLKW:
                pass
            else:
                return -EINVAL

            fcntl.lockf(self.fd, op, kw['l_start'], kw['l_len'])

class DicomDataset(object):
    def __init__(self):
        self.dataset = gdcm.DataSet()        
        pass

    def add(self,tag,value=""):
        self.dataelement=gdcm.DataElement(tag)
        self.dataelement.SetByteValue(value,gdcm.VL(len(value)))
        self.dataset.Insert(self.dataelement)
        return self

    def get(self):
        return self.dataset

class DicomConnection(object):

    server=""
    remoteport=0
    localport=0
    aetitle=""
    caller=""
    
    def __init__(self,server="",port=1040,aetitle="DICOMFS_AE",caller="DICOMFS_CALLER",localport=11112):
        self.server=str(server)
        self.remoteport=int(port)
        self.localport=int(localport)
        self.aetitle=str(aetitle)
        self.caller=str(caller)

        self.tag_StudyUID = gdcm.Tag(0x20,0x0d)
        self.tag_StudyDate = gdcm.Tag(0x08,0x20)
        self.tag_StudyTime = gdcm.Tag(0x08,0x30)
        self.tag_StudyID = gdcm.Tag(0x20,0x10)
        self.tag_StudyDescription = gdcm.Tag(0x08,0x1030)        

        self.tag_Study=[self.tag_StudyDate,self.tag_StudyTime,self.tag_StudyID,self.tag_StudyDescription]

        self.tag_SeriesUID = gdcm.Tag(0x20,0x0e)        
        self.tag_SeriesDate = gdcm.Tag(0x08,0x21)
        self.tag_SeriesTime = gdcm.Tag(0x08,0x31)        
        self.tag_SeriesDescription = gdcm.Tag(0x0008,0x103e) 
        self.tag_SeriesModality = gdcm.Tag(0x0008,0x0060) # modality
        self.tag_Series=[self.tag_SeriesDate,self.tag_SeriesDescription,self.tag_SeriesModality]
 
        self.tag_infmodel = gdcm.Tag(0x0008,0x0052)
        self.tag_SOP_UID = gdcm.Tag(0x08,0x0018)        

        self.tag_PatientName = gdcm.Tag(0x10,0x10)
        self.tag_PatientID = gdcm.Tag(0x10,0x20)        
        self.tag_Patient = [self.tag_PatientName]
#Patient's Name 	(0010,0010) 	R
#Patient ID 	(0010,0020) 	U
#Referenced Patient Sequence 	(0008,1120) 	O
#>Referenced SOP Class UID 	(0008,1150) 	O
#>Referenced SOP Instance UID 	(0008,1155) 	O
#Patient's Birth Date 	(0010,0030) 	O
#Patient's Birth Time 	(0010,0032) 	O
#Patient's Sex 	(0010,0040) 	O
#Other Patient Ids 	(0010,1000) 	O
#Other Patient Names 	(0010,1001) 	O
#Ethnic Group 	(0010,2160) 	O
#Patient Comments 	(0010,4000) 	O
#Number of Patient Related Studies 	(0020,1200) 	O
#Number of Patient Related Series 	(0020,1202) 	O
#Number of Patient Related Instances 	(0020,1204) 	O
#All other Attributes at Patient Level 		O 

    def ping(self):
        self.pingStart=time.time()
        self.pingSuccess=gdcm.CompositeNetworkFunctions.CEcho(self.server,self.remoteport,self.aetitle,self.caller)
        self.pingStop=time.time()
        if self.pingSuccess==False: return -1
        return self.pingStop-self.pingStart


    def listPatients(self):  # root query

        self.d=DicomDataset()
        self.queryTag=self.tag_PatientID
        self.d.add(self.queryTag)
        for tag in self.tag_Patient:
            self.d.add(tag)
        
        self.cnf = gdcm.CompositeNetworkFunctions()
        self.theQuery = self.cnf.ConstructQuery (gdcm.eStudyRootType,gdcm.eStudy,self.d.get())
        self.ret_query= gdcm.DataSetArrayType()

        self.cnf.CFind(self.server,self.remoteport,self.theQuery,self.ret_query,self.aetitle,self.caller)
        UIDs=[]
        desc=[]
        timestamps=[]
        for i in range(0,self.ret_query.size()):
            x=str(self.ret_query[i].GetDataElement( self.queryTag ).GetValue())
            if x in UIDs: continue
            UIDs.append(str(self.ret_query[i].GetDataElement( self.queryTag ).GetValue()))
            desc.append(str(self.ret_query[i].GetDataElement( self.tag_PatientName ).GetValue()))
            timestamps.append(0)

        return UIDs,desc,timestamps


    def listStudies(self,patientID=None):  # root query

        self.d=DicomDataset()
        self.queryTag=self.tag_StudyUID
        self.d.add(self.queryTag)
        for tag in self.tag_Study:
            self.d.add(tag)
        if patientID!=None:
            self.d.add(self.tag_PatientID,patientID)
        
        self.cnf = gdcm.CompositeNetworkFunctions()
        self.theQuery = self.cnf.ConstructQuery (gdcm.eStudyRootType,gdcm.eStudy,self.d.get())
        self.ret_query= gdcm.DataSetArrayType()

        self.cnf.CFind(self.server,self.remoteport,self.theQuery,self.ret_query,self.aetitle,self.caller)
        UIDs=[]
        desc=[]
        timestamp=[]
        for i in range(0,self.ret_query.size()):
            x=str(self.ret_query[i].GetDataElement( self.queryTag ).GetValue())
            if x in UIDs: continue
            UIDs.append(str(self.ret_query[i].GetDataElement( self.queryTag ).GetValue()))
            desc.append(str(self.ret_query[i].GetDataElement( self.tag_StudyDescription ).GetValue()))

            datestring=(self.ret_query[i].GetDataElement( self.tag_StudyDate).GetValue())
            timestring=(self.ret_query[i].GetDataElement( self.tag_StudyTime).GetValue())

            if datestring!=None and timestring!=None:
                datestring=str(datestring)
                timestring=str(timestring)
                d=datetime.datetime(year=int(datestring[0:4]),month=int(datestring[4:6]),day=int(datestring[6:]),hour=int(timestring[0:2]), minute=int(timestring[2:4]),second=int(timestring[4:]),microsecond=0)
                timestamp.append(time.mktime(d.timetuple()))
            else:
                timestamp.append(0)

        return UIDs,desc,timestamp
        

    def listSeries(self,studyUID,patientID=None):  # root query
        self.d=DicomDataset()
        self.queryTag=self.tag_SeriesUID

        self.d.add(self.queryTag)
        self.d.add(self.tag_StudyUID,studyUID)
        self.d.add(self.tag_infmodel,"SERIES")

        for tag in self.tag_Study:
            self.d.add(tag)
        for tag in self.tag_Series:
            self.d.add(tag)
        if patientID!=None:
            self.d.add(self.tag_PatientID,patientID)

        self.cnf = gdcm.CompositeNetworkFunctions()
        self.theQuery = self.cnf.ConstructQuery (gdcm.eStudyRootType,gdcm.eSeries,self.d.get())
        self.ret_query= gdcm.DataSetArrayType()

        self.cnf.CFind(self.server,self.remoteport,self.theQuery,self.ret_query,self.aetitle,self.caller)
        UIDs=[]
        desc=[]
        modality=[]
        for i in range(0,self.ret_query.size()):
            x=str(self.ret_query[i].GetDataElement( self.queryTag ).GetValue())
            if x in UIDs: continue
            UIDs.append(str(self.ret_query[i].GetDataElement( self.queryTag ).GetValue()))
            desc.append(str(self.ret_query[i].GetDataElement( self.tag_SeriesDescription ).GetValue()))
            modality.append(str(self.ret_query[i].GetDataElement( self.tag_SeriesModality ).GetValue()))
            
        return UIDs,desc,modality


    def downloadSeries(self,studyUID,seriesUID,outputdir,patientID=None):
            hashdir=hashlib.sha224(studyUID+"/"+seriesUID).hexdigest()
            targetdir=outputdir+"/"+hashdir+"/"

            if not os.path.exists(outputdir): os.makedirs(outputdir)
            if not os.path.exists(targetdir): os.makedirs(targetdir)
            
            self.de_infmodel = gdcm.DataElement(self.tag_infmodel)
            self.de_infmodel.SetByteValue('SERIES',gdcm.VL(len('SERIES')))
            self.de_StudyUID = gdcm.DataElement(self.tag_StudyUID)
            self.de_StudyUID.SetByteValue(studyUID,gdcm.VL(len(studyUID)))
            self.de_SeriesUID = gdcm.DataElement(self.tag_SeriesUID)
            self.de_SeriesUID.SetByteValue(seriesUID,gdcm.VL(len(seriesUID)))
            self.dataset_seriesQuery = gdcm.DataSet()        
            self.dataset_seriesQuery.Insert(self.de_StudyUID)
            self.dataset_seriesQuery.Insert(self.de_infmodel)
            self.dataset_seriesQuery.Insert(self.de_SeriesUID)

            self.cnf_series = gdcm.CompositeNetworkFunctions()
            self.theSeriesQuery = self.cnf_series.ConstructQuery (gdcm.eStudyRootType,gdcm.eSeries,self.dataset_seriesQuery)
           # Execute the C-FIND query
            self.cnf_series.CMove(self.server,self.remoteport,self.theSeriesQuery, self.localport,self.aetitle,self.caller,targetdir)
            return targetdir

fileaccessCache=dict()

class MyStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

class DicomFS(Fuse):

    def __init__(self, *args, **kw):
        Fuse.__init__(self, *args, **kw)
        self.serveraddress="localhost"
        self.serverport=104
        self.aec="AEC"
        self.aet="DICOMFS"
        self.localport=11112
        self.cachedir="/tmp/dicomfs"
        self.refresh=999999
        self.clearCaches()
        
    def createConnection(self):
        self.dicomConnection=DicomConnection(server=self.serveraddress, port=self.serverport, localport=self.localport, aetitle=self.aet, caller=self.aec)
        globaloptions['cachedir']=self.cachedir        
        globaloptions['server']=self.serveraddress
        globaloptions['remoteport']=self.serverport
        globaloptions['aet']=self.aet
        globaloptions['aec']=self.aec
        self.clearCaches()

    def clearCaches(self):
        self.directoryCache=dict()
        self.attributeCache=dict()
        self.study_mapping=dict()
        self.series_mapping=dict()       
        self.studyTimestamp=dict()
        self.patient_mapping=dict()
        self.patientroot_study_mapping=dict()
        self.patientroot_series_mapping=dict()        
        fileaccessCache=dict()       


#########################################################################

    def truncate(self, path, len):
        f = open(globaloptions['cachedir'] + path, "a")
        f.truncate(len)
        f.close()

    def mknod(self, path, mode, dev):
        os.mknod(globaloptions['cachedir'] + path, mode, dev)

    def mkdir(self, path, mode):
        os.mkdir(globaloptions['cachedir'] + path, mode)

    def fsinit(self):
        os.chdir(globaloptions['cachedir'])
        os.mkdir(globaloptions['cachedir']+"/upload")

    def chmod(self,path,mode):
        pass

#########################################################################

    def getattr(self, path):
        
        stripped_path=path.strip("/")
        
        pathparts=stripped_path.split('/')

        st = MyStat()
        st.st_nlink = 2

        if pathparts[0]=='upload':
            if len(pathparts)==1:
                st.st_mode = stat.S_IFDIR | 0770
            else:
                st=os.lstat(self.cachedir+"/"+stripped_path)

            self.attributeCache[stripped_path]=st  
            return st

        if stripped_path in self.attributeCache:
            return self.attributeCache[stripped_path]

        st.st_mode = stat.S_IFDIR | 0660
        self.attributeCache[stripped_path]=st
        if len(pathparts)==0:
            return st
        

        if os.path.exists(self.cachedir+"/"+stripped_path):
            st=os.lstat(self.cachedir+"/"+stripped_path)
            self.attributeCache[stripped_path]=st  
            return st            

        st.st_mode = stat.S_IFDIR | 0755
        st.st_nlink = 2

        self.attributeCache[stripped_path]=st
        return st

    def readdir(self, path, offset):
        path_stripped=path.strip("/")

        tmp=list()
        tmp.append('.')
        tmp.append('..')

        if path_stripped=="clear_cache":
            self.clearCaches()

        if path_stripped.startswith("upload"):
            ls=os.listdir(self.cachedir+"/"+path_stripped)
            ls.append(".")
            ls.append("..")
            tmp=list(set(ls))
            self.directoryCache[path_stripped]=tmp

        if path_stripped == '':
            tmp.extend(['upload','Patient-Study-Series-Instance','Study-Series-Instance','Study-Series-Instance_UID', 'clear_cache']) 
            self.directoryCache[path_stripped]=tmp

        if path_stripped in self.directoryCache:
            for r in self.directoryCache[path_stripped]:
                yield fuse.Direntry(r)
            return

        if path_stripped == 'Study-Series-Instance_UID':
            study,desc,timestamp=self.dicomConnection.listStudies()
            for i in range(0,len(study)):
                s=("%s - %s" % (desc[i],study[i])).replace("/","")
                tmp.append(study[i])
                self.study_mapping[s]=study[i]

        if path_stripped == 'Study-Series-Instance':
            study,desc,timestamp=self.dicomConnection.listStudies()
            for i in range(0,len(study)):
                s=("%s - %s" % (desc[i],study[i])).replace("/","")
                tmp.append(s)
                self.study_mapping[s]=study[i]
                self.studyTimestamp[study[i]]=timestamp[i]

        if path_stripped == 'Patient-Study-Series-Instance':
            patient,desc,timestamp=self.dicomConnection.listPatients()
            for i in range(0,len(patient)):
                s=("%s ID:%s" % (desc[i],patient[i])).replace("/","")
                tmp.append(s)
                self.patient_mapping[s]=patient[i]


        self.pathparts=path_stripped.split('/')

        if len(self.pathparts)==2 and self.pathparts[0] == 'Patient-Study-Series-Instance':
            if self.pathparts[1] in self.patient_mapping:
                patientID=self.patient_mapping[self.pathparts[1]]
            else:
                patientID=self.pathparts[1]
            studies,desc,modality=self.dicomConnection.listStudies(patientID=patientID)
            for i in range(0,len(studies)):
                s=("%s - %s" % (desc[i],studies[i])).replace("/","")
                tmp.append(s)
                self.patientroot_study_mapping[s]=studies[i]

        if len(self.pathparts)==3 and self.pathparts[0] == 'Patient-Study-Series-Instance':
            if self.pathparts[1] in self.patient_mapping:
                patientID=self.patient_mapping[self.pathparts[1]]
            else:
                patientID=self.pathparts[1]
                        
            if self.pathparts[2] in self.patientroot_study_mapping:
                studyUID=self.patientroot_study_mapping[self.pathparts[2]]
            else:
                studyUID=self.pathparts[2]

            series,desc,modality=self.dicomConnection.listSeries(patientID=patientID,studyUID=studyUID)
            for i in range(0,len(series)):
                s=("%s - %s - %s" % (desc[i],modality[i],series[i])).replace("/","")
                tmp.append(s)
                self.patientroot_series_mapping[s]=series[i]   

        if len(self.pathparts)==4 and (self.pathparts[0] == 'Patient-Study-Series-Instance' or self.pathparts[0] == 'Patient-Study-Series-Instance'):
            if self.pathparts[1] in self.patient_mapping:
                patientID=self.patient_mapping[self.pathparts[1]]
            else:
                patientID=self.pathparts[1]
            if self.pathparts[2] in self.patientroot_study_mapping:
                studyUID=self.patientroot_study_mapping[self.pathparts[2]]
            else:
                studyUID=self.pathparts[2]
            if self.pathparts[3] in self.patientroot_series_mapping:
                seriesUID=self.patientroot_series_mapping[self.pathparts[3]]
            else:
                seriesUID=self.pathparts[3]
            resdir=self.dicomConnection.downloadSeries(studyUID=studyUID, seriesUID=seriesUID, outputdir=self.cachedir).strip("/")
            for f in os.listdir("/"+resdir):
                st=os.lstat("/"+resdir + "/"+f)
                self.attributeCache[path_stripped+ "/"+f]=st
                fileaccessCache[path_stripped+ "/"+f]="/"+resdir + "/"+f
                tmp.append(f)
              
        if len(self.pathparts)==2 and self.pathparts[0] == 'Study-Series-Instance':
            if self.pathparts[1] in self.study_mapping:
                studyUID=self.study_mapping[self.pathparts[1]]
            else:
                studyUID=self.pathparts[1]
            series,desc,modality=self.dicomConnection.listSeries(studyUID=studyUID)
            for i in range(0,len(series)):
                s=("%s - %s - %s" % (desc[i],modality[i],series[i])).replace("/","")
                tmp.append(s)
                self.series_mapping[s]=series[i]                

        if len(self.pathparts)==2 and self.pathparts[0] == 'Study-Series-Instance_UID':
            if self.pathparts[1] in self.study_mapping:
                studyUID=self.study_mapping[self.pathparts[1]]
            else:
                studyUID=self.pathparts[1]        
            series,desc,modality=self.dicomConnection.listSeries(studyUID=studyUID)
            for i in range(0,len(series)):
                s=("%s - %s - %s" % (desc[i],modality[i],series[i])).replace("/","")
                tmp.append(series[i])
                self.series_mapping[s]=series[i]                

        if len(self.pathparts)==3 and (self.pathparts[0] == 'Study-Series-Instance_UID' or self.pathparts[0] == 'Study-Series-Instance'):
            if self.pathparts[1] in self.study_mapping:
                studyUID=self.study_mapping[self.pathparts[1]]
            else:
                studyUID=self.pathparts[1]
            if self.pathparts[2] in self.series_mapping:
                seriesUID=self.series_mapping[self.pathparts[2]]
            else:
                seriesUID=self.pathparts[1]
            resdir=self.dicomConnection.downloadSeries(studyUID=studyUID, seriesUID=seriesUID, outputdir=self.cachedir).strip("/")
            for f in os.listdir("/"+resdir):
                st=os.lstat("/"+resdir + "/"+f)
                self.attributeCache[path_stripped+ "/"+f]=st
                fileaccessCache[path_stripped+ "/"+f]="/"+resdir + "/"+f
                tmp.append(f)
            
        for name in tmp:
            yield fuse.Direntry(name)
        self.directoryCache[path_stripped]=tmp




    def main(self, *a, **kw):
        self.file_class = XmpFile
        return Fuse.main(self, *a, **kw)

def main():
    usage="""
DicomFS 

""" + Fuse.fusage
    server = DicomFS(version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle')

    server.multithreaded = False
    server.parser.add_option(mountopt="serveraddress", metavar="IP", default=server.serveraddress, help="remote DICOM server IP address [default: %default]")
    server.parser.add_option(mountopt="serverport", metavar="PORT", default=server.serverport, help="remote DICOM server port [default: %default]")
    server.parser.add_option(mountopt="aec", metavar="AETITLE", default=server.aec, help="called AE title of peer [default: %default]")
    server.parser.add_option(mountopt="aet", metavar="AETITLE", default=server.aet, help="calling AE title [default: %default]")
    server.parser.add_option(mountopt="localport", metavar="PORT", default=server.localport, help="local port [default: %default]")
    server.parser.add_option(mountopt="cachedir", metavar="PATH", default=server.cachedir, help="directory for caching [default: %default]")
    server.parser.add_option(mountopt="refresh", metavar="SEC", default=server.refresh, help="network refresh time (cache clear time) [default: %default]")

    server.parse(values=server, errex=1)
    server.createConnection()
    server.main()

if __name__ == '__main__':
    main()
