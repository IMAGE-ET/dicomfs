#!/usr/bin/env python
#
#
#    This program can be distributed under the terms of the GNU GPL v3.
#

# to be cited: Distributed PACS using Network Shared File System


import os, stat, errno, commands, sys, re,time,threading
from errno import *
from stat import *
import fuse,fcntl
from fuse import Fuse
import gdcm
import base64
import hashlib
import datetime

if gdcm.Version.GetMajorVersion()<2 or (gdcm.Version.GetMajorVersion()==2 and gdcm.Version.GetMinorVersion()<2):
    print
    print "DicomFS requires version 2.2 or higher from libgdcm/python-gdcm."
    print "Current version: %s" % gdcm.Version.GetVersion()
    sys.exit(-1)

if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)


def flag2mode(flags):
    md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)
        pass
    return m

class XmpFile(object):

        def __init__(self, path, flags, *mode):
            self.filename=fileaccessCache[path.strip("/")]
            self.file = os.fdopen(os.open(self.filename, flags, *mode),flag2mode(flags))
            self.fd = self.file.fileno()
            self.upload = False
            self.direct_io=0
            self.keep_cache=1            

        def read(self, length, offset):
            self.file.seek(offset)
            return self.file.read(length)

        def write(self, buf, offset):
            self.file.seek(offset)
            self.file.write(buf)

            return len(buf)

        def release(self, flags):
            self.file.close()
#            if self.upload: uploadFile(self,self.cachedir+path)
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
    remoteport=1040
    localport=11112
    aetitle="DICOMFS_AE"
    caller="DICOMFS_CALLER"
    
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

        self.tag_patientname = gdcm.Tag(0x10,0x10)

    def ping(self):
        self.pingStart=time.time()
        self.pingSuccess=gdcm.CompositeNetworkFunctions.CEcho(self.server,self.remoteport,self.aetitle,self.caller)
        self.pingStop=time.time()
        if self.pingSuccess==False: return -1
        return self.pingStop-self.pingStart


    def StudyRoot_listStudies(self):  # root query

        self.d=DicomDataset()
        self.queryTag=self.tag_StudyUID
        self.d.add(self.queryTag)
        for tag in self.tag_Study:
            self.d.add(tag)
        
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
        

    def StudyRoot_listSeries(self,studyUID):  # root query
        self.d=DicomDataset()
        self.queryTag=self.tag_SeriesUID

        self.d.add(self.queryTag)
        self.d.add(self.tag_StudyUID,studyUID)
        self.d.add(self.tag_infmodel,"SERIES")

        for tag in self.tag_Study:
            self.d.add(tag)
        for tag in self.tag_Series:
            self.d.add(tag)

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


    def StudyRoot_downloadSeries(self,studyUID,seriesUID,outputdir):
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

    def createConnection(self):
        self.dicomConnection=DicomConnection(server=self.serveraddress, port=self.serverport, localport=self.localport, aetitle=self.aet, caller=self.aec)
        self.clearCaches()

    def clearCaches(self):
        self.directoryCache=dict()
        self.attributeCache=dict()
        self.study_mapping=dict()
        self.series_mapping=dict()       
        self.studyTimestamp=dict()
        fileaccessCache=dict()       

    def empty(self):
        pass

    def runOnce(self):
        self.clearCaches()
        self.runOnce=self.empty

    def getattr(self, path):
        self.runOnce()
        self.path=path.strip("/")
        self.pathparts=path.split('/')
        if self.path in self.attributeCache:
            return self.attributeCache[self.path]

        st = MyStat()

        if len(self.pathparts)<=4:
            st.st_mode = stat.S_IFDIR | 0755
            st.st_nlink = 2
          
        self.attributeCache[self.path]=st
        return self.attributeCache[self.path]

    def readdir(self, path, offset):
        path=path.strip("/")

        if path=="clear_cache":
            self.clearCaches()

        if path in self.directoryCache:
            for r in self.directoryCache[path]:
                yield fuse.Direntry(r)
            return

        path_stripped=path.strip()

        tmp=list()
        tmp.append('.')
        tmp.append('..')

        if path == '':
            tmp.extend(['Study-Series-Instance','Study-Series-Instance_UID', 'clear_cache']) # 'Patient-Study-Modality-Series-Instance'

        if path_stripped == 'Patient-Study-Modality-Series-Instance':
            pass

        if path_stripped == 'Study-Series-Instance_UID':
            study,desc,timestamp=self.dicomConnection.StudyRoot_listStudies()
            for i in range(0,len(study)):
                s=("%s - %s" % (desc[i],study[i])).replace("/","")
                tmp.append(study[i])
                self.study_mapping[s]=study[i]

        if path_stripped == 'Study-Series-Instance':
            study,desc,timestamp=self.dicomConnection.StudyRoot_listStudies()
            for i in range(0,len(study)):
                s=("%s - %s" % (desc[i],study[i])).replace("/","")
                tmp.append(s)
                self.study_mapping[s]=study[i]
                self.studyTimestamp[study[i]]=timestamp[i]

        self.pathparts=path.split('/')
              
        if len(self.pathparts)==2 and self.pathparts[0] == 'Study-Series-Instance':
            if self.pathparts[1] in self.study_mapping:
                studyUID=self.study_mapping[self.pathparts[1]]
            else:
                studyUID=self.pathparts[1]
            series,desc,modality=self.dicomConnection.StudyRoot_listSeries(studyUID=studyUID)
            for i in range(0,len(series)):
                s=("%s - %s - %s" % (desc[i],modality[i],series[i])).replace("/","")
                tmp.append(s)
                self.series_mapping[s]=series[i]                

        if len(self.pathparts)==2 and self.pathparts[0] == 'Study-Series-Instance_UID':
            if self.pathparts[1] in self.study_mapping:
                studyUID=self.study_mapping[self.pathparts[1]]
            else:
                studyUID=self.pathparts[1]        
            series,desc,modality=self.dicomConnection.StudyRoot_listSeries(studyUID=studyUID)
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
            resdir=self.dicomConnection.StudyRoot_downloadSeries(studyUID=studyUID, seriesUID=seriesUID, outputdir=self.cachedir).strip("/")
            for f in os.listdir("/"+resdir):
                st=os.lstat("/"+resdir + "/"+f)
                self.attributeCache[path_stripped+ "/"+f]=st
                fileaccessCache[path_stripped+ "/"+f]="/"+resdir + "/"+f
                tmp.append(f)
            
        for name in tmp:
            yield fuse.Direntry(name)
        self.directoryCache[path]=tmp

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

#print "/a/b/".strip("/").split("/")
if __name__ == '__main__':
    main()
