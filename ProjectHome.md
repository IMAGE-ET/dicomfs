# FUSE Filesystem for DICOM server access #

Feedback:
The development in an early phase, so feedbacks and advices are appreciated.

Features:
  * access DICOM server as a filesystem
  * Patient root / Study root information model
  * DICOM UIDs/IDs appear as directory (e.g. patient name/study/series/ )
  * upload files to server

Currently it works on Debian Unstable (Sid) or on Ubuntu 12.04. On Ubuntu 12.10 it should work with the Ubuntu 12.04 packages, but it's not tested.
(If you use backported packages from Sid. See the **Install instructions** and the **Downloads** sections.)

Main used libraries:
  * gdcm 2.2, python-gdcm
  * python-fuse

DICOMFS is tested with: http://www.dicomserver.co.uk/

## Install instructions ##

### Debian SID ###
Install these packages: python-gdcm, python-fuse

**`sudo apt-get install python-gdcm python-fuse`**

Than install the deb package:

**`sudo dpkg -i dicomfs*deb`**

### Ubuntu 12.04 or 11.10 ###

Download the appropriate zip file for your architecture.

Install these packages: python-fuse

**`sudo apt-get install python-fuse libcharls1 libexpat1 libopenjpeg2 libssl1.0.0 libuuid1 zlib1g python2.7`**

Than install the deb files from the zip file:

**`sudo dpkg -i *deb`**

### Other systems ###

DicomFS should work everywhere where python-fuse and python-gdcm > 2.2 are present.

Install these files, than download the source from svn and try it.