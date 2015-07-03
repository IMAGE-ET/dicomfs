# DICOMFS Introduction #


## Example ##

dicomfs.py /mnt/mountpoint -o serveraddress=localhost,serverport=1040,localport=11112 aec=aec,aet=aet,cachedir=/tmp/dcm-cache

Explanation:
  * /mnt/mountpoint  is the mount point
  * serveraddress = the name or IP address of the DICOM server, e.g.  www.dicomserver.co.uk or localhost
  * serverport = the port where the server listens for incoming connections. Usually it's 104 or 11112.
  * localport = the port where the client (dicomfs) will listen. For ports below 1024 you usually need root priviledge.
  * cachedir = the directory where transferred files will be cached
  * aec = AEC, check the DICOM documentation
  * aet = AET, check the DICOM documentation
Test servers like www.dicomserver.co.uk usually accept everything for AEC and AET. Otherwise you have to set these values according to the PACS settings.

### Port forwaring on Linux ###

It's hard to reach DICOM server if you are behind a firewall, because the client must open a port to receive images.

Let's imagine a network topology like this:

Client - Firewall (router, etc.) - Relay computer - DICOM server

You logged in to your client computer (localhost).
You have to create an SSH tunnel to the relay computer (relay.org)
All your request to the _local_ _port_ of the localhost will be forwarded to the _remote_ _port_ of the target server (target.com) with the help of the relay server.

ssh -L 1040:target.com:11112 user@relay.org -g

Explanation:
  * 1040 is the local port
  * target.com is the target server
  * the remote port on the target server is 11112
  * all the traffic will go through the relay.org, so the target server thinks that is the origin of the communication.

Well, that is the half of the problem. The target server also wants to access our local client. We need an open port which is accessible. Let's open a port on the relay server, and forward all incoming traffic of this port to the local client.

Explanation:
  * 11112 is the open port on the relay server
  * 11113 is the local port on the local client
  * traffic is forwarded by the relay.org server

ssh -R relay.org:11112:localhost:11112

Putting together the two port forwards:

ssh -R relay.org:11112:localhost:11112 -L 1040:www.dicomserver.co.uk:11112 user@relay.org -g
