# -*- coding: utf-8 -*-
"""
Created on Mon Feb 27 13:15:05 2023

@author: coss.31
"""
#standard imports
import requests
from zipfile import ZipFile
from io import BytesIO
from numpy import genfromtxt
import numpy as np
from os import  scandir, remove
from shutil import rmtree
from datetime import datetime as dt
from datetime import date as dtd
import pandas as pd

#third party imports
from hsclient import HydroShare
#from Clara
#"https:/www.hydroshare.org/hsapi/resource/a0a51f97bd064896b91ac0e23926468e/__;!!KGKeukY!1wHawDYfAK-I7ewHZg4WfibYP8yRayvGclS54hkJz6IaPYI4PvRI4bMTxyDVGZlNGOPo3Mze3xfLzgsHWO8$"
#authenticate
UN="SteveCossSWOT"
PW="9Jn3FJNJs!!KXDj"
#RI='96f1928c95834539ba260ab65ea8db8e'
RI='38feeef698ca484b907b7b3eb84ad05b'
URLst='https://www.hydroshare.org/hsapi/resource/' + RI +'/'
DLpath='C:/Users/coss.31/OneDrive - The Ohio State University/Documents/SWOT_Mission_REPOS/HydroSharePull/resources'
DLpathL='C:/Users/coss.31/OneDrive - The Ohio State University/Documents/SWOT_Mission_REPOS/HydroSharePull/List'    
#UL.request.urlretrieve("https://www.hydroshare.org/resource/96f1928c95834539ba260ab65ea8db8e/",DLpathL)

def remove_files(DLdir):
    """Remove files found in directory.
    
    Excludes run type directories.
    
    Parameters
    ----------
    sos_dir: Path
        Path to sos directory to delete files in
    """

    with scandir(DLdir) as entries:
        HS_files = [ entry for entry in entries if entry.name ]
    
    for HS_file in HS_files: 
        rmtree(DLdir +"/" +HS_file.name)
""" clear previous pull"""
remove_files(DLpathL)
remove_files(DLpath)


r=requests.get(URLst)


r.headers.get('Content-Type')
z = ZipFile(BytesIO(r.content))    
file = z.extractall( DLpathL)
csvpath=DLpathL+"/"+RI+"/data/contents/collection_list_"+RI+".csv"
Collection= genfromtxt(csvpath, delimiter=',', dtype='unicode',skip_header=1)
#log in
hs = HydroShare(UN,PW)
#dl all resources
Sf=[]
Rid=[]
Nid=[]
x=[]
y=[]
T=[]
Q=[]
Qu=[]
D=[]
Du=[]
V=[]
Vu=[]
for resource in Collection:
#hs.sign_in()
    Tstr=resource[2]
    res = hs.resource(Tstr)
    
    NOWres=res.download(DLpath)
    z = ZipFile(DLpath+'/'+Tstr+'.zip')   
    
    file =z.extractall(DLpath)
    z.close()
    remove(DLpath+'/'+Tstr+'.zip')
    csvpath= DLpath+'/'+ Tstr + '/data/contents/'
    
    with scandir(csvpath) as entries:
        RES_files = [ entry for entry in entries if entry.name ]
    
    for RES_file in RES_files: 
        if RES_file.name[-12:-4] != 'template': 
             if RES_file.name[0:6] != 'SCoss2':
                 if RES_file.name[-12:-4] != 'OT_SHCQ1': 
        
                    RESlist= genfromtxt(csvpath+"/" +RES_file.name, delimiter=',',dtype ='unicode',skip_header=1)
                    c=1
                    for measurement in RESlist:               
                       if len(measurement[0])>0:
                            print(c)
                            c=c+1
                            Sf.append(RES_file.name)
                            Rid.append(measurement[0].astype(np.int64))
                            Nid.append(measurement[1].astype(np.int64))
                            x.append(measurement[2].astype(np.float32))
                            y.append(measurement[3].astype(np.float32))
                            date=measurement[4].strip()
                            date=date.strip("'")
                            d=dt.strptime( date, '%d-%m-%Y')
                            T.append(d.toordinal())
                            Q.append(measurement[5].astype(float))
                            Qu.append(measurement[6].astype(float))
                            D.append(measurement[7].astype(float))
                            Du.append(measurement[8].astype(float))
                            V.append(measurement[9].astype(float))
                            Vu.append(measurement[10].astype(float))


data_id=[]
data_rid=[]
data_Nid=[]
data_x=[]
data_y=[]
data_t=[]
data_q=[]
data_qu=[]
data_d=[]
data_du=[]
data_v=[]
data_vu=[]
print('Concatinate')            
Ureach=np.unique(Rid)
for reach in Ureach:
    TR=np.where(Rid==reach)
    #sort all reach data by time
    Tnp=np.array(T)
    Rtid=np.argsort(Tnp[TR],axis=None)
    
    idx=list(TR[0])
    data_id.append(np.array(Sf)[TR][0])
    data_rid.append(reach)
    data_Nid.append(np.array(Nid)[idx])
    data_x.append(np.array(x)[idx][0])
    data_y.append(np.array(y)[idx][0])
    data_t.append(np.array(T)[idx])
    data_q.append(np.array(Q)[idx])
    data_qu.append(np.array(Qu)[idx])
    data_d.append(np.array(D)[idx])
    data_du.append(np.array(Du)[idx])
    data_v.append(np.array(V)[idx])
    data_vu.append(np.array(Vu)[idx])
    
    
    
    
    
    
    
# generate empty arrays for nc output
st=dtd.fromordinal(min(T))
et=dtd.fromordinal(max(T))
ALLt=pd.date_range(start=st,end=et)
EMPTY=np.nan
MONQ=np.full((len(data_rid),12),EMPTY)
Qmean=np.full((len(data_rid)),EMPTY)
Qmin=np.full((len(data_rid)),EMPTY)
Qmax=np.full((len(data_rid)),EMPTY)
FDQS=np.full((len(data_rid),20),EMPTY)
TwoYr=np.full(len(data_rid),EMPTY)
Twrite=np.full((len(data_rid),len(ALLt)),EMPTY)
Qwrite=np.full((len(data_rid),len(ALLt)),EMPTY)
Mt=list(range(1,13))
P=list(range(1,99,5))

# process recrds for dictionary
for i in range(len(data_rid)):
            
    # pull in Q
    Q=data_q[i]
   
    if Q.empty is False:
        print(i)
        Q=Q.to_numpy()
      
        T=data_t[i].index.values        
        T=pd.DatetimeIndex(T)
        T=T[Mask]
        moy=T.month
        yyyy=T.year
        moy=moy.to_numpy()       
        thisT=np.zeros(len(T))
        for j in range((len(T))):
            thisT=np.where(ALLt==np.datetime64(T[j]))
            Qwrite[i,thisT]=Q[j]
            Twrite[i,thisT]=date.toordinal(T[j])
            # with df pulled in run some stats
            #basic stats
            Qmean[i]=np.nanmean(Q)
            Qmax[i]=np.nanmax(Q)
            Qmin[i]=np.nanmin(Q)
            #monthly means
            Tmonn={}    
            for j in range(12):
                Tmonn=np.where(moy==j+1)
                if not np.isnan(Tmonn).all() and Tmonn: 
                    MONQ[i,j]=np.nanmean(Q[Tmonn])
                    
                #flow duration curves (n=20)
                
                p=np.empty(len(Q))  
                    
                for j in range(len(Q)):
                    p[j]=100* ((j+1)/(len(Q)+1))           
                    
                    
                thisQ=np.flip(np.sort(Q))
                FDq=thisQ
                FDp=p
                FDQS[i]=np.interp(list(range(1,99,5)),FDp,FDq)
                    #FDPS=list(range(0,99,5))
                    # Two year recurrence flow
                    
                Yy=np.unique(yyyy); 
                Ymax=np.empty(len(Yy))  
                for j in range(len(Yy)):
                    Ymax[j]=np.nanmax(Q[np.where(yyyy==Yy[j])]);
                
                MAQ=np.flip(np.sort(Ymax))
                m = (len(Yy)+1)/2
                    
                TwoYr[i]=MAQ[int(np.ceil(m))-1]

        Mt=list(range(1,13))
        P=list(range(1,99,5))

HydroShare_dict = {
            "data": data,
            "reachId": reachID,            
            "Qwrite": Qwrite,
            "Twrite": Twrite,
            "Qmean": Qmean,
            "Qmax": Qmax,
            "Qmin": Qmin,
            "MONQ": MONQ,
            "Mt": Mt,
            "P": P,
            "FDQS": FDQS,
            "TwoYr": TwoYr,
            "Agency":['USGS']*len(dataUSGS)
        }