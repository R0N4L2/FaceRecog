import urllib3
import json
import datetime
import humanfriendly
from datetime import timedelta
from bs4 import BeautifulSoup
import os
import gc
import pandas as pd
import numpy as np
import shutil
from os.path import dirname, abspath, join, exists
import pickle

def writePickle(df,fileName):
	with open(os.path.join(relPathToDatos,fileName), 'wb') as f:
		pickle.dump(df, f, pickle.HIGHEST_PROTOCOL)

"""SE CONECTA A IMDB PARA OBTENER FOTOS DE VARIOS ARTISTAS POR NOMBRE Y GENERO"""
GENERO=["female","male"]
http = urllib3.PoolManager()
di = join(dirname(abspath('')),'imagenes\\')
wiki="https://en.wikipedia.org/wiki/"
start=True
if not exists(di):
    os.mkdir(di)
for gen in GENERO:
    urlFoLaction = "https://www.imdb.com/search/name?gender="+gen+"&start=1&ref_=rlm"
    response = http.request('GET',urlFoLaction)
    soup = BeautifulSoup(response.data)
    s=pd.Series(soup.find_all("span"))
    cant=int(str(s[19]).split(' ')[2].replace(",",""))
    if not exists(join(di,gen+'\\')):
        os.mkdir(join(di,gen+'\\'))
    di1=join(di,gen+'\\')
    for k in range(0,min(cant,10000),50):
        urlFoLaction = "https://www.imdb.com/search/name?gender="+gen+"&start="+str(1+k)+"&ref_=rlm"
        response = http.request('GET',urlFoLaction)
        soup = BeautifulSoup(response.data)
        s=pd.Series(soup.find_all("img"))
        A=s[4:-1].astype(str).str.split("=",expand=True)
        A=A[[1,3]]
        A[1]=A[1].str.split('"',expand=True)[1]
        A[3]=A[3].str.split('"',expand=True)[1]
        A=A.reset_index(drop=True)
        for k in range(A.shape[0]):
            filename=di1+A[1].iloc[k].replace(" ","_")
            url=A[3].iloc[k]
            n=int(url.find("nopicture")>0)
            response =http.request('GET',wiki+A[1].iloc[k].replace(" ","_"))
            soup = BeautifulSoup(response.data)
            dt=pd.Series(soup.find_all("tbody"))
            s2=BeautifulSoup(str(dt[0]))
            if len(s2.find_all("img"))>0:
                n+=1
                img2="https:"+str(s2.find_all("img")[0]).split(" ")[-3]
                with http.request('GET',img2, preload_content=False) as resp, open(filename+'_'+str(n)+'.jpg', 'wb') as out_file:
                    shutil.copyfileobj(resp, out_file)
                resp.release_conn()
            if n>0:
                name=A[1].iloc[k]
                bday=str(s2.find_all("span")[1]).split('"bday">')[1].split("</span>")[0]
                old=humanfriendly.format_timespan(datetime.datetime.today()-datetime.datetime.strptime(bday, '%Y-%m-%d'),max_units=1)
                old=int(old.split(" ")[0])
                dic={'Nombre':[name],"genero":[gen],"Nacimiento":[bday],"Edad":[old]}
                if url.find("nopicture")>0:
                    dic.update({"img1":[filename+'.jpg']})
                if len(s2.find_all("img"))>0:
                    dic.update({"img2":[filename+'_'+str(n)+'.jpg']})
                S=pd.DataFrame.from_dict(dic)
                if start:
                    DFR=S
                    start=False
                else:
                    DFR=DFR.append(S)
                with http.request('GET',url, preload_content=False) as resp, open(filename+'.jpg', 'wb') as out_file:
                    shutil.copyfileobj(resp, out_file)
                resp.release_conn()
writePickle(DFR,di+'FaceRecognition.pickle')