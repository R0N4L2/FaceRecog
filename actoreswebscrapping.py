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
siz=1
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
        A=A.drop(columns=list(set(A.columns.tolist())-{1,3}))
        A[1]=A[1].str.split('"',expand=True)[1]
        A[3]=A[3].str.split('"',expand=True)[1]
        A=A.reset_index(drop=True)
        for k in range(A.shape[0]):
            filename=di1+A[1].iloc[k].replace(" ","_")
            url=A[3].iloc[k]
            if url.find("nopicture")<0:
                n=int(url.find("nopicture")<0)
                with http.request('GET',url, preload_content=False) as resp, open(filename+'.jpg', 'wb') as out_file:
                    shutil.copyfileobj(resp, out_file)
                resp.release_conn()
            response =http.request('GET',wiki+A[1].iloc[k].replace(" ","_"))
            soup = BeautifulSoup(response.data)
            dt=pd.Series(soup.find_all("tbody"))
            s2=BeautifulSoup(str(dt[0]))
            im2=[]
            if len(s2.find_all("img"))>0:
                n+=1
                aa1=pd.Series(str(s2.find_all("img")[0]).split('"'))
                ss=aa1.str.find(".jpg")
                aa1=aa1[ss>0].reset_index(drop=True)
                ss=aa1.str.find("wikipedia")
                aa1=aa1[ss>0].reset_index(drop=True)
                if any(aa1.str.find(",")>0):
                    aa1=aa1.str.split(',',expand=True)
                    aa1=aa1.values.reshape(aa1.size)
                    aa1=pd.Series(list(set(aa1.tolist())-{None})).str.split(' ',expand=True)
                    aa1=pd.Series(list(set(aa1.values.reshape(aa1.size).tolist())-{None}))
                    ss=aa1.str.find(".jpg/")
                    aa1='https:'+pd.unique(aa1[ss>0].str.split('.jpg/',expand=True)[0])+'.jpg'
                    for g,h in enumerate(aa1):                
                        im2+=[g+n]
                        with http.request('GET',h, preload_content=False) as resp, open(filename+'_'+str(g+n)+'.jpg', 'wb') as out_file:
                            shutil.copyfileobj(resp, out_file)
                        resp.release_conn()
            #Photography of last year
            response =http.request('GET',"https://www.gettyimages.es/fotos/{}?compositions=headshot&family=editorial&numberofpeople=one&recency=last12months&sort=best".format(A[1].iloc[k].lower().replace(" ","_")))
            soup = BeautifulSoup(response.data)
            s3=soup.find_all("img")
            ultima=False
            if s3>0:
                S=pd.Series(s3).astype(str)
                P=S.str.find(A[1].iloc[k])
                if any(P>0):
                    n+=1
                    ultima=True
                    S=S[P>0].reset_index(drop=True)
                    S=S.str.split('"',expand=True)[5]
                    if siz<S.shape[0]:
                        aa=np.random.randint(S.shape[0], size=siz).tolist()
                    else:
                        aa=range(S.shape[0])
                    for g,h in enumerate(S.iloc[aa]):
                        with http.request('GET',h, preload_content=False) as resp, open(filename+'_lastyear_'+str(g)+'.jpg', 'wb') as out_file:
                            shutil.copyfileobj(resp, out_file)
                        resp.release_conn()
            if n>0:
                name=A[1].iloc[k]
                bday=str(s2.find_all("span")[1]).split('"bday">')[1].split("</span>")[0]
                old=humanfriendly.format_timespan(datetime.datetime.today()-datetime.datetime.strptime(bday, '%Y-%m-%d'),max_units=1)
                old=int(old.split(" ")[0])
                dic={'Nombre':[name],"genero":[gen],"Nacimiento":[bday],"Edad":[old]}
                if url.find("nopicture")<0:
                    dic.update({"img1":[filename+'.jpg']})
                if len(img2)>0:
                    for g in img2:
                        dic.update({"img"+str(k):[filename+'_'+str(g)+'.jpg']})
                if ultima:
                    for g in range(len(aa)):
                        dic.update({"img_lastyear"+str(g):[filename+'_lastyear_'+str(g)+'.jpg']})
                S=pd.DataFrame.from_dict(dic)
                if start:
                    DFR=S
                    start=False
                else:
                    DFR=DFR.append(S)                
writePickle(DFR,di+'FaceRecognition.pickle')