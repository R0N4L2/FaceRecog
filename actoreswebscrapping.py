import urllib3
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
import unidecode
import time
try:
    from urllib.request import urlretrieve  # Python 3
except ImportError:
    from urllib import urlretrieve  # Python 2

def writePickle(df,file_name):
	with open(file_name, 'wb') as f:
		pickle.dump(df, f, pickle.HIGHEST_PROTOCOL)
def web4photo(web,file_name):
    time.sleep(1)
    try: 
        urlretrieve(web,file_name)
    except:
        time.sleep(1)
        http = urllib3.PoolManager()
        with http.request('GET',web, preload_content=False) as resp, open(file_name, 'wb') as out_file:
            shutil.copyfileobj(resp, out_file)
        resp.release_conn()
def writeCSV(df,file_name,useIndex = False):
	df.to_csv(file_name,index=useIndex)	
def writeExcel(df,file_name,useIndex = False):
	df.to_excel(file_name,index=useIndex)
def writeFile(df,file_name,useIndex = False):
	if file_name.endswith('.csv'):
		writeCSV(df,file_name,useIndex = useIndex)
	if file_name.endswith('.xlsx'):
		writeExcel(df,file_name,useIndex = useIndex)
	if file_name.endsWith('.pickle'):
		writePickle(df,file_name)   
def readFile(file_name):
	if file_name.endswith('.csv'):
		return pandas.read_csv(file_name)
	if file_name.endswith('.xlsx'):
		return pandas.read_excel(file_name)
	if file_name.endsWith('.pickle'):
		with open(file_name, 'rb') as f:
			return pickle.load(f)

def changeFormat(x):
    return [x.lower().replace(" ","_"),x.replace(" ","%20"),x.lower().replace(" ","%20"),x.replace(" ","_"),x.replace(" ",""),x.lower().replace(" ",""),x,x.lower()]   
"""SE CONECTA A IMDB PARA OBTENER FOTOS DE VARIOS ARTISTAS POR NOMBRE Y GENERO"""
GENERO=["female","male"]
http = urllib3.PoolManager()
di = join(dirname(abspath('')),'imagenes\\')
url_wiki="https://en.wikipedia.org/wiki/"
start=True
print('Ingrese la cantidad de fotos que quiere rescatar:')
photos_totales=int(input())
if not exists(di):
    os.mkdir(di)
for gen in GENERO:
    url_imdb = "https://www.imdb.com/search/name?gender="+gen+"&start=1&ref_=rlm"
    response = http.request('GET',url_imdb)
    soup = BeautifulSoup(response.data)
    s=pd.Series(soup.find_all("span"))
    actores_totales=int(str(s[19]).split(' ')[2].replace(",",""))
    if not exists(join(di,gen+'\\')):
        os.mkdir(join(di,gen+'\\'))
    di1=join(di,gen+'\\')
    for j in range(0,min(actores_totales,10000),50):
        url_imdb = "https://www.imdb.com/search/name?gender="+gen+"&start="+str(1+j)+"&ref_=rlm"
        response = http.request('GET',url_imdb)
        soup = BeautifulSoup(response.data)
        s=pd.Series(soup.find_all("img"))
        A=s[4:-1].astype(str).str.split("=",expand=True)
        A=A.drop(columns=list(set(A.columns.tolist())-{1,3}))
        A[1]=A[1].str.split('"',expand=True)[1]
        A[3]=A[3].str.split('"',expand=True)[1]
        A=A.reset_index(drop=True)
        for k in range(A.shape[0]):
            name=unidecode.unidecode(A[1].iloc[k])
            file_name=di1+name.replace(" ","_")
            url_photo=A[3].iloc[k]
            if url_photo.find("nopicture")<0:
                n=int(url_photo.find("nopicture")<0)
                web4photo(url_photo,file_name+'.jpg')
            response =http.request('GET',url_wiki+name.replace(" ","_"))
            soup = BeautifulSoup(response.data)
            dt=pd.Series(soup.find_all("tbody"))
            if any(dt.astype(str).str.find("bday")>0):
                s2=BeautifulSoup(dt[dt.astype(str).str.find("bday")>0].astype(str).iloc[0])
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
                        if len(aa1)>photos_totales:
                            ss=np.random.permutation(len(aa1)).tolist()[:photos_totales]
                            aa1=aa1[ss]                        
                        for g,h in enumerate(aa1):                
                            im2+=[g+n]
                            web4photo(h,file_name+'_'+str(g+n)+'.jpg')
                #Photography of last year
                url_gettyimages="https://www.gettyimages.com/photos/{n[0]}?compositions=headshot&family=editorial&numberofpeople=one&phrase={n[1]}&recency=last12months&sort=newest".format(n=changeFormat(name))
                response =http.request('GET',url_gettyimages)
                soup = BeautifulSoup(response.data)
                s3=soup.find_all("img")
                ultima=False
                if len(s3)>0:
                    S=pd.Series(s3).astype(str)
                    Q=[S.str.find(j).values>0 for j in changeFormat(name)]
                    if np.any(Q):
                        n+=1
                        ultima=True
                        S=S[np.any(Q,axis=0)].reset_index(drop=True)
                        S=S.str.split('"',expand=True)[5]
                        if photos_totales<S.shape[0]:
                            aa=np.random.permutation(S.shape[0]).tolist()[:photos_totales]
                        else:
                            aa=range(S.shape[0])
                        for g,h in enumerate(S.iloc[aa]):
                            web4photo(h,file_name+'_lastyear_'+str(g)+'.jpg')
                if n>0:
                    nombre=A[1].iloc[k]
                    s2=pd.Series(s2.find_all("span")).astype(str)
                    bday=s2[s2.str.find("bday")>0].iloc[0].split('"bday">')[1].split("</span>")[0]
                    edad=humanfriendly.format_timespan(datetime.datetime.today()-datetime.datetime.strptime(bday, '%Y-%m-%d'),max_units=1)
                    edad=int(edad.split(" ")[0])
                    dic={'Nombre':[nombre],"genero":[gen],"Nacimiento":[bday],"Edad":[edad]}
                    if url_photo.find("nopicture")<0:
                        dic.update({"img1":[file_name+'.jpg']})
                    if len(im2)>0:
                        for g in im2:
                            dic.update({"img"+str(k):[file_name+'_'+str(g)+'.jpg']})
                    if ultima:
                        for g in range(len(aa)):
                            dic.update({"img_lastyear"+str(g):[file_name+'_lastyear_'+str(g)+'.jpg']})
                    S=pd.DataFrame.from_dict(dic)
                    if start:
                        DFR=S
                        start=False
                    else:
                        DFR=DFR.append(S)                
writeFile(DFR,di+'FaceRecognition.pickle')