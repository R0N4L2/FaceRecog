import urllib3
import json
import datetime
from bs4 import BeautifulSoup
import os
import gc
import pandas as pd
import numpy as np
import shutil
from os.path import dirname, abspath, join, exists
"""SE CONECTA A IMDB PARA OBTENER FOTOS DE VARIOS ARTISTAS POR NOMBRE Y GENERO"""
GENERO=["female","male"]
http = urllib3.PoolManager()
di = join(dirname(abspath('')),'imagenes\\')
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
    for k in range(int(np.ceil(cant/50))):
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
            filename=di1+A[1].iloc[k].replace(" ","_")+'.jpg'
            url=A[3].iloc[k]
            with http.request('GET',url, preload_content=False) as resp, open(filename, 'wb') as out_file:
                shutil.copyfileobj(resp, out_file)
            resp.release_conn()