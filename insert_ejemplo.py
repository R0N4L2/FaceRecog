import pandas
import sys 
import os
import numpy

if sys.platform.startswith('win'):
	sys.path.append(os.path.relpath("..\\utilities"))
else:
	sys.path.append(os.path.relpath("../utilities"))
from util import executeQuery,writeFile

if __name__ == "__main__":
	df=executeQuery("select a.codigoubicacion,b.codigoarticulo,b.codigounidadmanejo,TRANSLATE(b.nombrearticulo,'AEIOUaeiouNn_','ÁÉÍÓÚáéíóúÑñ ') as  nombrearticulo,b.alto,b.ancho,b.largo,b.volumen,b.peso,b.densidad,b.resistencia,b.contaminante,c.nave,c.pasillo,c.rack,a.coordenadaxlocal,a.coordenadaylocal,a.coordenadaxglobal,a.coordenadayglobal from SMXSIC.sblogvubiart a, SMXSIC.SCARTVINFART b,SMXSIC.SBLOGVUBICACIONES c where  c.codigoubicacion=a.codigoubicacion and a.codigoarticulo=b.codigoarticulo and b.codigounidadmanejo=a.codigounidadmanejo and b.codigoareatrabajosubbodega=c.codigoareatrabajosubbodega")
	insert1="INSERT INTO SMXSIC.SBLOGTBITTARARETRA (CODIGOCOMPANIA,CODIGOAREATRABAJO,CODTARARETRA,CODIGOARTICULO,CODIGOUNIDADMANEJO,CODIGOUBICACION,CANTIDAD,DETALLEERROR,IDUSUARIOREGISTRO,FECHAREGISTRO, IDUSUARIOMODIFICACION,FECHAMODIFICACION) VALUES (1, NEXT VALUE FOR SBLOGTBITTARARETRA, NEXT VALUE FOR SBLOGTBITTARARETRA,"
	insert2="estan vacias','FRM0',CURRENT TIMESTAMP,'FRM0',CURRENT TIMESTAMP)"
	df=df.fillna(value=numpy.nan)
	df1=[]
	col=df.columns.values
	insert=[]
	for k in range(df.shape[0]):
		a=df.iloc[k].isnull().values
		if a.any():
			insert+=[insert1+str(df['CODIGOARTICULO'].iloc[k])+","+str(df['CODIGOUNIDADMANEJO'].iloc[k])+","+str(df['CODIGOUBICACION'].iloc[k])+",1,'Las columnas "+",".join(col[a].tolist())+insert2]
		else:
			df1+=[df.iloc[k].values.tolist()]
	if len(df1)>0:
		df1=pandas.DataFrame(df1)
		old_names=df1.columns.tolist()
		new_names=df.columns.tolist()
		df1.rename(columns=dict(zip(old_names, new_names)),inplace=True)
		writeFile(df1,"Verificados.pickle")
	if len(insert)>0:
		insert=pandas.DataFrame(insert)
		writeFile(insert,"Error_insert_in_SBLOGTBITTARARETRA.xlsx")