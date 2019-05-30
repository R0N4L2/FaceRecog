import pandas
import sys 
import os
import numpy
import datetime
if sys.platform.startswith('win'):
	sys.path.append(os.path.relpath("..\\utilities"))
else:
	sys.path.append(os.path.relpath("../utilities"))
from util import executeQuery,writeFile
if __name__ == "__main__":
	qry="select d.fecha,d.codigoubicacion,d.codigoarticulo,d.ubicacion,d.codigounidadmanejo,d.cantidad,\
	d.tipopedido,d.codigoareatrabajo,TRANSLATE(b.nombrearticulo,'AEIOUaeiouNn_','¡…Õ”⁄·ÈÌÛ˙—Ò ') as nombrearticulo,\
	b.volumen,b.peso,b.densidad,b.resistencia,b.contaminante,a.coordenadaxlocal,a.coordenadaylocal,a.coordenadaxglobal,\
	a.coordenadayglobal from SMXSIC.SLPEAVPEDIDO d INNER JOIN SMXSIC.SCARTVINFART b ON d.CODIGOARTICULO=b.CODIGOARTICULO \
	AND d.CODIGOAREATRABAJOSUBBODEGA=b.CODIGOAREATRABAJOSUBBODEGA LEFT JOIN SMXSIC.SBLOGVUBIART A ON d.CODIGOARTICULO=A.CODIGOARTICULO \
	AND d.CODIGOUBICACION=A.CODIGOUBICACION and b.codigounidadmanejo=a.codigounidadmanejo WHERE d.CODIGOAREATRABAJOBODEGA=10 AND d.FECHA ='2019-04-3'"
	df=executeQuery(qry)
	insert1="INSERT INTO SMXSIC.SBLOGTBITTARARETRA (CODIGOCOMPANIA,CODIGOAREATRABAJO,CODTARARETRA,CODIGOARTICULO,CODIGOUNIDADMANEJO,\
	CODIGOUBICACION,CANTIDAD,DETALLEERROR,IDUSUARIOREGISTRO,FECHAREGISTRO,IDUSUARIOMODIFICACION,FECHAMODIFICACION) VALUES (1, NEXT VALUE FOR SBLOGTBITTARARETRA,\
	NEXT VALUE FOR SBLOGTBITTARARETRA,"
	insert2=",'FRM0',CURRENT TIMESTAMP,'FRM0',CURRENT TIMESTAMP)"
	df=df.replace(to_replace=[None],value=numpy.nan)
	col=df.columns.values
	df1=df[df.notna().values.all(1)]
	writeFile(df1[df1['DENSIDAD']<=2500],"Verificados.pickle")
	dfna=df[df.isna().values.any(1)].astype(str)
	dfDA=df1[df1['DENSIDAD']>2500].astype(str) 
	insert=insert1+dfDA['CODIGOARTICULO']+","+dfDA['CODIGOUNIDADMANEJO']+","+dfDA['CODIGOUBICACION']+",1,'La densidad es mayor que 2500'"+insert2
	dfc=pandas.Series((dfna.isna()*col)[col[dfna.isna().values.any(0)]].values.tolist()).map(lambda x: list(set(x)-{''})).str.join(",")
	dfna=dfna.fillna("").reset_index(drop=True)
	insert=pandas.concat([insert,insert1+dfna['CODIGOARTICULO']+","+dfna['CODIGOUNIDADMANEJO']+","+dfna['CODIGOUBICACION']+",1,'Las columnas "+dfc+"estan vacias'"+insert2]).reset_index(drop=True)
	writeFile(insert,"Error_insert_in_SBLOGTBITTARARETRA.xlsx")