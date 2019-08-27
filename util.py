import pandas
import os
import sys
import pickle
import pyodbc
import numpy
import progressbar
import gc
import regex
import datetime

relPathToDatos = "..\\Datos\\"
if not sys.platform.startswith('win'):
	relPathToDatos = "../Datos/"

def readFile(fileName,folder=relPathToDatos):
#Lee un archivo y lo transforma a un DataFrame
	if fileName in os.listdir(folder):
		if fileName.endswith('.csv'):
			df=pandas.read_csv(os.path.join(folder,fileName),dtype={'CODIGO BARRAS': str,'UBICACION': str},encoding='cp1252')
		elif fileName.endswith('.xlsx'):
			df=pandas.read_excel(os.path.join(folder,fileName),dtype={'CODIGO BARRAS': str,'UBICACION': str},encoding='cp1252')
		elif fileName.endswith('.pickle'):
			with open(os.path.join(folder,fileName), 'rb') as f:
				df=pickle.load(f)
	else:
		df=None
		print("The file "+fileName+" could not be found")
	return df

def writeCSV(df,fileName,folder=relPathToDatos,useIndex = False):
#Escribe un archivo csv desde un DataFrame
	df.to_csv(os.path.join(folder,fileName),index=useIndex)
	
def writeExcel(df,fileName,folder=relPathToDatos,useIndex = False):
#Escribe un archivo xlsx desde un DataFrame
	df.to_excel(os.path.join(folder,fileName),index=useIndex)
	
def writePickle(df,fileName,folder=relPathToDatos):
#Escribe un archivo pickle desde un DataFrame
	with open(os.path.join(folder,fileName), 'wb') as f:
		pickle.dump(df, f, pickle.HIGHEST_PROTOCOL)
		
def writeFile(df,fileName,Folder=relPathToDatos,useIndex = False):
#Escribe un archivo desde un DataFrame
	if fileName.endswith('.csv'):
		writeCSV(df,fileName,folder=Folder,useIndex = useIndex)
	if fileName.endswith('.xlsx'):
		writeExcel(df,fileName,folder=Folder,useIndex = useIndex)
	if fileName.endswith('.pickle'):
		writePickle(df,fileName,folder=Folder)

def createConnection(typcon="DB2"):
#Crea un a coneccion de Sql
	if typcon=="DB2":
		connection = pyodbc.connect(driver='{IBM i Access ODBC Driver 64-bit}',system='10.200.20.49',uid='ISMX1AYCD',database='SMXSIC',pwd='Ej9Gtp75')
	else:
		connection = pyodbc.connect(driver='{SQL Server}',server='10.200.20.49',uid='ISMX1AYCD', database='SMXSIC',pwd='Ej9Gtp75')
	return connection

def executeQuery(query,typconn="DB2"):
#Ejecuta una query de sql y la exporta como un DataFrame
	query=regex.sub(" +"," ",query)
	try:
		con = createConnection(typconn)
		cant = pandas.read_sql_query("select count(*) "+query[query.find("from"):(query.find(" order")*(query.find("order")>=0)+len(query)*(query.find("order")<0))],con).astype(int).values[0][0]
		if cant<50000:
			df=pandas.read_sql_query(query,con)
		else:
			bar = progressbar.ProgressBar(max_value=cant)
			try:
				j=0
				df=pandas.DataFrame()
				for df_scratch in pandas.read_sql_query(query,con,chunksize=50000):
					df=pandas.concat([df,df_scratch])
					j+=df_scratch.shape[0]
					bar.update(j)
					gc.collect()
			except:
				p=int(numpy.log10(cant))
				k=10**p
				j=0
				df_scratch=[]
				while j<cant:
					try:
						df_scratch+=pandas.read_sql_query(query+" limit "+str(j)+","+str(min(k,cant-j)),con).values.tolist()
						j+=k
						bar.update(j)
						p=min(p,int(numpy.log10(max(cant-j,1))))+1
					except:
						p=max(0,p-1)
					k=min(cant-j,10**p)
					gc.collect()
				df=pandas.DataFrame(df_scratch,columns=pandas.read_sql_query(query+" limit 0,1",con).columns.tolist())
		con.close()
		if len(df) == 0:
			print("size of data is :"+str(len(df)))
			return None
		else:
			return df
	except Exception as e:
		print(e)
		return None

def CheckVerificados(fileName,errores=False,ext=".pickle"):
#crea un archivo de verificados y un archivo de errores desde un DataFrame o un archivo
	df=checkFileorDF2DF(fileName)
	fileName=fileName.split(".")[0]
	df=df.replace(to_replace=[None],value=numpy.nan).drop_duplicates().reset_index(drop=True)
	df1=df[df.notna().values.all(1)]
	df2=df1[(df1['DENSIDAD']<=2500)&(df1['VOLUMEN']<=2.5)&(df1['VOLUMEN']>0)&(df1['PESO']<=1000)&((df1['UBICACION'].str.split(" ",expand=True).applymap(lambda x:len(x)*str.isnumeric(x))*[100,10,1]).sum(1)==343)]
	cd=df2['CODIGODESPACHO'].astype(int).drop_duplicates()
	ca=df2['CODIGOARTICULO'].astype(int).drop_duplicates()
	qry="select codigoarticulo,CODIGODESPACHO from SMXSIC.SLPEAVPEDIDO where codigoarticulo in ('"+"','".join(ca.astype(str).values.tolist())+"') and CODIGODESPACHO in ("+",".join(cd.astype(str).values.tolist())+")"
	dfq=executeQuery(qry)
	df3=pandas.merge(df2,dfq,how='inner',on=['CODIGOARTICULO','CODIGODESPACHO'])
	if df3.shape[0]>0:
		writeFile(df3.reset_index(drop=True),fileName)
	else:
		print("ERROR:The file don't meet the necesary conditions")
	if errores:
		errores1=df.loc[list(set(df.index.tolist())-set(df3.index.tolist()))]
		if errores1.shape[0]>0:
			writeFile(errores1.reset_index(drop=True),"Error_"+fileName+ext)
		else:
			print("The file doesn't have errors")

def InserTable(insert,typconn="DB2",showerr=True,loginsert=False):
#Con esta funcion se puede realizar un Insert o un Update en base de datos.
	insert=regex.sub(" +"," ",insert).upper()
	if insert.find("INTO")>=0:
		table=insert[insert.find("INTO")+5:insert.find("(")].strip()
		action="insert"
	elif insert.find("UPDATE")>=0:
		table=insert[insert.find("UPDATE")+6:insert.find("SET")].strip()
		action="update"
	elif insert.find("DELETE")>=0:
		table=insert[insert.find("FROM")+5:insert.find("WHERE")].strip()
		action="delete"
	try:
		con = createConnection(typconn)
		cursor = con.cursor()
		cursor.execute(insert)
		con.commit()
		con.close()
		if loginsert:
			print("Log: "+insert)
		return False
	except Exception as e:
		print("\n"+''.join(numpy.repeat("*",210).tolist()))
		if showerr:
			print(e)
		print("Cannot "+action+" in "+table)
		if showerr:
			print("Error: "+insert)
		print(''.join(numpy.repeat("-",210).tolist())+"\n")
		return True

def InsertUpdateProcesotarea(valorestado,codigoProcesoTarea=0,valorestadoInicial="ECI"):
#ejemplo: InsertUpdateProcesotarea("ECI") generara INSERT INTO SMXSIC.SBLOGTPROCESOTAREA (CODIGOCOMPANIA, CODIGOPROCESOTAREA, TIPOESTADOPROCESO, VALORESTADOPROCESO, FECHAPROCESO, ESTADO, IDUSUARIOREGISTRO, FECHAREGISTRO) VALUES(1, NEXT VALUE FOR SBLOGSECPROCESOTAREA, 103011, 'IES', CURRENT_TIMESTAMP, '1', 'USR1093525', CURRENT_TIMESTAMP);
# InsertUpdateProcesotarea("VCD") generara UPDATE SMXSIC.SBLOGTPROCESOTAREA SET VALORESTADOPROCESO = 'VCD', IDUSUARIOMODIFICACION = 'USR1093525', FECHAMODIFICACION = CURRENT_TIMESTAMP WHERE CODIGOCOMPANIA = 1 AND CODIGOPROCESOTAREA = 1000{dependiendo de que valor es el ultimo CODIGOPROCESOTAREA};
	if codigoProcesoTarea==0:
		codigoProcesoTarea=checkCodigoprocesotarea(valorestado,columna="VALORESTADOPROCESO")
	if valorestado==valorestadoInicial:
		InUp="INSERT INTO SMXSIC.SBLOGTPROCESOTAREA (CODIGOCOMPANIA,CODIGOPROCESOTAREA,TIPOESTADOPROCESO,VALORESTADOPROCESO,FECHAPROCESO,ESTADO,IDUSUARIOREGISTRO,FECHAREGISTRO) \
    VALUES (1,{1},103011,'{0}',CURRENT_TIMESTAMP,'1','USR1093525',CURRENT_TIMESTAMP)"
	else:
		InUp="UPDATE SMXSIC.SBLOGTPROCESOTAREA SET VALORESTADOPROCESO='{0}',IDUSUARIOMODIFICACION='USR1093525',FECHAMODIFICACION=CURRENT_TIMESTAMP \
    WHERE CODIGOCOMPANIA=1 AND CODIGOPROCESOTAREA={1}"
	InserTable(InUp.format(valorestado,codigoProcesoTarea+(valorestado=="ECI")))#"VCD"
 
def InsertTest(valorestado,err="",codigoProcesoTarea=0):
#ejemplo: si tiene error: InsertTest('VCD','ERROR OCURRIDO EN...') generara: INSERT INTO SMXSIC.SBLOGTBITEST(CODIGOCOMPANIA, CODIGOPROCESOTAREA, TIPOESTADOPROCESO, VALORESTADOPROCESO, ESTADO, EXISTEERROR, LOGERROR, IDUSUARIOREGISTRO, FECHAREGISTRO) VALUES(1,1000{dependiendo de que valor es el ultimo CODIGOPROCESOTAREA}, 103011, 'VCD', '1', '1', 'ERROR OCURRIDO EN...', 'USR1093525', CURRENT_TIMESTAMP);
#ejemplo: si no tiene errro: InsertTest('VCD') generara: INSERT INTO SMXSIC.SBLOGTBITEST(CODIGOCOMPANIA, CODIGOPROCESOTAREA, TIPOESTADOPROCESO, VALORESTADOPROCESO, ESTADO, EXISTEERROR, LOGERROR, IDUSUARIOREGISTRO, FECHAREGISTRO) VALUES(1,1000{dependiendo de que valor es el ultimo CODIGOPROCESOTAREA}, 103011, 'VCD', '1', '0',NULL, 'USR1093525', CURRENT_TIMESTAMP);
	if codigoProcesoTarea==0:
		codigoProcesoTarea=checkCodigoprocesotarea(valorestado,columna="VALORESTADOPROCESO")
	query="select count(*) from SMXSIC.SBLOGTBITEST where CODIGOCOMPANIA=1 and CODIGOPROCESOTAREA={0} and TIPOESTADOPROCESO=103011 and VALORESTADOPROCESO='{1}' and ESTADO='1' and IDUSUARIOREGISTRO='USR1093525'"
	if executeQuery(query.format(codigoProcesoTarea,valorestado)).astype(int).values[0][0]==0:
		InsUP="INSERT INTO SMXSIC.SBLOGTBITEST (CODIGOCOMPANIA, CODIGOPROCESOTAREA, TIPOESTADOPROCESO, VALORESTADOPROCESO, ESTADO, EXISTEERROR, LOGERROR, IDUSUARIOREGISTRO, FECHAREGISTRO) \
  VALUES (1,{0},103011,'{1}','1','{2}',{3},'USR1093525',CURRENT_TIMESTAMP)"
	else:
		InsUP="UPDATE SMXSIC.SBLOGTBITEST SET EXISTEERROR='{2}',LOGERROR={3},FECHAREGISTRO=CURRENT_TIMESTAMP \
    WHERE CODIGOCOMPANIA=1 AND CODIGOPROCESOTAREA={0} AND TIPOESTADOPROCESO=103011 AND VALORESTADOPROCESO='{1}' AND IDUSUARIOREGISTRO='USR1093525'"
	InserTable(InsUP.format(codigoProcesoTarea,valorestado,int(err!=""),(err=="")*"NULL"+(err!="")*("'"+err+"'")))

def CheckProceso(valorestado,error1="",cpt=0,valorestadoInicial="ECI"):
#funcion para checkear en que parte del proceso esta, solo se pone los valores de SSPCOTCATALOGOVALOR y la frace de error si es que tiene.
#ejemplo: CheckProceso('VCD','ERROR OCURRIDO EN...')
	InsertUpdateProcesotarea(valorestado,cpt,valorestadoInicial)
	InsertTest(valorestado,error1,cpt)

def number2strList(word,string=False):
#checkea si una lista esta en formato string, o es un numero o es una lista de numeros, para transformarlo en un string de lista.
#Esta funcion fue creada, especialmente para poder insertar una lista de elementos en una query de sql.
#ejemplo: number2strList([1,2,3,4,5,6])="1,2,3,4,5,6"
#ejemplo: number2strList([1,2,3,4,5,6],True)="'1','2','3','4','5','6'"
#ejemplo: number2strList(['hola','chao'])="'hola','chao'"
	if (type(word).__name__ in ["Series","ndarray","list"] and (type(word[0]).__name__.find("int")>=0 or type(word[0]).__name__.find("float")>=0)) \
  or type(word).__name__.find("int")>=0 or type(word).__name__.find("float")>=0:
		word=pandas.Series(word).drop_duplicates().reset_index(drop=True).astype(float).astype(int).astype(str).tolist()
		word="'"*string+(",".join(["'"*string]*2)).join(word)+"'"*string
	elif type(word).__name__ in ["Series","ndarray","list"] and type(word[0]).__name__.find("str")>=0:
		word=pandas.Series(word).drop_duplicates().reset_index(drop=True)
		if word[0].isdigit():
			word=word.astype(float).astype(int)
		else:
			string=True
		word=word.astype(str).tolist()
		word="'"*string+(",".join(["'"*string]*2)).join(word)+"'"*string
	elif type(word).__name__.find("str")>=0 and word.find("[")>=0 and word.find("]")>=0:
		word=word[word.find("[")+1:word.find("]")-1] 
		if string:
			word="'"+word.replace(",","','")+"'"
	return word

def list2batch(lista,campo,batch=1000,string=False):
#Crea string para sql, ya que en una lista no puede tener mas de 1000 objetos para una query de sql.
#Ejemplo: list2batch([1,2,3,4,5,6,7,8,9],"CODIGOTAREA",batch=5,string=True)="CODIGOTAREA in ('1','2','3','4','5') or CODIGOTAREA in ('6','7','8','9')"
	L,k=[],0
	if type(lista).__name__.find("int")>=0 or type(lista).__name__.find("float")>=0 or type(lista).__name__.find("str")>=0:
		lista=[lista]
	lista=list(set(lista))
	for j in range(0,len(lista),batch):
		if j>0 or len(lista)<batch:
			p=j*(j<len(lista)//batch*batch)+len(lista)*(j==len(lista)//batch*batch)
			L+=[campo+" in ("+number2strList(lista[k:p],string)+")"]
			k=p
	return " or ".join(L)

def InsertUpdateTareaDespacho(codigoDespacho,procesoLogistico,codigoProcesoTarea=0):
#Esta funcion crea insert o actualiza la base de datos de TAREADESPACHO:
#ejemplo patra insertar: InsertUpdateTareaDespacho(20171581021,"PRO",1) genera: INSERT INTO SMXSIC.SBLOGTTAREADESPACHO (CODIGOCOMPANIA, CODIGODESPACHO, CODIGOPROCESOTAREA, TIPOESTADOPROCESOLOGISTICO, VALORESTADOPROCESOLOGISTICO,IDUSUARIOREGISTRO, FECHAREGISTRO)
#procesoLogistico="PRO" (proceso), ËRR" (error), "TER" (terminado)
#ejemplo de update: InsertUpdateTareaDespacho(20171581021,"TER",1) genera: UPDATE SMXSIC.SBLOGTTAREADESPACHO SET VALORESTADOPROCESOLOGISTICO='TER', IDUSUARIOMODIFICACION='USR1093525', FECHAMODIFICACION=CURRENT_TIMESTAMP WHERE CODIGOCOMPANIA=1 AND CODIGODESPACHO=20171581021 and CODIGOPROCESOTAREA=1;
	if codigoProcesoTarea==0:
		codigoProcesoTarea=checkCodigoprocesotarea(codigoDespacho)
	if procesoLogistico=="PRO":
		InUp1="INSERT INTO SMXSIC.SBLOGTTAREADESPACHO (CODIGOCOMPANIA,CODIGODESPACHO,CODIGOPROCESOTAREA,TIPOESTADOPROCESO,VALORESTADOPROCESO,IDUSUARIOREGISTRO,FECHAREGISTRO) VALUES "
		InUp2="(1,{0},{1},101015,'PRO','USR1093525',CURRENT_TIMESTAMP)"
		InUp3=[]
		if type(codigoDespacho).__name__.find("int")>=0:
			codigoDespacho=[codigoDespacho]
		codigoDespacho=list(codigoDespacho)
		if type(codigoProcesoTarea).__name__.find("int")>=0 or (type(codigoProcesoTarea).__name__.find("list")>=0 and len(codigoProcesoTarea)==1):
			codigoProcesoTarea=numpy.repeat([codigoProcesoTarea],len(codigoDespacho)).tolist()
		bar=progressbar.ProgressBar(max_value=len(codigoDespacho))
		for j in range(len(codigoDespacho)):
			InUp3+=[InUp2.format(codigoDespacho[j],codigoProcesoTarea[j])]
			InUp3=insertBatch(InUp1,InUp3,j==len(codigoDespacho)-1,1000)
			bar.update(j)
	else:
		query="select * from SMXSIC.SBLOGTTAREADESPACHO WHERE CODIGOCOMPANIA=1 and IDUSUARIOREGISTRO='USR1093525' and VALORESTADOPROCESO='{2}' and ({0}) and ({1})"
		df=executeQuery(query.format(list2batch(codigoDespacho,"CODIGODESPACHO",1000),list2batch(codigoProcesoTarea,"CODIGOPROCESOTAREA",1000),procesoLogistico))
		if type(df).__name__.find("NoneType")>=0:
			codd,codt=codigoDespacho,codigoProcesoTarea
			InsertUpdateTareaDespacho(codd,"PRO",codt)
		elif df.shape[0]<len(codigoDespacho):
			if type(codigoDespacho).__name__.find("int")>=0:
				codigoDespacho=[codigoDespacho]
			codigoDespacho=list(codigoDespacho)
			if type(codigoProcesoTarea).__name__.find("int")>=0 or (type(codigoProcesoTarea).__name__.find("list")>=0 and len(codigoProcesoTarea)==1):
				codigoProcesoTarea=numpy.repeat([codigoProcesoTarea],len(codigoDespacho)).tolist()
			codd=pandas.Series(codigoDespacho)
			codt=pandas.Series(codigoProcesoTarea)
			codt=codt[~codd.isin(df.CODIGODESPACHO.tolist())].tolist()
			codd=codd[~codd.isin(df.CODIGODESPACHO.tolist())].tolist()
			InsertUpdateTareaDespacho(cd,"PRO",ct)
		InUp="UPDATE SMXSIC.SBLOGTTAREADESPACHO SET VALORESTADOPROCESO='{2}', IDUSUARIOMODIFICACION='USR1093525', FECHAMODIFICACION=CURRENT_TIMESTAMP \
    WHERE CODIGOCOMPANIA=1 and ({0}) and ({1})"
		InserTable(InUp.format(list2batch(codigoDespacho,"CODIGODESPACHO",1000),list2batch(codigoProcesoTarea,"CODIGOPROCESOTAREA",1000),procesoLogistico))

def InsertUpdateManyTareaDespachosFile(TareaDespachosFile,codigoprocesotarea=0):
#Realiza una carga masiva desde un archivo o DataFrame a la base de datos SMXSIC.SBLOGTTAREADESPACHO
#Ejemplo: InsertUpdateManyTareaDespachosFile("tareas_generadas1.xlsx") esta funcion insertara en la tabla de SMXSIC.SBLOGTTAREADESPACHO con "PRO", si en la columan VALORESTADOPROCESO del archivo tiene PRO.
#Actualizara con "TER" y/o "ERR" en la tabla de SMXSIC.SBLOGTTAREADESPACHO, si en la columan VALORESTADOPROCESO del archivo tiene "TER" y/o "ERR" respectivamente.
#El archivo o DataFrame para ejecutar la funcion, debe tener las siguientes 3 columnas: CODIGODESPACHO,CODIGOPROCESOTAREA,VALORESTADOPROCESO("PRO","TER" y/o "ERR").
	task=checkFileorDF2DF(TareaDespachosFile)
	if type(task).__name__!="NoneType" and task.shape[0]>0:
		if "CODIGOPROCESOTAREA" not in task.columns:
			if codigoprocesotarea==0:
				task["CODIGOPROCESOTAREA"]=checkCodigoprocesotarea(task.CODIGODESPACHO.astype(int).drop_duplicates().tolist())
			elif type(codigoprocesotarea).__name__.find("int")>=0 or type(codigoprocesotarea).__name__.find("float")>=0:
				task["CODIGOPROCESOTAREA"]=int(codigoprocesotarea)
			elif type(codigoprocesotarea).__name__.find("list")>=0 and len(codigoprocesotarea)==task.shape[0]:
				task["CODIGOPROCESOTAREA"]=pandas.Series(codigoprocesotarea).astype(int).tolist()
			elif ((type(codigoprocesotarea).__name__.find("Series")>=0 or type(codigoprocesotarea).__name__.find("DataFrame")>=0) and codigoprocesotarea.shape[0]==task.shape[0]):
				task["CODIGOPROCESOTAREA"]=codigoprocesotarea.astype(int).tolist()
		df1=executeQuery("select CODIGODESPACHO,CODIGOPROCESOTAREA,VALORESTADOPROCESO from SMXSIC.SBLOGTTAREADESPACHO WHERE CODIGOCOMPANIA=1 and ({0}) and ({1})".\
        format(list2batch(task.CODIGODESPACHO.astype(int).drop_duplicates().tolist(),"CODIGODESPACHO",1000),list2batch(task.CODIGOPROCESOTAREA.astype(int).drop_duplicates().tolist(),"CODIGOPROCESOTAREA",1000)))
		if type(df1).__name__!="NoneType":
			task=task[~(task[["CODIGODESPACHO","CODIGOPROCESOTAREA","VALORESTADOPROCESO"]].isin(df1[["CODIGODESPACHO","CODIGOPROCESOTAREA","VALORESTADOPROCESO"]]).all(axis=1))]\
      [["CODIGODESPACHO","CODIGOPROCESOTAREA","VALORESTADOPROCESO"]]
		if task.shape[0]>0:
			task=task.drop_duplicates().reset_index(drop=True)
			taskPRO=task[task.VALORESTADOPROCESO=="PRO"]
			taskTER=task[task.VALORESTADOPROCESO=="TER"]
			taskERR=task[task.VALORESTADOPROCESO=="ERR"]
			if taskPRO.shape[0]>0:
				InsertUpdateTareaDespacho(taskPRO.CODIGODESPACHO.tolist(),"PRO",taskPRO.CODIGOPROCESOTAREA.tolist())
			if taskTER.shape[0]>0:
				InsertUpdateTareaDespacho(taskTER.CODIGODESPACHO.tolist(),"TER",taskTER.CODIGOPROCESOTAREA.tolist())
			if taskERR.shape[0]>0:
				InsertUpdateTareaDespacho(taskERR.CODIGODESPACHO.tolist(),"ERR",taskERR.CODIGOPROCESOTAREA.tolist())

def insertBatch(insert,inslist,checkend=False,batch=1000):
#Llena la base de datos port batch.
	if len(inslist)%batch==0 or checkend:
		if InserTable(insert+",".join(inslist),"DB2",False):
			table="Llenado de "+insert[insert.find("INTO")+5:insert.find("(")].strip().upper()+" 1 a 1:"
			bar=progressbar.ProgressBar(widgets=[table,progressbar.Bar(),' ',progressbar.Counter(format='%(value)02d/%(max_value)d'),],max_value=len(inslist))
			for j,ins in enumerate(inslist):
				InserTable(insert+ins)
				bar.update(j)
		inslist=[]
	return inslist

#######UNDER BUILDING#########
'''
def insertAll(insert,insArrayList,typconn="DB2",batch=1000):
	try:
		con = createConnection(typconn)
		cursor = con.cursor()
		cursor.executemany(insert,insArrayList)
		con.commit()
		con.close()
	except:
		ins0=insert.upper().split("VALUES")
		ins=ins0[1].split("?")
		iL=ins[0]+insArrayList.iloc[:,0]
		for j in range(1,insArrayList.shape[1]):
			iL+=ins[j]+insArrayList.iloc[:,j]
		iL+=ins[-1]
		ir=numpy.array_split(numpy.arange(iL.size),iL.size//batch+(iL.size%batch>0))
		for j in range(len(ir)):
			insertBatch(ins0[0],iL[ir[j]].tolist(),ir[j].size<batch,batch)
'''
#######UNDER BUILDING#########

def checkFileorDF2DF(FileorDF):
#checkea si es un archivo o un DataFrame, para exportarlo como DataFrame.
	if type(FileorDF).__name__=="str":
		return readFile(FileorDF)
	elif type(FileorDF).__name__=="DataFrame":
		return FileorDF

def articuloConError(errorFile,batch=1000):
#Hace el insert de los errores de los articulos (ej. volumen, peso, densidad, entre otros).
#Este insert, puede ser hecho desde un archivo o desde un DataFrame,realiza los insert por batch.
#Ejemplo: articuloConError("Erroes_Verificados.pickle") esta funcion llenara las tablas de SMXSIC.SBLOGTBITTARDES y SMXSIC.SBLOGTDETALLEERROR, con los errores respectivos.
	err=checkFileorDF2DF(errorFile)
	if type(err).__name__!="NoneType" and err.shape[0]>0:
		if err.isna().any().any():
			typerr=readFile("erroresNull.xlsx")
		inserr11="INSERT INTO SMXSIC.SBLOGTBITTARDES (CODIGOCOMPANIA,CODIGODESPACHO,CODIGOARTICULO,CODIGOUNIDADMANEJO,CODIGOUBICACION,CANTIDAD,IDUSUARIOREGISTRO,FECHAREGISTRO) VALUES "
		inserr12="(1,{0},'{1}',{2},{3},{4},'USR1093525',CURRENT_TIMESTAMP)"
		inserr21="INSERT INTO SMXSIC.SBLOGTDETALLEERROR (CODIGOCOMPANIA,CODIGODESPACHO,CODIGOARTICULO,CODIGOUNIDADMANEJO,VALORTIPOERROR,CODIGOTIPOERROR,DESCRIPCION,IDUSUARIOREGISTRO,\
    FECHAREGISTRO) VALUES "
		inserr22="(1,{0},'{1}',{2}"
		inserr13=[]
		inserr24=[]
		q1="select CODIGODESPACHO,CODIGOARTICULO,CODIGOUNIDADMANEJO from SMXSIC.SBLOGTBITTARDES where "
		qL=("(CODIGODESPACHO="+err["CODIGODESPACHO"].astype(int).astype(str)+" and CODIGOARTICULO='"+err["CODIGOARTICULO"].astype(int).astype(str)+"' and CODIGOUNIDADMANEJO="+\
    err["CODIGOUNIDADMANEJO"].astype(int).astype(str)+")")
		q2="select CODIGODESPACHO,CODIGOARTICULO,CODIGOUNIDADMANEJO,VALORTIPOERROR from SMXSIC.SBLOGTDETALLEERROR where "
		k,df10,df20=0,[],[]
		ir=numpy.array_split(numpy.arange(qL.size),qL.size//batch+(qL.size%batch>0))
		bar=progressbar.ProgressBar(widgets=["Check de errores en las tablas:",progressbar.Bar(),' ',progressbar.Counter(format='%(value)02d/%(max_value)d'),],max_value=len(ir))
		for j in range(len(ir)):
			df00=executeQuery(q1+" or ".join(qL[ir[j]]))
			if type(df00).__name__!="NoneType":
				df10+=df00.astype(int).values.tolist()
			df00=executeQuery(q1+" or ".join(qL[ir[j]]))
			if type(df00).__name__!="NoneType":
				df20+=df00.astype(int).values.tolist()
			bar.update(j)
		df1=pandas.DataFrame(df10,columns=["CODIGODESPACHO","CODIGOARTICULO","CODIGOUNIDADMANEJO"])
		df2=pandas.DataFrame(df20,columns=["CODIGODESPACHO","CODIGOARTICULO","CODIGOUNIDADMANEJO","VALORTIPOERROR"])
		err1=err[["CODIGODESPACHO","CODIGOARTICULO","CODIGOUNIDADMANEJO"]].astype(int)
		if ~err1.isin(df1).all(None):
			err2=err[~err1.isin(df1).all(1)]
			err2["UbicacionIsNull"]="Null"
			err2.loc[err2.CODIGOUBICACION.notna(),"UbicacionIsNull"]=err2.loc[err2.CODIGOUBICACION.notna(),"CODIGOUBICACION"].astype(int).astype(str)
			err2["CantidadIsNull"]="Null"
			err2.loc[err2.CANTIDAD.notna(),"CantidadIsNull"]=err2.loc[err2.CANTIDAD.notna(),"CANTIDAD"].astype(int).astype(str)
			err2[["CODIGODESPACHO","CODIGOARTICULO","CODIGOUNIDADMANEJO"]]=err2[["CODIGODESPACHO","CODIGOARTICULO","CODIGOUNIDADMANEJO"]].astype(int).astype(str)
			bar=progressbar.ProgressBar(widgets=["Llenado de SMXSIC.SBLOGTBITTARDES:",progressbar.Bar(),' ',progressbar.Counter(format='%(value)02d/%(max_value)d'),],max_value=err2.shape[0])
			for j in range(err2.shape[0]):
				inserr13+=[inserr12.format(*err2.iloc[j][["CODIGODESPACHO","CODIGOARTICULO","CODIGOUNIDADMANEJO","UbicacionIsNull","CantidadIsNull"]].tolist())]
				inserr13=insertBatch(inserr11,inserr13,j==err.shape[0]-1,batch)
				bar.update(j)
		bar=progressbar.ProgressBar(widgets=["Llenado de SMXSIC.SBLOGTDETALLEERROR:",progressbar.Bar(),' ',progressbar.Counter(format='%(value)02d/%(max_value)d'),],max_value=err.shape[0])
		for j in range(err.shape[0]):
			d,a,u=err.iloc[j][["CODIGODESPACHO","CODIGOARTICULO","CODIGOUNIDADMANEJO"]].astype(int).tolist()
			inserr23=inserr22.format(*[d,a,u])+",'{0}',103020,'{1}','USR1093525',CURRENT_TIMESTAMP)"
			errnum=df2.loc[(df2.CODIGODESPACHO==d)&(df2.CODIGOARTICULO==a)&(df2.CODIGOUNIDADMANEJO==u),"VALORTIPOERROR"].tolist()
			if err.iloc[j].isna().any():
				colerr=typerr.COLUMNA_NULL[typerr.COLUMNA_NULL.isin(err.columns[err.iloc[j].isna()].tolist())].tolist()
				if len(colerr)>0:
					typerr1=typerr[typerr.COLUMNA_NULL.isin(colerr)]
					numerr=list(set(typerr1.NUMERO_ERROR.astype(int).drop_duplicates().tolist())-set(errnum))
					for ne in numerr:
						msg=typerr1.loc[typerr.NUMERO_ERROR==ne,"ERROR_DESCRIPCION"].values[0]
						inserr24+=[inserr23.format(ne,msg[:100])]
						inserr24=insertBatch(inserr21,inserr24,j==err.shape[0]-1,batch)
			if pandas.notna(err.UBICACION.iloc[j]) and sum(pandas.Series(err.UBICACION.iloc[j].split(" ")).str.len()*pandas.Series(err.UBICACION.iloc[j].split(" ")).str.isnumeric()*[100,10,1])!=343 and 3 not in errnum:
				inserr24+=[inserr23.format(3,"Campo de Ubicacion no corresponda al formato XXX XXXX XXX")]
				inserr24=insertBatch(inserr21,inserr24,j==err.shape[0]-1,batch)
			if pandas.notna(err.DENSIDAD.iloc[j]) and err.DENSIDAD.iloc[j]>2500 and 4 not in errnum:
				inserr24+=[inserr23.format(4,"Campo de Densidad mayor a 2500 kg/m3")]
				inserr24=insertBatch(inserr21,inserr24,j==err.shape[0]-1,batch)
			if pandas.notna(err.RESISTENCIA.iloc[j]) and abs(int(err.RESISTENCIA.iloc[j])-3)>1 and 1 not in errnum:
				msg="la resistencia es "+bool(int(err.RESISTENCIA.iloc[j])<2)*"menor a 2"+bool(int(err.RESISTENCIA.iloc[j])>4)*"mayor a 4"
				inserr24+=[inserr23.format(1,msg[:100])]
				inserr24=insertBatch(inserr21,inserr24,j==err.shape[0]-1,batch)
			if pandas.notna(err.PESO.iloc[j]) and err.PESO.iloc[j]>1000 and 6 not in errnum:
				inserr24+=[inserr23.format(6,"peso mayor a 1000 kg")]
				inserr24=insertBatch(inserr21,inserr24,j==err.shape[0]-1,batch)
			if pandas.notna(err.VOLUMEN.iloc[j]) and err.VOLUMEN.iloc[j]>2.5 and 7 not in errnum:
				inserr24+=[inserr23.format(7,"volumen mayor a 2.5 m3")]
				inserr24=insertBatch(inserr21,inserr24,j==err.shape[0]-1,batch)
			if err.iloc[j][["VOLUMEN","PESO"]].notna().all() and err.iloc[j][["VOLUMEN","PESO"]].prod()<=0 and 10 not in errnum:
				msg=bool(err.PESO.iloc[j]<=0)*"peso"+bool(err.iloc[j][["VOLUMEN","PESO"]].prod()<=0)*"y "+bool(err.VOLUMEN.iloc[j]<=0)*"volumen"+"menor o igual a 0"+\
        bool(err.VOLUMEN.iloc[j]<=0)*" m3"+bool(err.PESO.iloc[j]<=0)*" kg"
				inserr24+=[inserr23.format(7+bool(err.VOLUMEN.iloc[j]<=0)*3,msg)]
				inserr24=insertBatch(inserr21,inserr24,j==err.shape[0]-1,batch)
			dx=readFile("resistencias.xlsx")[["RESISTENCIA","maxPeso"]]
			if err.iloc[j][["PESO","RESISTENCIA"]].notna().all() and ((err.PESO.iloc[j]>dx.loc[dx.RESISTENCIA==1,"maxPeso"].values[0] and err.RESISTENCIA.iloc[j]=="2") or \
      (err.iloc[j]["PESO"]>dx.loc[dx.RESISTENCIA==2,"maxPeso"].values[0] and err.RESISTENCIA.iloc[j]=="3")) and 11 not in errnum:
				msg="Peso del Articulo supera peso maximo que soporta por resistencia (Resistencia "+str(2+(err.RESISTENCIA.iloc[j]=="3"))+" hasta "+\
        str(dx.loc[dx.RESISTENCIA<2,"maxPeso"].values[0]*(err.RESISTENCIA.iloc[j]=="2")+dx.loc[dx.RESISTENCIA==2,"maxPeso"].values[0]*(err.RESISTENCIA.iloc[j]=="3"))+" kg)"
				inserr24+=[inserr23.format(11,msg[:100])]
				inserr24=insertBatch(inserr21,inserr24,j==err.shape[0]-1,batch)
			bar.update(j)
		inserr24=insertBatch(inserr21,inserr24,len(inserr24)>0,batch)
def InsertTareas(taskFile,batch=1000):
#Hace el insert de las tareas generadas despues de la optimizacion por pallet, lego y recorrido. Este insert, puede ser hecho desde un archivo o desde un DataFrame,
#realiza los insert por batch.
	task=checkFileorDF2DF(taskFile)
	if type(task).__name__!="NoneType" and task.shape[0]>0:
		instaskdet="INSERT INTO SMXSIC.SBLOGTTARDET (CODIGOCOMPANIA,CODIGOTAREA,CODIGODESPACHO,CODIGOARTICULO,CODIGOUNIDADMANEJO,CODIGOUBICACION,CANTIDAD,CODIGOTIPODIRECCION,VALORTIPODIRECCION,ORDEN,\
     IDUSUARIOREGISTRO, FECHAREGISTRO) VALUES "
		instaskdet2="(1,{0},{1},'{2}',{3},{4},{5},103013,'{6}',{7},'USR1093525',CURRENT_TIMESTAMP)"
		instask00="INSERT INTO SMXSIC.SBLOGTTAREA (CODIGOCOMPANIA,CODIGOTAREA,CODIGODESPACHO,IDUSUARIOREGISTRO,FECHAREGISTRO) VALUES "
		instask01="INSERT INTO SMXSIC.SBLOGTTAREA (CODIGOCOMPANIA,CODIGOTAREA,CODIGODESPACHO,CODIGOCOMPANIAPADRE,CODIGOTAREAPADRE,CODIGODESPACHOPADRE,IDUSUARIOREGISTRO,FECHAREGISTRO) VALUES "
		instask10="(1,{0},{1},'USR1093525',CURRENT_TIMESTAMP)"
		instask11="(1,{0},{1},1,{2},{1},'USR1093525',CURRENT_TIMESTAMP)"
		df1=task[["CODIGODESPACHO","ID_PALLET","ID_LEGO"]].drop_duplicates().reset_index(drop=True)
		df2=df1[df1.ID_PALLET!=df1.ID_LEGO][["CODIGODESPACHO","ID_PALLET","ID_PALLET"]]
		df2.columns=df1.columns.tolist()
		df1=pandas.concat([df1,df2]).drop_duplicates()
		df1["check"]=(df1.ID_PALLET!=df1.ID_LEGO)*1
		df1=df1.sort_values(by=["CODIGODESPACHO","check","ID_PALLET","ID_LEGO"]).reset_index(drop=True)
		df1["contar"]=numpy.nan
		df2=df1.groupby(by="CODIGODESPACHO",as_index=False)["ID_PALLET"].count()
		for cd in df2.CODIGODESPACHO:
			df1.loc[df1.CODIGODESPACHO==cd,"contar"]=list(range(1,df2.loc[df2.CODIGODESPACHO==cd,"ID_PALLET"].iloc[0]+1))
		df1["contar"]=df1["contar"].astype(int)
		df2=df1.loc[df1.ID_PALLET==df1.ID_LEGO,["CODIGODESPACHO","ID_PALLET","contar"]]
		df2["contar2"]=df2["contar"]
		df1=df1.merge(df2[["CODIGODESPACHO","ID_PALLET","contar2"]],on=["CODIGODESPACHO","ID_PALLET"],how="left")
		task1=df1.loc[df1.check==0,["contar","CODIGODESPACHO"]].sort_values(by=["CODIGODESPACHO","contar"]).astype(int)
		instask2=[]
		bar=progressbar.ProgressBar(widgets=["TareasPadreHijoIguales ",progressbar.Bar(),' ',progressbar.Counter(format='%(value)02d/%(max_value)d'),],max_value=task1.shape[0])
		for k in range(task1.shape[0]):
			instask2+=[instask10.format(*task1.iloc[k].tolist())]
			instask2=insertBatch(instask00,instask2,k==task1.shape[0]-1,batch)
			bar.update(k)
		task1=df1.loc[df1.check==1,["contar","CODIGODESPACHO","contar2"]].sort_values(by=["CODIGODESPACHO","contar"]).astype(int)
		instask2=[]
		bar=progressbar.ProgressBar(widgets=["TareasPadreHijoDistintos ",progressbar.Bar(),' ',progressbar.Counter(format='%(value)02d/%(max_value)d'),],max_value=task1.shape[0])
		for k in range(task1.shape[0]):
			instask2+=[instask11.format(*task1.iloc[k].tolist())]
			instask2=insertBatch(instask01,instask2,k==task1.shape[0]-1,batch)
			bar.update(k)
		task2=task.merge(df1,on=["CODIGODESPACHO","ID_PALLET","ID_LEGO"],how="left")[["contar","CODIGODESPACHO","CODIGOARTICULO","CODIGOUNIDADMANEJO","CODIGOUBICACION","CANTIDAD","VALORTIPODIRECCION","ORDEN"]]\
    .astype(int).drop_duplicates()
		instaskdet3=[]
		bar=progressbar.ProgressBar(widgets=["TareasDetalles ",progressbar.Bar(),' ',progressbar.Counter(format='%(value)02d/%(max_value)d'),],max_value=task2.shape[0])
		for k in range(task2.shape[0]):
			instaskdet3+=[instaskdet2.format(*task2.iloc[k].tolist())]
			instaskdet3=insertBatch(instaskdet,instaskdet3,k==task2.shape[0]-1,batch)
			bar.update(k)

def verificados(timestamp,codigoprocesotarea,bodega,subbodega,codigodespacho,tipo,error=True,ext=".pickle"):
#funcion generadora de verificados 
#y:año, m:mes, d:dia, j:bodega
#error: True si se requiere generaro los errores de bases de datos en un excel,False, si no se requiere generar los errores de base de datos en un excel.
#ejemplo:err=verificados(1562292056,20858,["PEA","SPE"])
	if timestamp!=None:
		date=datetime.datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
	q="select d.fechadespacho,d.codigoubicacion,d.codigoarticulo,d.ubicacion,d.codigounidadmanejo,d.cantidad,d.codigodespacho,\
  b.nombrearticulo,b.volumen,b.peso,b.densidad,b.resistencia,b.contaminante,\
  case when int(substring(a.pasillo,2,2))>=98 then a.coordenadaxlocal else a.coordenadaxpasillolocal end as X_PASILLO_LOCAL ,a.coordenadaylocal,a.coordenadaxglobal,a.coordenadayglobal,a.nivel,a.nave,a.rack,\
  int(substring(a.pasillo,2,2)) as pasillo, a.coordenadaxlocal,d.prioridaddespacho \
  from SMXSIC.SLPEAVPEDIDO d left JOIN SMXSIC.SCARTVINFART b \
  ON d.CODIGOARTICULO=b.CODIGOARTICULO AND d.CODIGOAREATRABAJOSUBBODEGA=b.CODIGOAREATRABAJOSUBBODEGA and d.codigounidadmanejo=b.codigounidadmanejo \
  left JOIN SMXSIC.SBLOGVUBIART A \
  ON d.CODIGOARTICULO=A.CODIGOARTICULO AND d.CODIGOUBICACION=A.CODIGOUBICACION and d.codigounidadmanejo=a.codigounidadmanejo "
	if (codigodespacho==None or len(codigodespacho)==0) and len(tipo)>0:
		q+="WHERE d.codigocompania=1 and d.CODIGOAREATRABAJOSUBBODEGA in ({3}) AND d.fechadespacho='{0}' and d.CODIGOAREATRABAJOBODEGA in ({1}) and d.tipoobjetivodespacho in ({2}) and d.CODIGOPROCESOTAREA={4}"
		q=q.format(date,number2strList(bodega),number2strList(tipo),number2strList(subbodega),codigoprocesotarea)
		errtxt="CODIGOAREATRABAJOSUBBODEGA=[{3}], FECHADESPACHO='{0}', CODIGOAREATRABAJOBODEGA=[{1}], TIPOOBJETIVODESPACHO=[{2}] y CODIGOPROCESOTAREA={4}"
		errtxt=errtxt.format(date,number2strList(bodega),number2strList(tipo),number2strList(subbodega),codigoprocesotarea)
	elif len(codigodespacho)>0 and (tipo==None or len(tipo)==0):
		q+="WHERE d.codigocompania=1 and d.codigodespacho in ({0}) and d.CODIGOPROCESOTAREA={1}"
		q=q.format(number2strList(codigodespacho),codigoprocesotarea)
		errtxt="CODIGODESPACHO=[{0}] y CODIGOPROCESOTAREA={1}"
		errtxt=errtxt.format(number2strList(codigodespacho),codigoprocesotarea)
	else:
		q+="WHERE d.codigocompania=1 and d.CODIGOAREATRABAJOSUBBODEGA in ({3}) AND d.fechadespacho='{0}' and d.CODIGOAREATRABAJOBODEGA in ({1}) and d.tipoobjetivodespacho in ({2}) and d.codigodespacho in ({4}) and d.CODIGOPROCESOTAREA={5}"
		q=q.format(date,number2strList(bodega),number2strList(tipo),number2strList(subbodega),number2strList(codigodespacho),codigoprocesotarea)
		errtxt="CODIGOAREATRABAJOSUBBODEGA=[{3}], FECHADESPACHO='{0}', CODIGOAREATRABAJOBODEGA=[{1}], TIPOOBJETIVODESPACHO=[{2}], CODIGODESPACHO=[{4}] y CODIGOPROCESOTAREA={5}"
		errtxt=errtxt.format(date,number2strList(bodega),number2strList(tipo),number2strList(subbodega),number2strList(codigodespacho),codigoprocesotarea)
	fileName="Verificados"
	df=executeQuery(q+" order by d.prioridaddespacho")
	if type(df).__name__!='NoneType':
		df=df.replace(to_replace=[None],value=numpy.nan).drop_duplicates().reset_index(drop=True)
		df.NOMBREARTICULO=df.NOMBREARTICULO.str.replace('  ',' ').str.replace(' ','_')
		df1=df[df.notna().all(1)]
		dx=readFile("resistencias.xlsx")[["RESISTENCIA","maxPeso"]]
		df1[["NAVE","PASILLO","RESISTENCIA"]]=df1[["NAVE","PASILLO","RESISTENCIA"]].astype(int)
		df1=df1[(df1.DENSIDAD<=2500)&(df1.VOLUMEN<=2.5)&(df1.VOLUMEN>0)&(df1.PESO>0)&(df1.PESO<=1000)&((df1.UBICACION.str.split(" ",expand=True).applymap(lambda x:len(x)*str.isnumeric(x))*[100,10,1]).sum(1)==343)&((df1.NAVE*100+df1.PASILLO)==df1.UBICACION.str.split(' ',expand=True)[0].astype(int))&(((df1.RESISTENCIA==2)&(df1.PESO<=dx.loc[dx.RESISTENCIA==1,'maxPeso'].values[0]))|((df1.RESISTENCIA==3)&(df1.PESO<=dx.loc[dx.RESISTENCIA==2,'maxPeso'].values[0]))|((df1.RESISTENCIA==4)&(df1.PESO<=dx.loc[dx.RESISTENCIA==3,'maxPeso'].values[0])))]
		if error:
			errores1=df.loc[list(set(df.index.tolist())-set(df1.index.tolist()))].reset_index(drop=True)
			if errores1.shape[0]>0:
				writeFile(errores1,"Error_"+fileName+ext)
		df1["RESISTENCIA"]-=1
		dx1=df1.groupby(['NAVE','PASILLO'],as_index=False)['RESISTENCIA'].max().rename(columns={'RESISTENCIA':'RESISTENCIA_PASILLO'})
		df1=pandas.merge(df1,dx1,how='left',on=['NAVE','PASILLO']).drop_duplicates()
		df1[['CODIGODESPACHO','CODIGOARTICULO','CODIGOUBICACION','CODIGOUNIDADMANEJO']]=df1[['CODIGODESPACHO','CODIGOARTICULO','CODIGOUBICACION','CODIGOUNIDADMANEJO']].astype(int).astype(str)
		writeFile(df1.reset_index(drop=True),fileName+ext)
		writeFile(df1.reset_index(drop=True),fileName+".xlsx")
		writeFile(df1.reset_index(drop=True),fileName+".pickle")
	else:
		print("Advertencia: Por favor verifique la entrada de sus datos, ya que "+errtxt+" no muestra data en la query:"+q)
		writeFile(df,fileName+ext)

def saveHistoricos(fileName,folder0=relPathToDatos):
#funcion para pasar archivos desde una carpeta a una subcarpeta llamada "Historicos"
	if sys.platform.startswith('win'):
		folder1=folder0+"Historicos\\"
	else:
		folder1=folder0+"Historicos/"
	if not(os.path.isdir(folder1)):
		os.makedirs(folder1)
	if fileName in os.listdir(folder0):
		if fileName in os.listdir(folder1):
			os.remove(os.path.join(folder1,fileName))
		writeFile(readFile(fileName,folder0),fileName,folder1)
		os.remove(os.path.join(folder0,fileName))
	else:
		print("Advertencia: El archivo "+fileName+" no ha sido encontrado en "+folder0)

def checkCodigoprocesotarea(lista,columna="CODIGODESPACHO"):
	df1=executeQuery("select "+columna+",CODIGOPROCESOTAREA from SMXSIC.SBLOGTTAREADESPACHO WHERE CODIGOCOMPANIA=1 and {}".\
        format(list2batch(lista,columna,1000)))
	if type(df1).__name__!="NoneType":
		listaSerie=pandas.Series(lista,name=columna)
		if ~listaSerie.isin(df1[columna]).all():
			codigoprocesotarea=executeQuery("select max(CODIGOPROCESOTAREA) from SMXSIC.SBLOGTPROCESOTAREA WHERE CODIGOCOMPANIA=1").astype(int).values[0][0]
			df2=listaSerie[~listaSerie.isin(df1[columna])].to_frame()
			df2["CODIGOPROCESOTAREA"]=codigoprocesotarea
			df1=pandas.concat([df1,df2])
		codigoprocesotarea=df1["CODIGOPROCESOTAREA"].tolist()
	else:
		codigoprocesotarea=executeQuery("select max(CODIGOPROCESOTAREA) from SMXSIC.SBLOGTPROCESOTAREA WHERE CODIGOCOMPANIA=1").astype(int).values[0][0]
	return codigoprocesotarea

def deleteSql(basedatos,where):
#ej: deleteSql("SMXSIC.SBLOGTTAREA","where IDUSUARIOREGISTRO='USR1093525'") borra la base de datos SMXSIC.SBLOGTTAREA, cuando IDUSUARIOREGISTRO='USR1093525'
	dele="DELETE FROM "+basedatos+" "+where
	InserTable(dele)

def deleteAllSql():
	deleteSql("SMXSIC.SBLOGTBITEST","where IDUSUARIOREGISTRO='USR1093525'")
	deleteSql("SMXSIC.SBLOGTTARDET","where IDUSUARIOREGISTRO='USR1093525'")
	deleteSql("SMXSIC.SBLOGTTAREA","where IDUSUARIOREGISTRO='USR1093525'")
	deleteSql("SMXSIC.SBLOGTTAREADESPACHO","where IDUSUARIOREGISTRO='USR1093525'")
	deleteSql("SMXSIC.SBLOGTDETALLEERROR","where IDUSUARIOREGISTRO='USR1093525'")
	deleteSql("SMXSIC.SBLOGTBITTARDES","where IDUSUARIOREGISTRO='USR1093525'")
	deleteSql("SMXSIC.SBLOGTPROCESOTAREA","where IDUSUARIOREGISTRO='USR1093525'")