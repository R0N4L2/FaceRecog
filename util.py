import pandas
import os
import sys
import pickle
import pyodbc
import numpy
import progressbar
import gc
import regex

relPathToDatos = "	\\Datos\\"
if not sys.platform.startswith('win'):
	relPathToDatos = "../Datos/"

def readFile(fileName):
	if fileName.endswith('.csv'):
		return pandas.read_csv(os.path.join(relPathToDatos,fileName),dtype={'CODIGO BARRAS': str,'UBICACION': str})
	if fileName.endswith('.xlsx'):
		return pandas.read_excel(os.path.join(relPathToDatos,fileName),dtype={'CODIGO BARRAS': str,'UBICACION': str})
	if fileName.endswith('.pickle'):
		with open(os.path.join(relPathToDatos,fileName), 'rb') as f:
			return pickle.load(f)
		
def writeCSV(df,fileName,useIndex = False):
	df.to_csv(os.path.join(relPathToDatos,fileName),index=useIndex)
	
def writeExcel(df,fileName,useIndex = False):
	df.to_excel(os.path.join(relPathToDatos,fileName),index=useIndex)
	
def writePickle(df,fileName):
	with open(os.path.join(relPathToDatos,fileName), 'wb') as f:
		pickle.dump(df, f, pickle.HIGHEST_PROTOCOL)
		
def writeFile(df,fileName,useIndex = False):
	if fileName.endswith('.csv'):
		writeCSV(df,fileName,useIndex = useIndex)
	if fileName.endswith('.xlsx'):
		writeExcel(df,fileName,useIndex = useIndex)
	if fileName.endswith('.pickle'):
		writePickle(df,fileName)
		
def createConnection(typcon = "DB2"):
	if typcon=="DB2":
		connection = pyodbc.connect(driver='{IBM i Access ODBC Driver 64-bit}',system='10.200.20.49',uid='ISMX1AYCD',database='SMXSIC',pwd='Ej9Gtp75')
	else:
		connection = pyodbc.connect(driver='{SQL Server}',server='10.200.20.49',uid='ISMX1AYCD', database='SMXSIC',pwd='Ej9Gtp75')
	return connection
def executeQuery(query,typconn = "DB2"):
	query=regex.sub(" +"," ",query)
	try:
		con = createConnection(typcon = typconn)
		cant = pandas.read_sql_query("select count(*) "+query[query.find("from"):],con).iloc[0,0]
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

def InserTable(insert,typconn = "DB2"):
	table=insert[insert.find("INTO")+5:insert.find("(")].strip()
	try:
		con = createConnection(typcon = typconn)
		cursor = con.cursor()
		cursor.execute(insert)
		con.commit()
		con.close()
	except Exception as e:
		print(e)
		print("Can't insert in "+table)