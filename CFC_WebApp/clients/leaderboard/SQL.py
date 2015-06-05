import pyodbc

CONNECTION = pyodbc.connect("DRIVER={SQL Server};SERVER=tcp:xalxu8zk56.database.windows.net,1433;DATABASE=e-mission_db;Uid=joshz@xalxu8zk56;PWD=Nathan123")

def put(key, value):
	cursor = CONNECTION.cursor()
	_id = hash(key)
	try:
		cursor.execute("insert into Scores(ID, Column1, Score) values ('%s', '%s', '%s')" % (_id, key, value))
		CONNECTION.commit()
	except:
		update(key, value)

def get(key):
	cursor = CONNECTION.cursor()
	cursor.execute("select Column1, Score from Scores where Column1=?", key)
	row = cursor.fetchone()
	return row[1]

def update(key, value):
	cursor = CONNECTION.cursor()
	cursor.execute("update Scores set Score=? where Column1=?", value, key)
	cursor.commit()