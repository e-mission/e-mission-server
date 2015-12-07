from emission.core.get_database import get_client_stats_db_backup, get_server_stats_db_backup, get_result_stats_db_backup


#x = get_client_stats_db_backup().find()
#print(list(x))

def write_stats(fname, headers, entries):
	f = open(fname, "w+")
	f.write(','.join(headers) + '\n')
	for entry in entries:
		row = convert_to_row(headers, entry)
		f.write(','.join(row) + '\n')

def convert_to_row(headers, entry_json):
	row = []
	try:
		for header in headers:
			row.append(str(entry_json[header]))
	except Exception as e:
		print(e)
	return row


def export_server_stats():
	entries = list(get_server_stats_db_backup().find())
	fname = "server_stats.csv"
	headers = ["stat", "_id", "reading", "ts", "user"]

	write_stats(fname, headers, entries)

def export_result_stats():
	entries = list(get_result_stats_db_backup().find())
	fname = "result_stats.csv"
	headers = ['stat', '_id', 'reading', 'ts', 'user']

	write_stats(fname, headers, entries)

def export_client_stats():

	entries = list(get_client_stats_db_backup().find())
	fname = "result_stats.csv"
	headers = ['reported_ts', 'stat', 'reading', 'ts', 'client_os_version', 'client_app_version', 'user', '_id']

	write_stats(fname, headers, entries)


#export_server_stats()
#export_result_stats()
export_client_stats()