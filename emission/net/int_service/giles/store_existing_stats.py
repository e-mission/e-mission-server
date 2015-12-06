import pandas
from emission.net.api.stats import storeClientEntry, storeServerEntry, storeResultEntry

def loadEntries(fname):
	df = pandas.read_csv(fname)
	entries = []

	for c in range(len(df)):
		entry = {}
		for key in df.keys():
			entry[key] = df[key][c]
		entries.append(entry)

	return entries

def storeClientEntries(fname):
	df = pandas.read_csv(fname)
	for c in range(len(df)):
		try:
			user = df['user'][c]
			stat = df['stat'][c]
			# float first, because int doesn't recognize floats represented as strings
			ts = int(float(df['client_ts'][c]))
			while ts > 9999999999:
				ts = ts/10
			print(df['client_ts'][c], ts)
			reading = float(df['reading'][c])

			metadata = {}
			for key in df:
				if key not in ['user', 'stat', 'client_ts', 'reading']:
					metadata[key] = df[key][c]
			storeClientEntry(user, stat, ts, reading, metadata)
		except Exception as e:
			print(e)
			print(user, stat, ts, reading)


def storeServerEntries(fname):
	df = pandas.read_csv(fname)
	for c in range(len(df)):
		try:
			print(c)
			user = df['user'][c]
			stat = df['stat'][c]
			ts = int(df['client_ts'][c])
			reading = float(df['reading'][c])
			storeServerEntry(user, stat, ts, reading)
		except Exception as e:
			print(e)
			print("fail")

def storeResultEntries(fname):
	df = pandas.read_csv(fname)
	for c in range(len(df)):
		try:
			user = df['user'][c]
			stat = df['stat'][c]
			ts = int(df['ts'][c])
			reading = float(df['reading'][c])
			storeResultEntry(user, stat, ts, reading)
		except Exception as e:
			print(e)


if __name__ == '__main__':
	storeResultEntries("emission/net/int_service/giles/result_stats_17_dec.csv")
	#storeServerEntries("emission/net/int_service/giles/server_stats_17_dec.csv")
	#storeClientEntries("emission/net/int_service/giles/client_stats_17_dec.csv")