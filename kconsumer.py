from kafka import KafkaConsumer
import logging
import gpudb
import time
import json

TOPIC='prices'

def main():
	logger = logging.getLogger('Price consumer')
	logger.setLevel(logging.DEBUG)
	#logging.basicConfig(filename='myapp.log', level=logging.INFO)
	consumer = KafkaConsumer('prices')

	db = gpudb.GPUdb( encoding='BINARY', host = 'kinetica-saif')
	columns = [
		["TIMESTAMP", "long", "timestamp"],
		["tickid", "long", "primary_key"],
		["TICKER", "string", "char32"],
		["DESC", "string", "char32"],
		["PX_LAST", "float"],
		]
	px_table = gpudb.GPUdbTable( columns, 'PRICES', db = db)

	print ("Eating off queue on topic %s " % TOPIC)

	for msg in consumer:
		quote=json.loads(msg.value)
		theTimestamp=int(round(time.time() * 1000))
		logger.critical(quote)
		print (px_table.insert_records(theTimestamp, quote['tickid'], quote['ticker'], quote['desc'], quote['pxlast']))

	print ("DONE Eating off queue on topic %s " % TOPIC)

if __name__ == '__main__':
    main()