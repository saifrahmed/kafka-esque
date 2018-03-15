from kafka import KafkaProducer
import random
import decimal
import requests
import json
import logging

TOPIC='prices'



SRC_URL='https://finance.yahoo.com/currencies/'
SOI=[	('ETHUSD=X','Ethereum-USD'),
		('BTC-USD','Bitcoin-USD'),
		('GBPUSD=X','GBP-USD'),
		('^DJI','Dow Jones Industrial Avg'),
		('^VIX','VIX Volatility Index'),
		('^GSPC','S&P 500'),
		('EURUSD=X','EUR-USD')
		]
TOPIC='prices'


PX_SIMULATED={}


def getMarketPrices():
	TO_RET={}
	url = requests.get(SRC_URL)
	htmltext = url.text
	STRING_INDEX_PX_JSON_START=htmltext.find("root.App.main =")
	STRING_INDEX_PX_JSON_END=htmltext.find("}(this));")
	STRING_PX_JSON = htmltext[STRING_INDEX_PX_JSON_START+16:STRING_INDEX_PX_JSON_END-2]

	j=json.loads(STRING_PX_JSON)

	for (ticker,desc) in SOI:
		TO_RET[ticker]=j['context']['dispatcher']['stores']['StreamDataStore']['quoteData'][ticker]['regularMarketPrice']['raw']
	return TO_RET

def main():
	logger = logging.getLogger('Price producer')
	logger.setLevel(logging.DEBUG)

	logger.critical("Writing to topic %s " % TOPIC)

	producer = KafkaProducer()

	tick_id=100000
	while True:
		PX_SIMULATED=getMarketPrices()
		for _ in range(100000):
			

			for (ticker,desc) in SOI:
				tick_id = tick_id + 1
				if tick_id%5000==0:
					logger.critical("Produced item " + str(tick_id))


				# fuzzer
				PX_SIMULATED[ticker]=PX_SIMULATED[ticker]*random.uniform(.999,1.001)

				tick = {}
				tick['tickid'] = tick_id
				tick['ticker'] = ticker
				tick['desc'] = desc
				tick['pxlast'] = PX_SIMULATED[ticker]
				future = producer.send(TOPIC, str(json.dumps(tick)).encode())
				result = future.get(timeout=60)

if __name__ == '__main__':
    main()			