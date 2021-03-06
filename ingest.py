from elasticsearch import Elasticsearch
import configparser
import pandas as pd
import os
import hashlib
from utils.ingest_tools import *
import dateparser
import numpy as np

class TwitterIngest():

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.es_cloud_id = config['es']['es_cloud_id']
        self.es_cloud_user = config['es']['es_cloud_user']
        self.es_cloud_pass = config['es']['es_cloud_pass']
        self.es_cloud_port = config['es']['es_cloud_port']
        self.es = Elasticsearch([self.es_cloud_id],http_auth=(self.es_cloud_user, self.es_cloud_pass), port=self.es_cloud_port,)

    def hash_string(self, string):
        return hashlib.sha256("{}".format(string).encode('utf-8')).hexdigest()


    def ingest(self):
        files = os.listdir('data/')[5:]
        for file in files:
            if file.endswith('.csv'):
                # try:

                df = pd.read_csv('data/' + file).fillna(np.nan).replace([np.nan], [None])
                #preprocessing for booleans
                d = {True: 'TRUE', False: 'FALSE'}
                for col in df:
                    df[col].map(d)
                index = list()
                for t in file.split('_'):
                    if t[0].isdigit():
                        if t[:2] == "20":
                            year = t[:4]
                        elif t[-4:-2] == "20":
                            year = t[-4:]
                        break
                    else:
                        if len(index) < 3:
                            index.append(t)
                if 'user' in file:
                    type = 'users'
                else:
                    type = 'tweets'
                index_name = 'twitter-io-{}-{}-{}'.format(type, '_'.join(index), year)
                failed = 0
                for index, row in df.iterrows():
                    id = self.hash_string("{}:{}".format(file, index))
                    for key in row.keys():
                        if row[key]:
                            if ('count' in key or 'id' in key) and 'date' not in key and 'account' not in key:
                                if not isinstance(row[key], str):
                                    if row[key] == ' ':
                                        row[key] = None
                                    else:
                                        row[key] = int(row[key])
                                else:
                                    row[key] = None
                            if 'date' in key or 'time' in key:
                                row[key] = dateparser.parse(row[key])
                    try:
                        self.es.index(index_name, id = id, body = dict(row))
                    except:
                        failed += 1
                        pass
                    print(index_name, index, '/', len(df), failed, 'failed', end = '\r')
                print('\n')

                #
                # except:
                #     pass

#
if __name__ == '__main__':
    ti = TwitterIngest()
    ti.ingest()
