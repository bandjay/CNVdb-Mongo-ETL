__author__ = 'm088378'

import sys, os, gzip, ConfigParser
from pymongo import MongoClient
from bson.json_util import dumps
from bson.json_util import loads

dirname, filename = os.path.split(os.path.dirname(os.path.realpath(__file__)))
conf_file = os.path.join(dirname,'config.cfg')
if not os.path.exists(conf_file):
    print("ERROR: Missing Config File")
    sys.exit(1)

config = ConfigParser.ConfigParser()
config.read(conf_file)
dbURL = config.get('mongodb-conf', 'url')
dbPort = int(config.get('mongodb-conf', 'port'))
dbName = config.get('mongodb-conf', 'database_name')
dbGeneCollection = "{}_{}".format(config.get('mongodb-conf', 'gene_collection_prefix'), "HG19")

# TODO: Add checking for existing file - allow to pass as Parameter.
#urlFile="http://hgdownload.cse.ucsc.edu/goldenPath/hg19/database/refFlat.txt.gz"
#urllib.urlretrieve(urlFile, "refFlat.txt.gz")


# string  geneName;           "Name of gene as it appears in Genome Browser."
# string  name;               "Name of gene"
# string  chrom;              "Chromosome name"
# char[1] strand;             "+ or - for strand"
# uint    txStart;            "Transcription start position"
# uint    txEnd;              "Transcription end position"
# uint    cdsStart;           "Coding region start"
# uint    cdsEnd;             "Coding region end"
# uint    exonCount;          "Number of exons"
# uint[exonCount] exonStarts; "Exon start positions"
# uint[exonCount] exonEnds;   "Exon end positions"

# TODO: add config to connect Mongo

####### Setup database connection #######
client = MongoClient(dbURL, dbPort)
db = client[dbName]
gnCollection = db[dbGeneCollection]


i=1
geneList = []
with gzip.open('refFlat.txt.gz','r') as fin:
    for line in fin:
        i+=1
        row = line.rstrip().split("\t")

        # WILL ONLY ACCEPT 1st appearance of a Gene Symbol.
        if row[0] in geneList:
            continue
        else:
            geneList.append(row[0])

        ### Create Formatted Object
        exSts = map(int, filter(None, row[9].split(',')))
        exEds = map(int, filter(None, row[10].split(',')))

        acc = {'geneName':row[0], 'tx':row[1], 'chrom':row[2], 'strand':row[3], 'txStart':int(row[4]),
               'txEnd': int(row[5]), 'exonCount': int(row[8]),
               'exonStarts': exSts, 'exonEnds': exEds
               }
        #print(acc)
        gnCollection.insert(loads(dumps(acc)))

        if i > 10:
            sys.exit()

print("Loaded "+str(i)+" Genes From Refflat")

## TODO: if downloaded refflat - clean up file afterwards.