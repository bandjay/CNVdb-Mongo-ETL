__author__ = 'm088378'

import sys, gzip
from pymongo import MongoClient
from bson.json_util import dumps
from bson.json_util import loads

# TODO: Add checking for existing file - allow to pass as Parameter.

#urlFile="http://hgdownload.cse.ucsc.edu/goldenPath/hg19/database/refFlat.txt.gz"
#urllib.urlretrieve(urlFile, "refFlat.txt.gz")

# TODO: add config to connect Mongo


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

####### Setup database connection #######
client = MongoClient('localhost', 27017)
db = client["cnvDB-research"]
gnCollection = db["gene_HG19"]


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

        #if i > 10:
        #    sys.exit()

print("Loaded "+str(i)+" Genes From Refflat")

## TODO: if downloaded refflat - clean up file afterwards.