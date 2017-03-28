__author__ = 'm088378'

import pprint
import sys
import os
import csv
import datetime, time
import argparse, ConfigParser
from pymongo import MongoClient
from bson.json_util import dumps
from bson.json_util import loads
from Utils.formatter import *
import logging

pp = pprint.PrettyPrinter(indent=4)

# Relative Find of Config File #
dirname = os.path.dirname(os.path.realpath(__file__))
#dirname, filename = os.path.split(os.path.dirname(os.path.realpath(__file__)))
conf_file = os.path.join(dirname, 'config.cfg')
#conf_file = os.path.join(dirname, 'config.rcf.cfg')
if not os.path.exists(conf_file):
    print("ERROR: Missing Config File")
    sys.exit(1)
config = ConfigParser.ConfigParser()
config.read(conf_file)


# Create CLI Args #
## -i input (s_011-HLH-004_within_run.id_zoomout_BinCovg.txt) WANDY_HG19 collection? WGS optimazation?
## -b bin size
## -m meta? need a standard? Hard code right now. Accept name for now.
## -s seg file to normal path?
parser = argparse.ArgumentParser(
    description='Load CNV Data downloaded.')
parser.add_argument("-i", "--bin-covg",
                    dest="input",
                    type=lambda x: is_valid_file(parser, x),
                    help="Text file BinCovg",
                    metavar="FILE", required=True)
parser.add_argument("-b", "--bin-size",
                    dest="binsize",
                    type=int,
                    help="bin size",
                    required=True)
parser.add_argument("-s", "--seg-file",
                    dest="seg_file",
                    type=lambda x: is_valid_file(parser, x),
                    help="Segment File",
                    metavar="FILE", required=False)
parser.add_argument("-m", "--meta",
                    dest="samplename")
parser.set_defaults(feature=False)
args = parser.parse_args()


# ADD LOGGING TO THIS SCRIPT #

####### Setup database connection #######
client = MongoClient(config.get('mongodb-conf', 'url'), int(config.get('mongodb-conf', 'port')))
db = client[config.get('mongodb-conf', 'database_name')]
print("Connect to ...",db)
try:
    db.authenticate(config.get('mongodb-conf', 'user'),config.get('mongodb-conf', 'password'))
except:
    sys.exit("Unable to reach Database.")

metaClcNom = "{}_{}".format(config.get('mongodb-conf', 'meta_collection_prefix'), "HG19")
cnvClcNom = "{}_{}".format(config.get('mongodb-conf', 'cnv_collection_prefix'), "HG19")
wgsClcNom = "{}_{}".format(config.get('mongodb-conf', 'wgs_collection_prefix'), "HG19")

cnvSmp = db[metaClcNom]
cnvClc = db[cnvClcNom]
cnvWgs = db[wgsClcNom]


### Will Need to expand this later. For Meta data.
my_Samp={"data_source": "Mayo-Wandy", "entity_type": "single", "sample_name" : args.samplename, "_owner": "user", "_algorithm": "wandy.v1.2"}
_id = cnvSmp.insert(loads(dumps(my_Samp)))

### Add Segments to common collection.
## Ability to filter on the fly ??? Only load significant segments l2r < -1 or l2r > .5 ??
if args.seg_file is not None:
    sCSVFile = csv.reader(open(args.seg_file, 'rU'), delimiter='\t')
    sCSVHeaders = sCSVFile.next()


### Required to Load Wandy Full Data to seperate Collection
defaultRecord={"sample":_id, "binSz": int(args.binsize), "bins":[], "l2r":[]}

sCSVFile = csv.reader(open(args.input, 'rU'), delimiter='\t')
sCSVHeaders = sCSVFile.next()
lastChr="chr1"
workingRecord = defaultRecord
workingRecord["chr"]=lastChr
for row in sCSVFile:
    if lastChr == row[0]:
        workingRecord["bins"].append(int(row[1]))
        workingRecord["l2r"].append(float(row[2]))
    else:
        #pp.pprint(workingRecord)
        print(lastChr)
        cnvWgs.insert(loads(dumps(workingRecord)))
        lastChr=row[0]
        workingRecord = defaultRecord
        workingRecord["chr"]=lastChr

cnvWgs.insert(loads(dumps(workingRecord)))
