__author__ = 'm088378'

import sys, os, csv
import argparse, ConfigParser
from pymongo import MongoClient
from bson.json_util import dumps
from bson.json_util import loads
from Utils.formatter import *
import logging

# Relative Find of Config File #
dirname = os.path.dirname(os.path.realpath(__file__))
#dirname, filename = os.path.split(os.path.dirname(os.path.realpath(__file__)))
conf_file = os.path.join(dirname, 'config.cfg')
if not os.path.exists(conf_file):
    print("ERROR: Missing Config File")
    sys.exit(1)
config = ConfigParser.ConfigParser()
config.read(conf_file)


# Create CLI Args #
parser = argparse.ArgumentParser(
    description='Load CNV Data downloaded from DGV [public source of normal samples] to Mayo centeralized database.')
parser.add_argument("-f", "--file",
                    dest="input",
                    type=lambda x: is_valid_file(parser, x),
                    help="Text file download from DGV",
                    metavar="FILE", required=True)
parser.add_argument("-m", "--meta",
                    dest="meta",
                    type=lambda x: is_valid_file(parser, x),
                    help="Meta File (webscraped from DGV)",
                    metavar="FILE", required=True)
parser.set_defaults(feature=False)
args = parser.parse_args()


# ADD LOGGING TO THIS SCRIPT #
logger = logging.getLogger('LoadDGV')
hdlr = logging.FileHandler(config.get('logger', 'logfile'))
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.WARNING)


# TODO: make database configureable

####### Setup database connection #######
client = MongoClient('localhost', 27017)
db = client["cnvDB-research"]
cnvSmp = db["meta_HG19"]
cnvClc = db["cnv_HG19"]


####### Detect and Load This Meta Data ######
print "Start Loading Samples..."
sampleMap = {}
aggregateDGVSamplelines = {}
metaCSVFile = csv.reader(open(args.meta), quotechar="'")
# for row in metaCSVFile:
# 	if row[1] == "null":
# 		print row
# 		sys.exit(1)
#
# 	if row[1] in aggregateDGVSamplelines:
# 		aggregateDGVSamplelines[row[1]].append(row)
# 	else:
# 		aggregateDGVSamplelines[row[1]] = [row]
#
# i=1
# for key in aggregateDGVSamplelines:
# 	my_Samp = dict()
# 	my_Samp['sample_name'] = key
# 	my_Samp['data_source'] = "DGV"
# 	my_Samp['bio_sample_type'] = "normal"
# 	my_Samp['entity_type'] = "single"
# 	my_Samp['bio_mutation_type'] = "germline"
# 	tmp = collapseAttributesToList(aggregateDGVSamplelines, key, 0)
# 	if checkToSave(tmp):
# 		my_Samp['studies'] = tmp
#
# 	tmp = collapseAttributesToList(aggregateDGVSamplelines, key, 1)
# 	if checkToSave(tmp):
# 		my_Samp['external_ids'] = tmp
#
# 	tmp = collapseAttributesToSingle(aggregateDGVSamplelines, key, 2)
# 	if checkToSave(tmp):
# 		my_Samp['bio_family_id'] = tmp
#
# 	tmp = collapseAttributesToSingle(aggregateDGVSamplelines, key, 3)
# 	if checkToSave(tmp):
# 		my_Samp['bio_source'] = tmp
#
# 	tmp = collapseAttributesToSingle(aggregateDGVSamplelines, key, 4)
# 	if checkToSave(tmp):
# 		my_Samp['bio_description'] = tmp
#
# 	tmp = collapseAttributesToJoin(aggregateDGVSamplelines, key, 5)
# 	if checkToSave(tmp):
# 		my_Samp['bio_ethnicity'] = tmp
#
# 	tmp = collapseAttributesToJoin(aggregateDGVSamplelines, key, 6)
# 	if checkToSave(tmp):
# 		my_Samp['bio_gender'] = tmp.lower()
#
# 	tmp = collapseAttributesToList(aggregateDGVSamplelines, key, 7)
# 	if checkToSave(tmp):
# 		my_Samp['bio_sample_cohort'] = tmp
#
# 	_id = cnvSmp.insert(loads(dumps(my_Samp)))
# 	sampleMap[key] = _id
#
# 	## Display Ticker
# 	if i%5000==1:
# 		print ""
# 	if i%100==1:
# 		sys.stdout.write('. ')
# 		sys.stdout.flush()
#
# 	i+=1

print "\nLoaded " + str(len(aggregateDGVSamplelines)) + " Samples Meta Data\n\n"



############### LOAD CNV DATA ########################
## ['variantaccession', 'chr', 'start', 'end', 'varianttype', 'variantsubtype', 'reference', 'pubmedid',
##  'method', 'platform', 'mergedvariants', 'supportingvariants', 'mergedorsample', 'frequency', 'samplesize',
##  'observedgains', 'observedlosses', 'cohortdescription', 'genes', 'samples']

sCSVFile = csv.reader(open(args.input), delimiter='\t')
sCSVHeaders = sCSVFile.next()
#print(sCSVHeaders)
#print(sCSVHeaders.index('variantaccession'))

n = 1
for row in sCSVFile:
    # Missing Sample Information
    if row[sCSVHeaders.index('samples')] == "":
        logger.debug("SKIP: [samples] n="+n+", variantaccession="+row[sCSVHeaders.index('variantaccession')])
        continue

    # Skip 'merge' CNV entries
    if row[sCSVHeaders.index('mergedorsample')] == "M":
        logger.debug("SKIP: [mergedorsample] n="+n+", variantaccession="+row[sCSVHeaders.index('variantaccession')])
        continue

    lgr = strToLog2Ratio(row[sCSVHeaders.index('variantsubtype')])


    #TODO make features, from setdiff of header.

    acc = {'accession': row[0], 'reported': row[5], 'pubmed': row[7]}

    vals = {'data_type': row[4] + ".call", 'log2ratio': lgr, 'chr': row[1], 'start': int(row[2]), 'end': int(row[3]),
            'features': acc, 'data_source': "DGV", 'entity_type': "single", 'bio_mutation_type': "germline"}

    samps = [x.strip() for x in row[sCSVHeaders.index('samples')].split(',')]
    for s in samps:
        if s in sampleMap:
            my_Var = vals
            my_Var["sample"] = sampleMap[s]
            my_Var["sample_name"] = s
            cnvClc.insert(loads(dumps(my_Var)))
        else:
            logger.error("No Sample Meta: (" + s + ")")

    # Display Ticker
    if n % 5000 == 1:
        print ""
    if n % 100 == 1:
        sys.stdout.write('. ')
        sys.stdout.flush()
    n += 1

print "\nLoaded " + str(n) + " CNV Records Data\n\n"
