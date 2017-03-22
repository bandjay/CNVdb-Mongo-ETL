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
parser = argparse.ArgumentParser(
    description='Load CNV Data downloaded from DGV [public source of normal samples] to Mayo centeralized database.')
parser.add_argument("-v", "--var-file",
                    dest="input",
                    type=lambda x: is_valid_file(parser, x),
                    help="Text file download from DGV",
                    metavar="FILE", required=True)
parser.add_argument("-s", "--support-var-file",
                    dest="input2",
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
#logger.setLevel(logging.WARNING)
ts = time.time()
logger.info(datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))

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

cnvSmp = db[metaClcNom]
cnvClc = db[cnvClcNom]

# should log this
#print (db)


####### Detect and Load This Meta Data ######
print "Start Loading Samples..."
sampleMap = {}
aggregateDGVSamplelines = {}
metaCSVFile = csv.reader(open(args.meta), quotechar="'")
for row in metaCSVFile:
    if row[1] == "null":
        print row
        sys.exit(1)

    if row[1] in aggregateDGVSamplelines:
        aggregateDGVSamplelines[row[1]].append(row)
    else:
        aggregateDGVSamplelines[row[1]] = [row]

i=1
for key in aggregateDGVSamplelines:

    if i > 1:
        continue

    my_Samp = dict()
    my_Samp['sample_name'] = key
    my_Samp['data_source'] = "DGV"
    my_Samp['bio_sample_type'] = "normal"
    my_Samp['entity_type'] = "single"
    my_Samp['bio_mutation_type'] = "germline"
    tmp = collapseAttributesToList(aggregateDGVSamplelines, key, 0)
    if checkToSave(tmp):
        my_Samp['studies'] = tmp

    tmp = collapseAttributesToList(aggregateDGVSamplelines, key, 1)
    if checkToSave(tmp):
        my_Samp['external_ids'] = tmp

    tmp = collapseAttributesToSingle(aggregateDGVSamplelines, key, 2)
    if checkToSave(tmp):
        my_Samp['bio_family_id'] = tmp

    tmp = collapseAttributesToSingle(aggregateDGVSamplelines, key, 3)
    if checkToSave(tmp):
        my_Samp['bio_source'] = tmp

    tmp = collapseAttributesToSingle(aggregateDGVSamplelines, key, 4)
    if checkToSave(tmp):
        my_Samp['bio_description'] = tmp

    tmp = collapseAttributesToJoin(aggregateDGVSamplelines, key, 5)
    if checkToSave(tmp):
        my_Samp['bio_ethnicity'] = tmp

    tmp = collapseAttributesToJoin(aggregateDGVSamplelines, key, 6)
    if checkToSave(tmp):
        my_Samp['bio_gender'] = tmp.lower()

    tmp = collapseAttributesToList(aggregateDGVSamplelines, key, 7)
    if checkToSave(tmp):
        my_Samp['bio_sample_cohort'] = tmp

    _id = cnvSmp.insert(loads(dumps(my_Samp)))
    sampleMap[key] = _id

    #pp.pprint(my_Samp)
    #print(_id,key)

    #time.sleep(1)
    #input("Press Enter to continue...")
    ## Display Ticker
    # if i%5000==1:
    #     print ""
    # if i%100==1:
    #     sys.stdout.write('. ')
    #     sys.stdout.flush()
    i+=1


print "\nLoaded " + str(len(aggregateDGVSamplelines)) + " Samples Meta Data\n\n"



############### LOAD CNV DATA ########################
## ['variantaccession', 'chr', 'start', 'end', 'varianttype', 'variantsubtype', 'reference', 'pubmedid',
##  'method', 'platform', 'mergedvariants', 'supportingvariants', 'mergedorsample', 'frequency', 'samplesize',
##  'observedgains', 'observedlosses', 'cohortdescription', 'genes', 'samples']

sCSVFile = csv.reader(open(args.input, 'rU'), delimiter='\t')
sCSVHeaders = sCSVFile.next()
#pp.pprint(sCSVHeaders)


skipHead = ['chr','start','end','varianttype', 'mergedorsample','samples','genes']
featureHead = list(set(sCSVHeaders) - set(skipHead))

n = 0
for row in sCSVFile:
    n += 1

    #print(sCSVHeaders)
    #print(row)
    if n > 20:
        continue

    # Missing Sample Information
    if row[sCSVHeaders.index('samples')] == "":
        logger.debug("SKIP: [samples] n="+str(n)+", variantaccession="+row[0])
        print("SKIP: [samples] n="+str(n)+", variantaccession="+row[0]+" : "+"-".join(row))
        continue

    # Skip 'merge' CNV entries
    if row[sCSVHeaders.index('mergedorsample')] == "M":
        logger.debug("SKIP: [mergedorsample] n="+str(n)+", variantaccession="+row[0])
        logger.debug("SKIP: [mergedorsample] n="+str(n)+", variantaccession="+row[0])
        continue

    lgr = strToLog2Ratio(row[sCSVHeaders.index('variantsubtype')])

    acc={}
    for fhd in featureHead:
        if row[sCSVHeaders.index(fhd)] is not None or row[sCSVHeaders.index(fhd)] is not '':
            acc[fhd] = row[sCSVHeaders.index(fhd)]

    enType = "single"
    numSamples = 1
    if row[sCSVHeaders.index('mergedorsample')].lower() == "m":
        enType = "multi"
        numSamples = row[sCSVHeaders.index('samples')].count(",")+1


    vals = {'data_type': row[4] + ".call", 'log2ratio': lgr, 'chr': row[1], 'start': int(row[2]), 'end': int(row[3]),
            'features': acc, 'data_source': "DGV", 'entity_type': enType, 'bio_mutation_type': "germline", "n":numSamples}



    #pp.pprint(vals)

    samps = [x.strip() for x in row[sCSVHeaders.index('samples')].split(',')]
    #pp.pprint(samps)

    for s in samps:
        if s in sampleMap:
            print(s,sampleMap[s])
            my_Var = vals
            my_Var["sample"] = sampleMap[s]
            my_Var["sample_name"] = s
            cnvClc.insert(loads(dumps(my_Var)))
        else:
            logger.error("No Sample Meta: (" + s + ")")



    #pp.pprint(my_Samp)
    #print(_id,key)

    #time.sleep(2)


    # Display Ticker
    #if n % 5000 == 1:
    #    print str(n)
    #if n % 100 == 1:
    #    sys.stdout.write('. ')
    #    sys.stdout.flush()

print "\nLoaded " + str(n) + " CNV Variant Records Data\n\n"











############### LOAD CNV DATA ########################
## ['variantaccession', 'chr', 'start', 'end', 'varianttype', 'variantsubtype', 'reference', 'pubmedid',
##  'method', 'platform', 'mergedvariants', 'supportingvariants', 'mergedorsample', 'frequency', 'samplesize',
##  'observedgains', 'observedlosses', 'cohortdescription', 'genes', 'samples']

sCSVFile = csv.reader(open(args.input2, 'rU'), delimiter='\t')
sCSVHeaders = sCSVFile.next()
#pp.pprint(sCSVHeaders)


skipHead = ['chr','start','end','varianttype', 'mergedorsample','samples','genes']
featureHead = list(set(sCSVHeaders) - set(skipHead))

n = 1
for row in sCSVFile:
    print(sCSVHeaders)
    print(row)

    # Missing Sample Information
    if row[sCSVHeaders.index('samples')] == "":
        logger.debug("SKIP: [samples] n="+str(n)+", variantaccession="+row[0])
        print("SKIP: [samples] n="+str(n)+", variantaccession="+row[0])
        continue

    # Skip 'merge' CNV entries
    if row[sCSVHeaders.index('mergedorsample')] == "M":
        logger.debug("SKIP: [mergedorsample] n="+str(n)+", variantaccession="+row[0])
        logger.debug("SKIP: [mergedorsample] n="+str(n)+", variantaccession="+row[0])
        continue

    lgr = strToLog2Ratio(row[sCSVHeaders.index('variantsubtype')])

    acc={}
    for fhd in featureHead:
        if row[sCSVHeaders.index(fhd)] is not None or row[sCSVHeaders.index(fhd)] is not '':
            acc[fhd] = row[sCSVHeaders.index(fhd)]

    enType = "single"
    numSamples = 1
    if row[sCSVHeaders.index('mergedorsample')].lower() == "m":
        enType = "multi"
        numSamples = row[sCSVHeaders.index('samples')].count(",")+1


    vals = {'data_type': row[4] + ".call", 'log2ratio': lgr, 'chr': row[1], 'start': int(row[2]), 'end': int(row[3]),
            'features': acc, 'data_source': "DGV", 'entity_type': enType, 'bio_mutation_type': "germline", "n":numSamples}



    pp.pprint(vals)

    samps = [x.strip() for x in row[sCSVHeaders.index('samples')].split(',')]
    #pp.pprint(samps)

    for s in samps:
        if s in sampleMap:
            print(s,sampleMap[s])
            my_Var = vals
            my_Var["sample"] = sampleMap[s]
            my_Var["sample_name"] = s
            cnvClc.insert(loads(dumps(my_Var)))
        else:
            logger.error("No Sample Meta: (" + s + ")")



    #pp.pprint(my_Samp)
    #print(_id,key)

    #time.sleep(2)


    # Display Ticker
    #if n % 5000 == 1:
    #    print str(n)
    #if n % 100 == 1:
    #    sys.stdout.write('. ')
    #    sys.stdout.flush()
    n += 1

print "\nLoaded " + str(n) + " CNV Supporting Variant Records Data\n\n"
