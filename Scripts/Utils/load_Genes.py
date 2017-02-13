__author__ = 'm088378'

import sys
import gzip

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

i=1
with gzip.open('refFlat.txt.gz','r') as fin:
    for line in fin:
        i+=1
        row = line.rstrip().split("\t")
        exSts = map(int, filter(None, row[9].split(',')))
        exEds = map(int, filter(None, row[10].split(',')))

        acc = {'geneName':row[0], 'tx':row[1], 'chrom':row[2], 'strand':row[3], 'txStart':int(row[4]),
               'txEnd': int(row[5]), 'exonCount': int(row[8]),
               'exonStarts': exSts, 'exonEnds': exEds
               }
        print(acc)
        if i > 10:
            sys.exit()

#f=gzip.open('refFlat.txt.gz','rb')
#file_content=f.read()
#print file_content