# Project Name: CNVdb ETL Scripts

This is an backend, infrastructure project to prepare a mongo database. Project linked to <TDB>.

## Installation & Setup (MacOS/Linux)

Install Mongodb. Instructions available here
[https://docs.mongodb.com/manual/tutorial/install-mongodb-on-windows/]
    
Clone this Repository:

    git clone https://github.com/raymond301/CNVdb-Mongo-ETL.git

Configure your Python enviroment: 
   
    #Check Version (2.7.x):
    python --version
    
    #Install Dependancies:
    pip install --upgrade -r Scripts/requirements.txt 
	python Scripts/DGV/load_DGV_all.py -h

Added a development user to database
db.createUser( { user: "devUser", pwd: "etldev", roles: [ "readWrite", "dbAdmin" ] } )

## Install Support Collections

Add Gene Refflat to Mongo


# Obtain Public Data Source

## DGV
### How to get the raw data from DGV Manually
Navigate to: http://dgv.tcag.ca/dgv/app/downloads?ref=GRCh37/hg19
Download: Supporting Variants - 2016-05-15
or

    wget http://dgv.tcag.ca/dgv/docs/GRCh37_hg19_supportingvariants_2016-05-15.txt
    
### How to get the sample level data from DGV Manually
Navigate to: http://dgv.tcag.ca/dgv/app/search?ref=GRCh37/hg19#tabs-view_all_info_sample
Inspect in browser, edit table show element to display all.
Then download via CSV

    Commandline example:
    python Scripts/load_DGV_nonMergedVars.py -f Download/GRCh37_hg19_variants_2016-05-15.txt -m Download/Database\ of\ Genomic\ Variants.csv



### Details on Conversion.
#### Sample Level
Original Column | Converted Field
--------------- | -----------
study | studies
external sample id | external_ids
family id | bio_family_id
source | bio_source
sample description | bio_description
ethnicity | bio_ethnicity
gender | bio_gender
cohort name | bio_sample_cohort



## TCGA

    Commandline example:
    python Python/Load/load_TCGA_all.py -g ../Data/TCGA/TCGA.hg19.June2011.gaf -s ../Data/TCGA/Job-30305677257777413408743051.csv -n /data2/bsi/staff_analysis/m092469/data/CNVdatabase/TCGA_PAN12_data/PANCAN12.Genome_Wide_SNP_6.cna.normal_whitelist -t /data2/bsi/staff_analysis/m092469/data/CNVdatabase/TCGA_PAN12_data/PANCAN12.Genome_Wide_SNP_6.cna.tumor_whitelist



## Wandy
This is a tool built and deployed by our department.

child_file<-"s_011-HLH-001_within_run.id/s_011-HLH-001_within_run.id_zoomout_BinCovg.txt"
father_file<-"s_011-HLH-003_within_run.id/s_011-HLH-003_within_run.id_zoomout_BinCovg.txt"
mother_file<-"s_011-HLH-004_within_run.id/s_011-HLH-004_within_run.id_zoomout_BinCovg.txt"

    python Scripts/load_Wandy_Sample.py -i /Users/m088378/Desktop/ChenCNV/wandy/run.id_wandy_ver0p95/s_011-HLH-001_within_run.id/s_011-HLH-001_within_run.id_zoomout_BinCovg.txt -b 10000 -m s_011-HLH-001
    python Scripts/load_Wandy_Sample.py -i /Users/m088378/Desktop/ChenCNV/wandy/run.id_wandy_ver0p95/s_011-HLH-003_within_run.id/s_011-HLH-003_within_run.id_zoomout_BinCovg.txt -b 10000 -m s_011-HLH-003 




## Contributing

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D

## TODO / Bugs
1. Need to add/fix Logging capabilities

## License

The MIT License (MIT)
=====================

Copyright © `2017` `Raymond Moore`

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the “Software”), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
