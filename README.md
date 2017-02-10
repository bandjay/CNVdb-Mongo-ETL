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


# Obtain Public Data Source

## DGV
### How to get the raw data from DGV Manually
Navigate to: http://dgv.tcag.ca/dgv/app/downloads?ref=GRCh37/hg19
Download: Supporting Variants - 2016-05-15
### How to get the sample level data from DGV Manually
Navigate to: http://dgv.tcag.ca/dgv/app/search?ref=GRCh37/hg19#tabs-view_all_info_sample
Inspect in browser, edit table show element to display all.
Then download via CSV

    Commandline example:
    python load_DGV_all.py -f GRCh37_hg19_supportingvariants_2016-05-15.txt -m SampleList.csv




## Contributing

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D


## License

TODO: Write license (MIT Open License?)
