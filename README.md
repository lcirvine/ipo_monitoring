# IPO Monitoring #
This project was created to monitor IPOs from various sources, compare the data to what has already been collected in the PEO-PIPE database, create RPDs for Symbology for all upcoming IPOs and email a report with the results.

![IPO Monitoring Overview](images/IPO Monitoring Overview.drawio.png)

## What it Does ##

### [IPO Monitoring](https://github.com/lcirvine/ipo_monitoring/blob/master/ipo_monitoring.py) ###
There are several stages to IPO monitoring, a batch file runs ipo_monitoring which calls each script. Each piece has try and except blocks so that if one part fails, the other the parts can continue.

### [Source Reference](https://github.com/lcirvine/ipo_monitoring/blob/master/source_reference.py) ###
I gather data from multiple websites. I create a JSON file with the details of each website (the url, table elements, etc.) so that I know what to look for on each website.

### [Website Scraping](https://github.com/lcirvine/ipo_monitoring/blob/master/website_scraping.py) ###
Using the JSON file, I go to each website and create a data frame with the IPO data I'm interested in. Then each source is saved in a separate table in the database.  

It's expected that the webscraping won't work sometimes. For example, if I'm looking for withdrawn IPOs and there haven't been any recently withdrawn IPOs. Therefore, each source is called in a try and except block so that if one fails, the rest of the webscraping can continue. I also create a log to show which sources were successful and which failed. 

There are also a few sources where the websites are so different they can't be scraped in the same way. I've created separate functions for those and the API that I use as a source.

### [Data Transformation](https://github.com/lcirvine/ipo_monitoring/blob/master/data_transformation.py) ###
When gathering the data I try to save it exactly as it appears without much manipulation. In this step I try to clean up the data, combine data from all the sources and map the data to common columns.

### [Entity Mapping](https://github.com/lcirvine/ipo_monitoring/blob/master/entity_mapping.py) ###
In order to compare the data, I need to find the entity identifiers for each company name. I use an internal API to find the entity identifiers. By default I'm only checking new entities that were added. However, I've found that sometimes re-requesting entity identifiers can return a mapping when there was no mapping available previously. So I've added an option to recheck all unmapped entities. 

### [Data Comparison](https://github.com/lcirvine/ipo_monitoring/blob/master/data_comparison.py) ###
Now that I have entity identifiers, I want to compare the data to what has been collected by our PEO-PIPE team. I run a query to the PEO-PIPE staging database to retrieve IPOs that were updated in the last 7 days. I add the results to a PEO-PIPE data file in the reference folder. I try to run a small query with updates and add it to existing data.

### [RPD Creation](https://github.com/lcirvine/ipo_monitoring/blob/master/rpd_creation.py) ###
Creates an IPO Monitoring RPD for each upcoming IPO. If an RPD already exists and the IPO was updated (i.e. the IPO date has changed), the original RPD is updated.

### [Workflow](https://github.com/lcirvine/ipo_monitoring/blob/master/workflow.py) ###
Creates tasks in the IPOMonitoring workflow in Genesys for each IPO that needs to be reviewed. 

### [File Management](https://github.com/lcirvine/ipo_monitoring/blob/master/file_management.py) ###
This just does some housekeeping. It deletes old files that are no longer needed.

### [Logging IPO Dates](https://github.com/lcirvine/ipo_monitoring/blob/master/logging_ipo_dates.py) ###
Creates a log for IPO Monitoring. At the start of the month the logs are archived.
