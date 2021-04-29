# IPO Monitoring #
This project was created to monitor IPOs from various sources, compare the data to what has already been collected in the PEO-PIPE database, create RPDs for Symbology for all upcoming IPOs and email a report with the results. 

![IPO Monitoring Overview](https://github.factset.com/lirvine/ipo_monitoring/blob/master/images/overview.png)

## What it Does ##

### [IPO Monitoring](https://github.factset.com/lirvine/ipo_monitoring/blob/master/ipo_monitoring.py) ###
There are several stages to IPO monitoring, a batch file runs ipo_monitoring which calls each script. Each piece has try and except blocks so that if one part fails, the other the parts can continue. 

Note: email_report is run by a separate batch file. 

### [Source Reference](https://github.factset.com/lirvine/ipo_monitoring/blob/master/source_reference.py) ###
I gather data from multiple websites and each one is different. I create a JSON file with the details of each website (the url, table elements, etc.) so that I know what to look for on each website.

### [Website Scraping](https://github.factset.com/lirvine/ipo_monitoring/blob/master/website_scraping.py) ###
Using the JSON file, I go to each website and create a data frame with the IPO data I'm interested in. Then each source is saved in a separate CSV file.  

It's expected that the webscraping won't work sometimes. For example, if I'm looking for withdrawn IPOs and there haven't been any recent withdrawn IPOs. Therefore each source is called in a try and except block so that if one fails, the rest of the webscraping can continue. I also create a log to show which sources were successful and which failed. 

There are also a few sources where the websites are so different they can't be scraped in the same way. I've created separate functions for those and the API that I use as a source.

### [Data Transformation](https://github.factset.com/lirvine/ipo_monitoring/blob/master/data_transformation.py) ###
When gathering the data I try to save it exactly as it appears without much manipulation. In this step I try to clean up the data, combine data from all the sources and map the data to common columns.

### [Entity Mapping](https://github.factset.com/lirvine/ipo_monitoring/blob/master/entity_mapping.py) ###
In order to compare the data, I need to find the FactSet entity identifiers for each company name. I use the [Concordance API](https://pages.github.factset.com/ccs-app-dev/concordance-env/_build/html/index.html) to find the entity identifiers. By default I'm only checking new entities that were added. However, I've found that sometimes re-requesting entity identifiers can return a mapping when there was no mapping available previously. So I've added an option to recheck all unmapped entities. 

### [Data Comparison](https://github.factset.com/lirvine/ipo_monitoring/blob/master/data_comparison.py) ###
Now that I have entity identifiers, I want to compare the data to what has been collected by our PEO-PIPE team. I run a query to the PEO-PIPE staging database to retrieve IPOs that were updated in the last 7 days. I add the results to a PEO-PIPE data file in the reference folder. I try to run a small query with updates and add it to existing data.

### [RPD Creation](https://github.factset.com/lirvine/ipo_monitoring/blob/master/rpd_creation.py) ###
Creates an IPO Monitoring RPD for each upcoming IPO using the [RPD API](http://is.factset.com/rpd/api/v2/help/). If an RPD already exists and the IPO was updated (i.e. the IPO date has changed), the original RPD is updated.

### [Email Report](https://github.factset.com/lirvine/ipo_monitoring/blob/master/email_report.py) ###
Emails out the report to the PEO-PIPE team. 

Note: this is not called by ipo_monitoring, there is a separate batch file that runs the script to email the report. The reason is because the report is emailed out to the PEO-PIPE team less frequently than the webscraping/RPD creation. 

### [File Management](https://github.factset.com/lirvine/ipo_monitoring/blob/master/file_management.py) ###
This just does some housekeeping. It deletes old files that are no longer needed.

### [Logging IPO Dates](https://github.factset.com/lirvine/ipo_monitoring/blob/master/logging_ipo_dates.py) ###
Creates logs for IPO Monitoring. Every 30 days the logs are archived. This also creates a summary of how many times each source was scraped successfully.
