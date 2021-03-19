import os
import source_reference
import website_scraping
import data_transformation
import email_report

source_reference.main()
website_scraping.main()
data_transformation.main()
email_report.main(os.path.join(os.getcwd(), 'Results', 'All IPOs.xlsx'))
