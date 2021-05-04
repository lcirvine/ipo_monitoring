from logging_ipo_dates import logger, consolidate_webscraping_results
import source_reference
import website_scraping
import data_transformation
import entity_mapping
import data_comparison
import file_management
import rpd_creation

logger.info('-' * 100)
source_reference.main()
website_scraping.main()
data_transformation.main()
entity_mapping.main()
df_summary = data_comparison.main()
# Note: email_report is being called separately with another batch file because the run schedule is different
rpd_creation.main()
consolidate_webscraping_results()
file_management.main()
logger.info('-' * 100)
