from logging_ipo_dates import logger  # , consolidate_webscraping_results
import source_reference
import website_scraping
import data_transformation
import data_transformation_db
import entity_mapping
import data_comparison
import file_management
import workflow
import rpd_creation

logger.info('-' * 100)
source_reference.main()
website_scraping.main()
data_transformation.main()
data_transformation_db.main()
entity_mapping.main()
data_comparison.main()
workflow.main()
rpd_creation.main()
# consolidate_webscraping_results()
file_management.main()
logger.info('-' * 100)
