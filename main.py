from logging_ipo_dates import logger
import source_reference
import website_scraping
import data_transformation_db
import entity_mapping
import data_comparison
import file_management
import workflow
import rpd_creation

logger.info('-' * 100)

try:
    source_reference.main()
except Exception as e:
    print(e)

try:
    website_scraping.main()
except Exception as e:
    print(e)

try:
    data_transformation_db.main()
except Exception as e:
    print(e)

try:
    entity_mapping.main()
except Exception as e:
    print(e)

try:
    data_comparison.main()
except Exception as e:
    print(e)

try:
    workflow.main()
except Exception as e:
    print(e)

try:
    rpd_creation.main()
except Exception as e:
    print(e)

try:
    file_management.main()
except Exception as e:
    print(e)

logger.info('-' * 100)

