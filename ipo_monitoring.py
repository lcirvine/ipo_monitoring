import os
import source_reference
import website_scraping
import data_transformation
import entity_mapping
import data_comparison
import email_report

source_reference.main()
website_scraping.main()
data_transformation.main()
entity_mapping.main()
df_summary = data_comparison.main()
email_report.main(file_attachment=os.path.join(os.getcwd(), 'Results', 'IPO Monitoring.xlsx'),
                  addtl_message=df_summary.to_html(na_rep="", index=False, justify="left"))
