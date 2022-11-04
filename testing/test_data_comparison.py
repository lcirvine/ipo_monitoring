import unittest
from data_comparison import DataComparison
import os
from datetime import datetime

dc = DataComparison()
test_start = datetime.now().timestamp()


class DataComparisonTest(unittest.TestCase):

    def test_peo_pipe_data(self):
        self.assertIsNotNone(dc.df_pp, "PEO-PIPE data not available for comparison")

    def test_entity_data(self):
        self.assertIsNotNone(dc.df_e, "Entity data not available for comparison")

    def test_exchange_data(self):
        self.assertIsNotNone(dc.df_s, "IPO data from stock exchanges not available for comparison")

    def test_concat_ticker_exchange(self):
        dc.concatenate_ticker_exchange()
        self.assertIsNotNone(dc.df_pp, "Error when concatenating ticker and exchange in PEO-PIPE data")

    def test_comparison(self):
        df_comp = dc.compare()
        self.assertIsNotNone(df_comp, "Comparison of PEO-PIPE and exchange data could not be made")

    def test_rpd_file(self):
        file = os.path.join(dc.ref_folder, 'IPO Monitoring Data.xlsx')
        file_update_time = os.path.getmtime(file)
        self.assertGreater(file_update_time, test_start, f"A new RPD file has not been created, check {file}")


if __name__ == '__main__':
    unittest.main()
