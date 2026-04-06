import unittest
from loader.csv_processor import determine_table

class TestCsvProcessor(unittest.TestCase):
    def test_determine_table(self):
        self.assertEqual(determine_table('SCADA_DATA.csv'), 'scada_data')
        self.assertEqual(determine_table('price.csv'), 'price_data')
        self.assertEqual(determine_table('unknown.csv'), 'other_data')

if __name__ == '__main__':
    unittest.main()