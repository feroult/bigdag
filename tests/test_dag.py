import unittest
from bigdag.dag import Dag
import os

class TestDag(unittest.TestCase):

    def test_get_execution_order(self):
        dag_directory = 'tests/dag1'
        dag = Dag(dag_directory)
        execution_order = dag.get_execution_order()
        expected_order = [
            'financial_raw_sales',
            'financial_trusted_sales',
            'financial_refined_monthly_sales'
        ]
        self.assertEqual(execution_order, expected_order)

    def test_get_type(self):
        dag_directory = 'tests/dag1'
        dag = Dag(dag_directory)
        
        self.assertEqual(dag.get_type('financial_raw_sales'), 'sheet')
        self.assertEqual(dag.get_type('financial_trusted_sales'), 'view')
        self.assertEqual(dag.get_type('financial_refined_monthly_sales'), 'table')

    def test_get_path_prefix(self):
        dag_directory = 'tests/dag1'
        dag = Dag(dag_directory)
        
        raw_sales_prefix = dag.get_path_prefix('financial_raw_sales')
        trusted_sales_prefix = dag.get_path_prefix('financial_trusted_sales')
        monthly_sales_prefix = dag.get_path_prefix('financial_refined_monthly_sales')

        self.assertEqual(raw_sales_prefix, 'tests/dag1/financial/raw/sales')
        self.assertEqual(trusted_sales_prefix, 'tests/dag1/financial/trusted/sales')
        self.assertEqual(monthly_sales_prefix, 'tests/dag1/financial/refined/monthly_sales')

    def test_auto_deps_dag_execution_order(self):
        dag_directory = 'tests/dag2'
        dag = Dag(dag_directory)
        execution_order = dag.get_execution_order()
        expected_order = [
            'raw_logistics_regional_stock',
            'trusted_logistics_regional_stock_view1',
            'trusted_logistics_regional_stock_view2',
            'refined_logistics_regional_product_table1',
            'refined_logistics_regional_product_table2'
        ]
        self.assertEqual(execution_order, expected_order)

if __name__ == '__main__':
    unittest.main()