import unittest
from bigdag.auto_deps import AutoDeps

class TestAutoDeps(unittest.TestCase):

    def test_get_object_ids(self):
        dag_folder = 'tests/dag1'
        expected_object_ids = [
            'financial_raw_sales',
            'financial_trusted_sales',
            'financial_refined_monthly_sales'
        ]
        auto_deps = AutoDeps(dag_folder)
        object_ids = auto_deps.get_object_ids()
        self.assertEqual(sorted(object_ids), sorted(expected_object_ids))

    def test_get_dag(self):
        dag_folder = 'tests/dag1'
        expected_dependencies = {
            'financial': {
                'trusted': {
                    'financial_trusted_sales': ['financial_raw_sales']
                },
                'refined': {
                    'financial_refined_monthly_sales': ['financial_trusted_sales']
                }
            }
        }
        auto_deps = AutoDeps(dag_folder)
        dependencies = auto_deps.get_dag()
        self.assertEqual(dependencies, expected_dependencies)

if __name__ == '__main__':
    unittest.main()