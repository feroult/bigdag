import unittest
from bigdag.bigquery_runner import BigQueryRunner
from bigdag.utils import readfile

class TestBigQueryRunner(unittest.TestCase):

    def test_get_commands(self):
        project_id = 'test_project'
        dataset_name = 'test_dataset'
        dag_directory = 'tests/dag1'
        runner = BigQueryRunner(project_id, dataset_name, dag_directory)
        commands = runner.get_commands()

        sales_view_query = readfile('tests/dag1/financial/trusted/sales.view.sql')
        monthly_sales_table_query = readfile('tests/dag1/financial/refined/monthly_sales.table.sql')
        monthly_sales_table_query = monthly_sales_table_query.replace("{{project_id}}", project_id).replace("{{dataset}}", dataset_name).replace("`", "\\`")

        expected_commands = [
            {
                'command': "bq mk --project_id test_project --dataset test_dataset",
                'description': "creating dataset test_dataset"
            },
            {
                'command': "bq mk --project_id test_project --schema tests/dag1/financial/raw/sales.sheet.schema.json --external_table_definition tests/dag1/financial/raw/sales.sheet.def.json test_dataset.financial_raw_sales",
                'description': "creating spreadsheet financial_raw_sales"
            },
            {
                'command': f"bq mk --project_id test_project --use_legacy_sql=false --view \"{sales_view_query}\" test_dataset.financial_trusted_sales",
                'description': "creating view financial_trusted_sales"
            },
            {
                'command': f"bq query --project_id test_project --use_legacy_sql=false --replace --destination_table=test_project:test_dataset.financial_refined_monthly_sales \"{monthly_sales_table_query}\"",
                'description': "creating table financial_refined_monthly_sales"
            }
        ]

        self.assertEqual(commands, expected_commands)

    def test_get_commands_with_recreate(self):
        project_id = 'test_project'
        dataset_name = 'test_dataset'
        dag_directory = 'tests/dag1'
        runner = BigQueryRunner(project_id, dataset_name, dag_directory, recreate=True)
        commands = runner.get_commands()

        sales_view_query = readfile('tests/dag1/financial/trusted/sales.view.sql')
        monthly_sales_table_query = readfile('tests/dag1/financial/refined/monthly_sales.table.sql')
        monthly_sales_table_query = monthly_sales_table_query.replace("{{project_id}}", project_id).replace("{{dataset}}", dataset_name).replace("`", "\\`")

        expected_commands = [
            {
                'command': "bq rm --recursive --force --project_id test_project --dataset test_dataset",
                'description': "dropping dataset test_dataset"
            },
            {
                'command': "bq mk --project_id test_project --dataset test_dataset",
                'description': "creating dataset test_dataset"
            },
            {
                'command': "bq mk --project_id test_project --schema tests/dag1/financial/raw/sales.sheet.schema.json --external_table_definition tests/dag1/financial/raw/sales.sheet.def.json test_dataset.financial_raw_sales",
                'description': "creating spreadsheet financial_raw_sales"
            },
            {
                'command': f"bq mk --project_id test_project --use_legacy_sql=false --view \"{sales_view_query}\" test_dataset.financial_trusted_sales",
                'description': "creating view financial_trusted_sales"
            },
            {
                'command': f"bq query --project_id test_project --use_legacy_sql=false --replace --destination_table=test_project:test_dataset.financial_refined_monthly_sales \"{monthly_sales_table_query}\"",
                'description': "creating table financial_refined_monthly_sales"
            }
        ]

        self.assertEqual(commands, expected_commands)

if __name__ == '__main__':
    unittest.main()