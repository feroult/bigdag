import os
import subprocess
import time
from .dag import Dag
from .utils import readfile

class BigQueryRunner:
    def __init__(self, project_id, dataset_name, dag_folder, recreate=False):
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.dag_folder = dag_folder
        self.recreate = recreate
        self.dag = Dag(dag_folder)

    def _apply_template(self, query):
        # Replace placeholders and escape backticks
        return query.replace("{{project_id}}", self.project_id).replace("{{dataset}}", self.dataset_name).replace("`", "\\`")

    def get_commands(self):
        commands = []
        
        if self.recreate:
            # Command to remove the dataset
            commands.append({
                'command': f"bq rm --recursive --force --project_id {self.project_id} --dataset {self.dataset_name}",
                'description': f"dropping dataset {self.dataset_name}"
            })

        # Command to create the dataset
        commands.append({
            'command': f"bq mk --project_id {self.project_id} --dataset {self.dataset_name}",
            'description': f"creating dataset {self.dataset_name}"
        })

        # Commands to create each object in the correct order
        execution_order = self.dag.get_execution_order()
        for obj_id in execution_order:
            obj_type = self.dag.get_type(obj_id)
            path_prefix = self.dag.get_path_prefix(obj_id)
            if obj_type == 'sheet':
                schema_file = f"{path_prefix}.sheet.schema.json"
                def_file = f"{path_prefix}.sheet.def.json"
                commands.append({
                    'command': f"bq mk --project_id {self.project_id} --schema {schema_file} --external_table_definition {def_file} {self.dataset_name}.{obj_id}",
                    'description': f"creating spreadsheet {obj_id}"
                })
            elif obj_type == 'view':
                sql_file = f"{path_prefix}.view.sql"
                view_query = readfile(sql_file)
                view_query = self._apply_template(view_query)
                commands.append({
                    'command': f"bq mk --project_id {self.project_id} --use_legacy_sql=false --view \"{view_query}\" {self.dataset_name}.{obj_id}",
                    'description': f"creating view {obj_id}"
                })
            elif obj_type == 'table':
                sql_file = f"{path_prefix}.table.sql"
                table_query = readfile(sql_file)
                table_query = self._apply_template(table_query)
                commands.append({
                    'command': f"bq query --project_id {self.project_id} --use_legacy_sql=false --replace --destination_table={self.project_id}:{self.dataset_name}.{obj_id} \"{table_query}\"",
                    'description': f"creating table {obj_id}"
                })

        return commands

    def run_commands(self, dry_run=False, verbose=False):
        commands = self.get_commands()

        for cmd_info in commands:
            command = cmd_info['command']
            description = cmd_info['description']
            print(f"{description} ", end='', flush=True)

            if not dry_run:
                start_time = time.time()
                result = subprocess.run(command, shell=True, capture_output=True, text=True)
                elapsed_time = time.time() - start_time
                if result.returncode != 0:
                    if verbose:
                        print(f"\nCommand: {command}")
                        print(f"Error: {result.stderr}")
                    raise RuntimeError(f"Command failed: {command}")
                if verbose:
                    print(f"\nCommand: {command}")
                    print(result.stdout)
                else:
                    print(f"[ok] {elapsed_time:.2f} secs")