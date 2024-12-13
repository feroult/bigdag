import click
import os
from .bigquery_runner import BigQueryRunner

@click.command(context_settings=dict(ignore_unknown_options=True))
@click.option('-f', '--folder', 'dag_folder', required=True, help='Path to the DAG folder.')
@click.option('-p', '--project', 'project_id', default=lambda: os.environ.get('BIGDAG_PROJECT_ID', None), help='Google Cloud project ID.')
@click.option('-d', '--dataset', 'dataset_name', required=True, help='Name of the dataset.')
@click.option('-r', '--recreate', is_flag=True, help='Recreate the dataset if it exists.')
@click.option('-dr', '--dry-run', 'dry', is_flag=True, help='Print the commands without executing them.')
@click.option('-v', '--verbose', is_flag=True, help='Enable verbose output.')
@click.argument('object_ids', nargs=-1)
def cli(dag_folder, dataset_name, project_id, recreate, dry, verbose, object_ids):
    """Entry point for the engine CLI."""
    if project_id is None:
        raise click.UsageError("Project ID must be provided either via the --project option or the BIGDAG_PROJECT_ID environment variable.")
    
    runner = BigQueryRunner(project_id, dataset_name, dag_folder)
    
    try:
        if dry:
            # Only print the commands without descriptions
            if recreate and not object_ids:
                commands = runner.get_recreate_all()
            else:
                commands = runner.get_commands(object_ids=object_ids, recreate=recreate)
            for cmd_info in commands:
                print(cmd_info['command'])
        else:
            if recreate and not object_ids:
                commands = runner.get_recreate_all()
                for cmd_info in commands:
                    print(cmd_info['description'])
                runner.run_commands(dry_run=dry, verbose=verbose)
            else:
                runner.run_commands(dry_run=dry, verbose=verbose)
    except ValueError as e:
        print(e)
    except RuntimeError as e:
        print(e)

if __name__ == "__main__":
    cli()