import click
import os
from .bigquery_runner import BigQueryRunner

@click.command()
@click.option('--folder', 'dag_folder', required=True, help='Path to the DAG folder.')
@click.option('--project', 'project_id', default=lambda: os.environ.get('BIGDAG_PROJECT_ID', None), help='Google Cloud project ID.')
@click.option('--dataset', 'dataset_name', required=True, help='Name of the dataset.')
@click.option('--recreate', is_flag=True, help='Recreate the dataset if it exists.')
@click.option('--dry', is_flag=True, help='Print the commands without executing them.')
@click.option('-v', '--verbose', is_flag=True, help='Enable verbose output.')
def cli(dag_folder, dataset_name, project_id, recreate, dry, verbose):
    """Entry point for the engine CLI."""
    if project_id is None:
        raise click.UsageError("Project ID must be provided either via the --project option or the BIGDAG_PROJECT_ID environment variable.")
    
    runner = BigQueryRunner(project_id, dataset_name, dag_folder, recreate)
    
    try:
        if dry:
            # Only print the commands without descriptions
            commands = runner.get_commands()
            for cmd_info in commands:
                print(cmd_info['command'])
        else:
            runner.run_commands(dry_run=dry, verbose=verbose)
    except ValueError as e:
        print(e)
    except RuntimeError as e:
        print(e)

if __name__ == "__main__":
    cli()