# BigDAG

BigDAG is an open-source project designed to facilitate the management and execution of data workflows using Google BigQuery. It provides a command-line interface (CLI) to automate the creation and management of datasets, tables, views, and external sheets in BigQuery.

## Features

- **DAG Management**: Define and manage Directed Acyclic Graphs (DAGs) for your data workflows.
- **BigQuery Integration**: Seamlessly create and manage datasets, tables, views, and external sheets in Google BigQuery.
- **Command-Line Interface**: Use the CLI to execute workflows, with options for dry runs and verbose output.
- **Dependency Management**: Automatically handle dependencies between different data objects in your DAG.

## Installation

To install BigDAG, clone the repository and install the dependencies:

```bash
git clone https://github.com/yourusername/bigdag.git
cd bigdag
pip install -r requirements.txt
```

## Usage

You can use the CLI to manage your data workflows. Below is an example of how to use the CLI:

```bash
python -m bigdag.cli --folder path/to/dag --project your_project_id --dataset your_dataset_name
```

### Options

- `--folder`: Path to the DAG folder.
- `--project`: Google Cloud project ID.
- `--dataset`: Name of the dataset.
- `--recreate`: Recreate the dataset if it exists.
- `--dry`: Print the commands without executing them.
- `-v`, `--verbose`: Enable verbose output.

## Testing

To run the tests, use the following command:

```bash
bash run_tests.sh
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any enhancements or bug fixes.

## License

This project is licensed under a permissive license. Feel free to use it in your projects, and please give credit where applicable.

## Acknowledgments

Thanks to all contributors and users for their support and feedback.