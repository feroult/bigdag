# BigDAG

BigDAG is an open-source project designed to facilitate the management and execution of data workflows using Google BigQuery. It provides a command-line interface (CLI) to automate the creation and management of datasets, tables, views, and external sheets in BigQuery.

## Features

- **DAG Management**: Define and manage Directed Acyclic Graphs (DAGs) for your data workflows.
- **BigQuery Integration**: Seamlessly create and manage datasets, tables, views, and external sheets in Google BigQuery.
- **Command-Line Interface**: Use the CLI to execute workflows, with options for dry runs and verbose output.
- **Automatic Dependency Detection**: Automatically detect and handle dependencies between different data objects in your DAG using the AutoDeps feature. This reduces the need for manual dependency specification and ensures that your data workflows are executed in the correct order.
- **Manual Dependency Specification**: In cases where dependencies cannot be automatically inferred from queries, you can specify them manually using a `deps.yaml` file.

## Installation

To install BigDAG, clone the repository and install it in editable mode:

```bash
git clone https://github.com/yourusername/bigdag.git
cd bigdag
pip install -e .
```

## Requirements

- Google Cloud SDK (gcloud) tools must be installed and configured on your system.

## Usage

You can use the CLI to manage your data workflows. Below is an example of how to use the CLI:

```bash
bigdag --folder path/to/dag --project your_project_id --dataset your_dataset_name
```

### Options

- `--folder`: Path to the DAG folder.
- `--project`: Google Cloud project ID.
- `--dataset`: Name of the dataset.
- `--recreate`: Recreate the dataset if it exists.
- `--dry`: Print the commands without executing them.
- `-v`, `--verbose`: Enable verbose output.

## Example DAG Folder

Here is an example structure of a DAG folder:

```
dag/
├── financial/
│   ├── raw/
│   │   ├── sales.sheet.def.json
│   │   └── sales.sheet.schema.json
│   ├── trusted/
│   │   └── sales.view.sql
│   └── refined/
│       └── monthly_sales.table.sql
└── deps.yaml
```

## BigQuery Tables

After running the following command:

```bash
bigdag --folder dag --dataset sales
```

The following tables and views will be available in the `sales` dataset in BigQuery:

- `financial_raw_sales`: An external table created from the sales sheet definition and schema.
- `financial_trusted_sales`: A view created from the `financial_raw_sales` table.
- `financial_refined_monthly_sales`: A table created from the `financial_trusted_sales` view.

## Testing

To run the tests, use the following command:

```bash
bash run_tests.sh
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. Feel free to use it in your projects, and please give credit where applicable.

## Acknowledgments

Thanks to all contributors and users for their support and feedback.