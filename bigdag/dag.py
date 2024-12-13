import os
import yaml
import networkx as nx
from bigdag.auto_deps import AutoDeps

class Dag:
    def __init__(self, dag_folder):
        self.dag_folder = dag_folder
        self.deps_file = os.path.join(dag_folder, 'deps.yaml')
        self.dag_objects = self._find_dag_objects()
        self.dependencies = self._load_dependencies()
        self.auto_deps = AutoDeps(dag_folder).get_dag()
        self._merge_dependencies()

    def _load_dependencies(self):
        if not os.path.exists(self.deps_file):
            return {}
        with open(self.deps_file, 'r') as file:
            dependencies = yaml.safe_load(file)
            if not dependencies:
                dependencies = {}
            return dependencies

    def _merge_dependencies(self):
        def merge_dicts(dict1, dict2):
            for key, value in dict2.items():
                if key in dict1:
                    if isinstance(value, dict):
                        merge_dicts(dict1[key], value)
                    else:
                        dict1[key] = list(set(dict1[key] + value))
                else:
                    dict1[key] = value

        merge_dicts(self.dependencies, self.auto_deps)

    def _find_dag_objects(self):
        dag_objects = {}
        for root, _, files in os.walk(self.dag_folder):
            for file in files:
                if file.endswith('.json') or file.endswith('.sql'):
                    name, ext = os.path.splitext(file)
                    relative_path = os.path.relpath(root, self.dag_folder)
                    obj_name = f"{relative_path.replace(os.sep, '_')}_{name.split('.')[0]}"
                    obj_type = self._determine_type(name, ext)
                    file_path = os.path.join(root, file)
                    dag_objects[obj_name] = (obj_type, file_path)
        return dag_objects

    def _determine_type(self, name, extension):
        if extension == '.json':
            return 'sheet'
        elif extension == '.sql':
            if 'view' in name:
                return 'view'
            elif 'table' in name:
                return 'table'
        return None

    def _build_dependency_graph(self):
        graph = nx.DiGraph()

        # Add nodes for all DAG objects
        for obj_name in self.dag_objects.keys():
            graph.add_node(obj_name)

        # Add edges based on dependencies
        def add_dependencies(dependencies, prefix=''):
            for key, value in dependencies.items():
                current_prefix = f"{prefix}_{key}" if prefix else key
                if isinstance(value, dict):
                    add_dependencies(value, current_prefix)
                else:
                    for dep in value:
                        if not graph.has_edge(dep, current_prefix):
                            graph.add_edge(dep, current_prefix)

        add_dependencies(self.dependencies)

        return graph

    def get_execution_order(self):
        graph = self._build_dependency_graph()
        try:
            return list(nx.topological_sort(graph))
        except nx.NetworkXUnfeasible as e:
            raise ValueError("Cycle detected in dependencies") from e

    def get_type(self, object_id):
        return self.dag_objects.get(object_id, (None,))[0]

    def get_path_prefix(self, object_id):
        file_path = self.dag_objects.get(object_id, (None, None))[1]
        if file_path:
            # Extract everything before the first dot in the file name
            base_name = os.path.basename(file_path)
            prefix = base_name.split('.')[0]
            return os.path.join(os.path.dirname(file_path), prefix)
        return None

def main():
    dag_directory = 'dag'
    dag = Dag(dag_directory)
    
    try:
        sorted_order = dag.get_execution_order()
        print("Order of objects to be created:")
        for obj in sorted_order:
            print(obj)
    except ValueError as e:
        print(e)

if __name__ == "__main__":
    main()