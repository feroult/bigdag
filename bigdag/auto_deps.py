import os

class AutoDeps:
    def __init__(self, dag_folder):
        self.dag_folder = dag_folder
        self.dag_objects = self._find_dag_objects()

    def _find_dag_objects(self):
        dag_objects = {}
        for root, _, files in os.walk(self.dag_folder):
            for file in files:
                if file.endswith('.json') or file.endswith('.sql'):
                    name, ext = os.path.splitext(file)
                    # Remove suffixes like .sheet, .view, .table
                    if ext == '.json' or ext == '.sql':
                        name = name.split('.')[0]  # Split at the first dot
                    relative_path = os.path.relpath(root, self.dag_folder)
                    obj_name = f"{relative_path.replace(os.sep, '_')}_{name}"
                    dag_objects[obj_name] = os.path.join(root, file)
        return dag_objects

    def get_object_ids(self):
        return list(set(self.dag_objects.keys()))

    def get_dag(self):
        dependencies = {}
        for obj_id, file_path in self.dag_objects.items():
            if file_path.endswith('.sql'):
                with open(file_path, 'r') as f:
                    sql_content = f.read()
                    for dep_id in self.dag_objects.keys():
                        if dep_id in sql_content:
                            path_parts = obj_id.split('_')
                            zone, subzone, obj_name = path_parts[0], path_parts[1], '_'.join(path_parts[2:])
                            if zone not in dependencies:
                                dependencies[zone] = {}
                            if subzone not in dependencies[zone]:
                                dependencies[zone][subzone] = {}
                            if obj_name not in dependencies[zone][subzone]:
                                dependencies[zone][subzone][obj_name] = []
                            dependencies[zone][subzone][obj_name].append(dep_id)
        return dependencies