import importlib.util
import sys
import types
from pathlib import Path


def install_airflow_stubs():
    if importlib.util.find_spec('airflow') is not None:
        return

    airflow = types.ModuleType('airflow')
    airflow.__path__ = []
    sys.modules['airflow'] = airflow

    dag_processing = types.ModuleType('airflow.dag_processing')
    dag_processing.__path__ = []
    sys.modules['airflow.dag_processing'] = dag_processing

    importers = types.ModuleType('airflow.dag_processing.importers')
    sys.modules['airflow.dag_processing.importers'] = importers

    class AbstractDagImporter:
        @classmethod
        def supported_extensions(cls):
            return []

        def can_handle(self, path):
            return Path(path).suffix.lower() in {
                extension.lower() for extension in self.supported_extensions()
            }

        def get_relative_path(self, file_path, bundle_path=None):
            if bundle_path:
                try:
                    return str(Path(file_path).relative_to(bundle_path))
                except ValueError:
                    pass
            return str(file_path)

    class DagImportError:
        def __init__(self, file_path, message, error_type, stacktrace):
            self.file_path = file_path
            self.message = message
            self.error_type = error_type
            self.stacktrace = stacktrace

    class DagImportResult:
        def __init__(self, file_path):
            self.file_path = file_path
            self.dags = []
            self.errors = []

    class _ImporterRegistry:
        def __init__(self):
            self._importers = []

        def get_importer(self, file_path):
            suffix = Path(file_path).suffix.lower()
            for importer in self._importers:
                if suffix in {ext.lower() for ext in importer.supported_extensions()}:
                    return importer
            return None

        def register(self, importer):
            self._importers.append(importer)

        def list_dag_files(self, directory):
            results = []
            for importer in self._importers:
                results.extend(importer.list_dag_files(directory))
            return results

    _registry = _ImporterRegistry()

    def get_importer_registry():
        return _registry

    importers.AbstractDagImporter = AbstractDagImporter
    importers.DagImportError = DagImportError
    importers.DagImportResult = DagImportResult
    importers.get_importer_registry = get_importer_registry

    manager = types.ModuleType('airflow.dag_processing.manager')
    sys.modules['airflow.dag_processing.manager'] = manager

    class DagFileProcessorManager:
        _excalidraw_discovery_patched = False

        def _find_files_in_bundle(self, bundle):
            return []

    manager.DagFileProcessorManager = DagFileProcessorManager

    sdk = types.ModuleType('airflow.sdk')
    sys.modules['airflow.sdk'] = sdk

    class DAG:
        def __init__(self, dag_id, schedule, start_date, catchup, params):
            self.dag_id = dag_id
            self.schedule = schedule
            self.start_date = start_date
            self.catchup = catchup
            self.params = params
            self.task_dict = {}

    sdk.DAG = DAG

    providers = types.ModuleType('airflow.providers')
    providers.__path__ = []
    sys.modules['airflow.providers'] = providers

    standard = types.ModuleType('airflow.providers.standard')
    standard.__path__ = []
    sys.modules['airflow.providers.standard'] = standard

    operators_pkg = types.ModuleType('airflow.providers.standard.operators')
    operators_pkg.__path__ = []
    sys.modules['airflow.providers.standard.operators'] = operators_pkg

    class BaseOperator:
        def __init__(self, task_id=None, dag=None, group_id=None, **kwargs):
            self.task_id = task_id
            self.group_id = group_id
            self.dag = dag
            self.downstream_task_ids = set()
            for key, value in kwargs.items():
                setattr(self, key, value)
            if dag is not None and task_id is not None:
                dag.task_dict[task_id] = self

        def __rshift__(self, other):
            self.downstream_task_ids.add(other.task_id)
            return other

    empty_module = types.ModuleType('airflow.providers.standard.operators.empty')
    empty_module.EmptyOperator = type('EmptyOperator', (BaseOperator,), {})
    sys.modules['airflow.providers.standard.operators.empty'] = empty_module

    bash_module = types.ModuleType('airflow.providers.standard.operators.bash')
    bash_module.BashOperator = type('BashOperator', (BaseOperator,), {})
    sys.modules['airflow.providers.standard.operators.bash'] = bash_module

    python_module = types.ModuleType('airflow.providers.standard.operators.python')
    python_module.PythonOperator = type('PythonOperator', (BaseOperator,), {})
    sys.modules['airflow.providers.standard.operators.python'] = python_module