import ast
import builtins
import datetime
import importlib
import inspect
import json
import os
import re
import traceback
from ast import literal_eval
from pathlib import Path

from airflow.dag_processing.importers import (
    AbstractDagImporter,
    DagImportError,
    DagImportResult,
    get_importer_registry,
)
from airflow.sdk import DAG


STANDARD_OPERATORS_PACKAGE = 'airflow.providers.standard.operators'
DEFAULT_OPERATOR_CLASS_PATH = f'{STANDARD_OPERATORS_PACKAGE}.empty.EmptyOperator'
EXCALIDRAW_SOURCE_PARAM_KEY = 'excalidraw_source_file'
EXCALIDRAW_BUILTINS = dict(vars(builtins))


def get_active_elements(elements):
    return [elem for elem in elements if not elem.get('isDeleted', False)]


def parse_value(raw_value):
    raw_value = raw_value.strip()
    if not raw_value:
        return raw_value

    try:
        return literal_eval(raw_value)
    except (ValueError, SyntaxError):
        return raw_value


def evaluate_value(raw_value, import_namespace=None):
    parsed_value = parse_value(raw_value)
    if parsed_value != raw_value:
        return parsed_value

    if import_namespace:
        try:
            return eval(raw_value, {'__builtins__': EXCALIDRAW_BUILTINS}, import_namespace)
        except Exception:
            pass

    return raw_value


def execute_python_value(raw_value, import_namespace=None):
    code = raw_value.strip()
    if not code:
        return ''

    try:
        parsed_code = ast.parse(code, mode='exec')
    except SyntaxError:
        return evaluate_value(raw_value, import_namespace)

    execution_namespace = dict(import_namespace or {})
    execution_namespace['__builtins__'] = EXCALIDRAW_BUILTINS

    if parsed_code.body and isinstance(parsed_code.body[-1], ast.Expr):
        expression = ast.Expression(parsed_code.body[-1].value)
        statements = ast.Module(body=parsed_code.body[:-1], type_ignores=[])

        if statements.body:
            exec(
                compile(statements, '<excalidraw-arg>', 'exec'),
                execution_namespace,
                execution_namespace,
            )

        return eval(
            compile(expression, '<excalidraw-arg>', 'eval'),
            execution_namespace,
            execution_namespace,
        )

    exec(
        compile(parsed_code, '<excalidraw-arg>', 'exec'),
        execution_namespace,
        execution_namespace,
    )

    if 'result' in execution_namespace:
        return execution_namespace['result']

    raise ValueError(
        "Argument code must end with an expression or assign the computed value to 'result'"
    )


def parse_task_argument_blocks(lines):
    arg_pattern = re.compile(r'^([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$')
    argument_blocks = []
    current_key = None
    current_value_lines = []

    for line in lines:
        match = arg_pattern.match(line)
        if match:
            if current_key is not None:
                argument_blocks.append((current_key, '\n'.join(current_value_lines).strip('\n')))

            current_key = match.group(1)
            current_value_lines = [match.group(2).lstrip()]
            continue

        if current_key is not None:
            current_value_lines.append(line)

    if current_key is not None:
        argument_blocks.append((current_key, '\n'.join(current_value_lines).strip('\n')))

    return argument_blocks


SECTION_HEADERS = {'dag:', 'imports:', 'vars:'}


def extract_named_blocks(elements, section_name):
    header = f'{section_name}:'
    blocks = []

    for elem in elements:
        if elem.get('type') != 'text':
            continue

        lines = elem.get('text', '').splitlines()
        current_block = None

        for line in lines:
            stripped_line = line.strip()
            if stripped_line in SECTION_HEADERS:
                if current_block is not None:
                    block = '\n'.join(current_block).strip()
                    if block:
                        blocks.append(block)
                current_block = [] if stripped_line == header else None
                continue

            if current_block is not None:
                current_block.append(line)

        if current_block is not None:
            block = '\n'.join(current_block).strip()
            if block:
                blocks.append(block)

    return blocks


def resolve_operator_class(class_path, import_namespace=None):
    if not class_path:
        class_path = DEFAULT_OPERATOR_CLASS_PATH

    if import_namespace and class_path in import_namespace:
        return import_namespace[class_path]

    if class_path.startswith('operators.'):
        class_path = f"{STANDARD_OPERATORS_PACKAGE}.{class_path[len('operators.'):]}"

    if '.' not in class_path:
        raise ValueError(
            f"Operator class '{class_path}' was not found in the imports/vars namespace and is not a fully qualified path"
        )

    module_path, class_name = class_path.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def instantiate_task_node(operator_class, task_id, dag, operator_kwargs):
    instance_kwargs = dict(operator_kwargs)
    has_group_id = 'group_id' in instance_kwargs

    try:
        signature = inspect.signature(operator_class)
    except (TypeError, ValueError):
        signature = None

    if signature is None:
        if not has_group_id and 'task_id' not in instance_kwargs:
            instance_kwargs['task_id'] = task_id
        if 'dag' not in instance_kwargs:
            instance_kwargs['dag'] = dag
        return operator_class(**instance_kwargs)

    parameters = signature.parameters
    accepts_var_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )

    if ('dag' in parameters or accepts_var_kwargs) and 'dag' not in instance_kwargs:
        instance_kwargs['dag'] = dag

    if (
        not has_group_id
        and ('task_id' in parameters or accepts_var_kwargs)
        and 'task_id' not in instance_kwargs
    ):
        instance_kwargs['task_id'] = task_id

    return operator_class(**instance_kwargs)


class ExcalidrawDagImporter(AbstractDagImporter):
    @classmethod
    def supported_extensions(cls):
        return ['.excalidraw']

    def list_dag_files(self, directory, safe_mode=True):
        path = Path(directory)
        if path.is_file():
            return [str(path)] if self.can_handle(path) else []
        if not path.is_dir():
            return []

        return [
            str(file_path)
            for file_path in path.rglob('*')
            if file_path.is_file() and file_path.suffix.lower() == '.excalidraw'
        ]

    def import_file(self, file_path, *, bundle_path=None, bundle_name=None, safe_mode=True):
        filepath = str(file_path)
        relative_path = self.get_relative_path(filepath, bundle_path)
        result = DagImportResult(file_path=relative_path)

        try:
            dag = parse_excalidraw_to_dag(filepath)
        except Exception as exc:
            result.errors.append(
                DagImportError(
                    file_path=relative_path,
                    message=str(exc),
                    error_type='import',
                    stacktrace=traceback.format_exc(),
                )
            )
            return result

        dag.fileloc = filepath
        dag.relative_fileloc = relative_path
        result.dags.append(dag)
        return result


def extract_import_namespace(elements):
    import_namespace = {}

    for import_block in extract_named_blocks(elements, 'imports'):
        if not import_block:
            continue

        try:
            parsed_imports = ast.parse(import_block, mode='exec')
        except SyntaxError as exc:
            raise ValueError(f'Invalid imports block: {exc}') from exc

        if any(not isinstance(node, (ast.Import, ast.ImportFrom)) for node in parsed_imports.body):
            raise ValueError('Imports blocks may only contain import statements')

        exec(
            compile(parsed_imports, '<excalidraw-imports>', 'exec'),
            {'__builtins__': EXCALIDRAW_BUILTINS},
            import_namespace,
        )

    return import_namespace


def extract_vars_namespace(elements, import_namespace):
    vars_namespace = {}

    for vars_block in extract_named_blocks(elements, 'vars'):
        if not vars_block:
            continue

        try:
            parsed_vars = ast.parse(vars_block, mode='exec')
        except SyntaxError as exc:
            raise ValueError(f'Invalid vars block: {exc}') from exc

        execution_namespace = {**import_namespace, **vars_namespace}
        exec(
            compile(parsed_vars, '<excalidraw-vars>', 'exec'),
            {'__builtins__': EXCALIDRAW_BUILTINS},
            execution_namespace,
        )
        vars_namespace = {
            key: value
            for key, value in execution_namespace.items()
            if key not in import_namespace
        }

    return vars_namespace


def extract_evaluation_namespace(elements):
    import_namespace = extract_import_namespace(elements)
    vars_namespace = extract_vars_namespace(elements, import_namespace)
    return {**import_namespace, **vars_namespace}


def is_text_inside_rectangle(rectangle, text_element):
    rect_left = rectangle.get('x', 0)
    rect_top = rectangle.get('y', 0)
    rect_right = rect_left + rectangle.get('width', 0)
    rect_bottom = rect_top + rectangle.get('height', 0)

    text_left = text_element.get('x', 0)
    text_top = text_element.get('y', 0)
    text_right = text_left + text_element.get('width', 0)
    text_bottom = text_top + text_element.get('height', 0)

    return (
        rect_left <= text_left <= rect_right
        and rect_left <= text_right <= rect_right
        and rect_top <= text_top <= rect_bottom
        and rect_top <= text_bottom <= rect_bottom
    )


def get_task_config_from_text(text, import_namespace=None):
    raw_lines = text.splitlines()
    if not raw_lines:
        return None

    first_non_empty_idx = next((idx for idx, line in enumerate(raw_lines) if line.strip()), None)
    if first_non_empty_idx is None:
        return None

    task_id = raw_lines[first_non_empty_idx].strip()

    task_config = {
        'task_id': task_id,
        'operator_class': resolve_operator_class(DEFAULT_OPERATOR_CLASS_PATH, import_namespace),
        'operator_kwargs': {},
    }

    argument_blocks = parse_task_argument_blocks(raw_lines[first_non_empty_idx + 1:])
    for key, value in argument_blocks:
        if key == 'class':
            try:
                resolved_class = execute_python_value(value, import_namespace)
            except NameError:
                resolved_class = value.strip()

            if isinstance(resolved_class, str):
                task_config['operator_class'] = resolve_operator_class(resolved_class, import_namespace)
            else:
                task_config['operator_class'] = resolved_class
        else:
            task_config['operator_kwargs'][key] = execute_python_value(value, import_namespace)

    return task_config


def get_text_for_rectangle(rectangle, text_elements):
    rectangle_group_ids = set(rectangle.get('groupIds', []))

    for text_element in text_elements:
        if text_element.get('containerId') == rectangle.get('id'):
            return text_element

    if rectangle_group_ids:
        for text_element in text_elements:
            text_group_ids = set(text_element.get('groupIds', []))
            if rectangle_group_ids.intersection(text_group_ids):
                return text_element

    for text_element in text_elements:
        if is_text_inside_rectangle(rectangle, text_element):
            return text_element

    return None


def parse_excalidraw_to_dag(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        all_elements = data.get('elements', [])
        elements = get_active_elements(all_elements)
        import_namespace = extract_evaluation_namespace(all_elements)

        dag_attrs = {}
        for elem in elements:
            if elem.get('type') == 'text':
                text = elem.get('text', '')
                if text.startswith('dag:'):
                    lines = text.split('\n')
                    for line in lines[1:]:
                        if '=' in line:
                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip()
                            if key == 'dag_id':
                                dag_attrs['dag_id'] = value
                            elif key == 'schedule':
                                dag_attrs['schedule'] = value
                    break

        if 'dag_id' not in dag_attrs:
            filename = os.path.basename(file_path)
            dag_attrs['dag_id'] = filename.split('.')[0]
        if 'schedule' not in dag_attrs:
            dag_attrs['schedule'] = None

        tasks = {}
        task_element_ids = {}
        text_elements = [elem for elem in elements if elem.get('type') == 'text']
        for elem in elements:
            if elem.get('type') != 'rectangle':
                continue

            text_element = get_text_for_rectangle(elem, text_elements)
            if not text_element:
                continue

            task_config = get_task_config_from_text(text_element.get('text', ''), import_namespace)
            if task_config:
                tasks[elem['id']] = task_config
                task_element_ids[elem['id']] = task_config['task_id']
                task_element_ids[text_element['id']] = task_config['task_id']

        dependencies = []
        for elem in elements:
            if elem.get('type') == 'arrow':
                start_binding = elem.get('startBinding', {})
                end_binding = elem.get('endBinding', {})
                start_id = start_binding.get('elementId')
                end_id = end_binding.get('elementId')
                start_task_id = task_element_ids.get(start_id)
                end_task_id = task_element_ids.get(end_id)
                if start_task_id and end_task_id:
                    dependencies.append((start_task_id, end_task_id))

        dag = DAG(
            dag_id=dag_attrs['dag_id'],
            schedule=dag_attrs['schedule'],
            start_date=datetime.datetime(2023, 1, 1),
            catchup=False,
            params={EXCALIDRAW_SOURCE_PARAM_KEY: Path(file_path).name},
        )

        task_ops = {}
        for task in tasks.values():
            task_id = task['task_id']
            task_ops[task_id] = instantiate_task_node(
                task['operator_class'],
                task_id,
                dag,
                task['operator_kwargs'],
            )

        for upstream, downstream in dependencies:
            task_ops[upstream] >> task_ops[downstream]

        return dag
    except Exception as exc:
        raise type(exc)(f"Failed to parse Excalidraw file '{file_path}': {exc}") from exc


def register_airflow_sketcher_importer():
    registry = get_importer_registry()
    existing_importer = registry.get_importer('example.excalidraw')
    if not isinstance(existing_importer, ExcalidrawDagImporter):
        registry.register(ExcalidrawDagImporter())

    from airflow.dag_processing.manager import DagFileProcessorManager

    if getattr(DagFileProcessorManager, '_excalidraw_discovery_patched', False):
        return

    original_find_files_in_bundle = DagFileProcessorManager._find_files_in_bundle

    def _find_files_in_bundle(self, bundle):
        rel_paths = list(original_find_files_in_bundle(self, bundle))
        seen_paths = set(rel_paths)

        for file_path in registry.list_dag_files(bundle.path):
            rel_path = Path(file_path).relative_to(bundle.path)
            if rel_path in seen_paths:
                continue
            rel_paths.append(rel_path)
            seen_paths.add(rel_path)

        return rel_paths

    DagFileProcessorManager._find_files_in_bundle = _find_files_in_bundle
    DagFileProcessorManager._excalidraw_discovery_patched = True