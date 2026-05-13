import sys
import unittest
from pathlib import Path

from tests.support.airflow_stubs import install_airflow_stubs


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / 'src'
FIXTURE_MODULES_DIR = REPO_ROOT / 'tests' / 'fixtures' / 'python_modules'
FIXTURE_DIAGRAMS_DIR = REPO_ROOT / 'tests' / 'fixtures' / 'excalidraw'

for path in (SRC_DIR, FIXTURE_MODULES_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


install_airflow_stubs()

from airflow_sketcher.dag_factory import EXCALIDRAW_SOURCE_PARAM_KEY, parse_excalidraw_to_dag


class ExcalidrawDagFactoryTests(unittest.TestCase):
    def parse_fixture(self, filename):
        return parse_excalidraw_to_dag(str(FIXTURE_DIAGRAMS_DIR / filename))

    def test_parses_branching_dag_from_excalidraw_file(self):
        dag = self.parse_fixture('dag2.excalidraw')

        self.assertEqual(dag.dag_id, 'aaa_my_second_dag')
        self.assertEqual(dag.schedule, '@daily')
        self.assertEqual(dag.params[EXCALIDRAW_SOURCE_PARAM_KEY], 'dag2.excalidraw')
        self.assertEqual(set(dag.task_dict), {'task_a', 'task_b', 'task_c', 'task_d', 'task_e'})
        self.assertEqual(type(dag.task_dict['task_a']).__name__, 'BashOperator')
        self.assertEqual(type(dag.task_dict['task_b']).__name__, 'BashOperator')
        self.assertEqual(type(dag.task_dict['task_c']).__name__, 'EmptyOperator')
        self.assertEqual(type(dag.task_dict['task_d']).__name__, 'EmptyOperator')
        self.assertEqual(type(dag.task_dict['task_e']).__name__, 'EmptyOperator')
        self.assertEqual(dag.task_dict['task_a'].bash_command, 'echo "task first"')
        self.assertEqual(dag.task_dict['task_b'].bash_command, 'echo "task second"')
        self.assertEqual(dag.task_dict['task_a'].downstream_task_ids, {'task_b'})
        self.assertEqual(dag.task_dict['task_b'].downstream_task_ids, {'task_c', 'task_d', 'task_e'})

    def test_parses_imports_and_vars_for_python_tasks(self):
        dag = self.parse_fixture('a_dag_with_python.excalidraw')

        self.assertEqual(dag.dag_id, 'a_dag_with_python')
        self.assertIsNone(dag.schedule)
        self.assertEqual(
            dag.params[EXCALIDRAW_SOURCE_PARAM_KEY],
            'a_dag_with_python.excalidraw',
        )
        self.assertEqual(set(dag.task_dict), {'first_fn', 'second_fn'})

        first_task = dag.task_dict['first_fn']
        second_task = dag.task_dict['second_fn']

        self.assertEqual(type(first_task).__name__, 'PythonOperator')
        self.assertEqual(type(second_task).__name__, 'PythonOperator')
        self.assertTrue(callable(first_task.python_callable))
        self.assertTrue(callable(second_task.python_callable))
        self.assertEqual(first_task.python_callable.__name__, 'first_func')
        self.assertEqual(second_task.python_callable.__name__, 'second_func')
        self.assertEqual(second_task.python_callable.__module__, 'shared_funcs')
        self.assertEqual(first_task.downstream_task_ids, {'second_fn'})


if __name__ == '__main__':
    unittest.main()