import unittest
from airflow.models import DagBag

class TestExampleDag(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(dag_folder='airflow-dags/dags', include_examples=False)

    def test_dag_import(self):
        self.assertFalse(
            self.dagbag.import_errors,
            f"DAG import errors: {self.dagbag.import_errors}"
        )

    def test_dag_loaded(self):
        dag_id = 'example_dag'  # Replace with your actual DAG ID
        dag = self.dagbag.get_dag(dag_id)
        self.assertIsNotNone(dag)
        self.assertEqual(dag.dag_id, dag_id)

    def test_task_count(self):
        dag_id = 'example_dag'  # Replace with your actual DAG ID
        dag = self.dagbag.get_dag(dag_id)
        self.assertEqual(len(dag.tasks), expected_task_count)  # Replace with the expected task count

    def test_task_dependencies(self):
        dag_id = 'example_dag'  # Replace with your actual DAG ID
        dag = self.dagbag.get_dag(dag_id)
        task_1 = dag.get_task('task_1')  # Replace with your actual task ID
        task_2 = dag.get_task('task_2')  # Replace with your actual task ID
        self.assertIn(task_2, task_1.downstream)  # Replace with actual dependency check

if __name__ == '__main__':
    unittest.main()