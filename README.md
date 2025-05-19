# Airflow DAGs Project

This repository contains a simple Apache Airflow project with an example DAG and utility functions. 

## Project Structure

```
airflow-dag-demo
├── dags
│   ├── example_dag.py       # Defines a simple Airflow DAG
│   └── utils
│       └── helpers.py       # Utility functions for the DAG
├── tests
│   └── test_example_dag.py   # Unit tests for the example DAG
├── requirements.txt          # Python dependencies
├── .airflowignore            # Files to ignore by Airflow
├── .gitignore                # Files to ignore by Git
└── README.md                 # Project documentation
```

## Setup Instructions

1. Clone the repository:
   ```
   git clone <repository-url>
   cd airflow-dag-demo
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Start the Airflow web server and scheduler:
   ```
   airflow db init
   airflow webserver --port 8080
   airflow scheduler
   ```

## Usage

- The example DAG can be found in `dags/example_dag.py`. You can modify it to suit your needs.
- Utility functions are located in `dags/utils/helpers.py` and can be imported into your DAGs as needed.
- Run the tests using your preferred testing framework to ensure everything is functioning correctly.

## Contributing

Feel free to submit issues or pull requests for improvements or additional features. 

## License

This project is licensed under the MIT License.