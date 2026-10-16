Architecture and data flow
Windows project folder
        |
        | docker compose up
        v
Docker container: osd-airflow
        |
        +--> Airflow web UI / scheduler / task execution
        |
        +--> Reads project source mounted at /opt/airflow/project
        |       |
        |       +--> src/extract/pypi.py
        |       |       +--> Reads config/package_seed.csv
        |       |       +--> Calls PyPI API
        |       |       +--> Writes raw JSON to data/raw/pypi/...
        |       |
        |       +--> src/storage/s3.py
        |               +--> Reads AWS profile/config
        |               +--> Uploads raw JSON to S3 Bronze
        |
        +--> airflow/dags/
                +--> dependency_ingestion.py
                +--> retry_demo.py

S3 bucket: osd-s3-bucket
Prefix:    pypi/
Region:    ap-south-1
AWS profile: dependency-dev
The project and AWS credential folders are mounted into the container. A mount lets the container access host files without copying them into the image. The AWS mount is read-only (:ro) so the container can read credentials/configuration but cannot modify that mounted directory.
Main ingestion workflow
Start manually
     |
     v
extract_pypi_metadata
     |
     v
validate_extraction
     |
     v
upload_to_s3
     |
     v
Complete
Airflow creates task dependencies from the way the tasks are called in the DAG function. The upload task depends on successful completion of validation, and validation depends on extraction.

Idempotency: Repeating an operation should not unintentionally create duplicate effects. The current ingestion uses a new run ID, so each run can create a new set of S3 keys.

Files:
- airflow/Dockerfile: defines the Airflow image and additional Python packages.
- docker-compose.yaml: configures how the container runs.
- airflow/dags/dependency_ingestion.py: the actual extraction → validation → upload workflow.
- airflow/dags/retry_demo.py: a small, safe workflow used to learn retry behavior.
- src/extract/pypi.py: contains the existing PyPI extraction implementation.
- src/storage/s3.py: contains the existing S3 upload implementation.
- config/package_seed.csv: supplies the package names and ecosystems used by ingestion.
- .env: local configuration values. Do not commit secrets or paste AWS credentials into chats.

The AWS profile itself is stored in the local AWS credentials/configuration files, not directly in the DAG. Docker Compose mounts the host .aws folder into /home/airflow/.aws as read-only.

Docker configuration
Airflow Dockerfile is used for configuration.

Docker Compose configuration
The intended Compose configuration is docker-compose.yaml

A mounted file change is generally visible inside the running container without rebuilding the image. A change to the Dockerfile or installed packages normally requires rebuilding.


Main DAG: dependency_ingestion.py

Task 1 — Extract
The extractor writes JSON observations into a path shaped like:
data/raw/pypi/ingested_date=YYYY-MM-DD/run_id=<uuid>/<package>.json
The date partition and run ID help separate observations from different runs.
Task 2 — Validate
The validation task receives the extraction summary and checks that:
1. The seed list did not produce zero total packages.
2. At least one package request succeeded.
3. A directory exists for the returned run ID.
4. The number of JSON files matches the total reported by extraction.
5. Any failed package requests are logged as warnings.
Validation is useful because a workflow should not blindly publish data after a clearly incomplete extraction. In this implementation, some individual package failures are warnings if at least one request succeeded and the expected JSON files were created. This is a learning-stage validation rule, not a guarantee that every package's payload is valid.
Task 3 — Upload to S3

Airflow's TaskFlow API passes the upstream task output to the downstream task and creates the dependencies. The order is:
1. Extraction must complete.
2. Validation must succeed.
3. Upload can then run.
If validation fails, upload should not start.


Retry demonstration DAG
The second DAG ID is:
airflow_retry_demo
It deliberately fails on the first task attempt and succeeds on the next attempt. It does not call PyPI or S3, so it is safe for testing retries without changing project data.


Commands used and what they do
Run these commands from the project root in PowerShell unless noted otherwise.
Check Docker installation
docker --version
docker compose version
Confirms that the Docker CLI and Compose plugin are available.
Start/build Airflow
docker compose up --build -d
- up: creates and starts the services.
- --build: rebuilds the image if needed.
- -d: runs in the background.
If the container is already built and you only need to start it:
docker compose up -d
Check running containers
docker compose ps
docker ps
Use these to check whether the Airflow container is running and whether port 8080 is published.
View logs
docker compose logs airflow
docker compose logs -f airflow
Check that Airflow can see the DAGs
docker compose exec airflow airflow dags list
Use it to confirm discovery of:
open_source_dependency_ingestion
airflow_retry_demo
Inspect DAG import errors
docker compose exec airflow airflow dags list-import-errors
Useful when a DAG is missing from the UI or CLI. Python import errors, syntax errors, missing dependencies, and DAG-definition issues can prevent discovery.
Open a shell in the container
docker compose exec airflow bash
This opens a shell inside the running container. Use exit to leave it.
Trigger the ingestion DAG manually
docker compose exec airflow airflow dags trigger open_source_dependency_ingestion
This creates a DAG run. It performs real extraction and S3 upload and can add more objects and storage cost. Do not run it just to test Airflow.
Trigger the retry demo manually
docker compose exec airflow airflow dags trigger airflow_retry_demo
This tests retry behavior without calling external data services or changing S3.
Check S3 object count and total size
aws s3 ls s3://osd-s3-bucket/pypi/ --recursive --summarize --profile dependency-dev
This lists object keys and then reports total object count and bytes. It can produce a long listing; use it when you actually need a full inventory.
To list the largest objects by size without downloading them:
aws s3api list-objects-v2 `
  --bucket osd-s3-bucket `
  --prefix pypi/ `
  --profile dependency-dev `
  --query "reverse(sort_by(Contents, &Size))[:15].[Size,Key]" `
  --output table
This is a metadata query. It does not download, upload, or delete the objects. It helps identify whether individual PyPI JSON files are unexpectedly large.
Stop Airflow
docker compose down
Stops and removes the Compose container/network. It does not normally delete the source code or S3 objects. It also does not delete named Docker volumes unless -v is specified; avoid -v unless you understand which volumes it would remove.
Rebuild after Dockerfile/package changes
docker compose up --build -d
Rebuild when the Dockerfile or image-installed dependencies change. Editing a mounted DAG file usually does not require a rebuild, though Airflow may need a short time to detect the change.


Airflow UI workflow
Open:
http://localhost:8080
The standalone setup generates local login information during startup; use the credentials shown in the container logs if the UI asks for them. Do not publish these credentials.
In the UI:
1. Find open_source_dependency_ingestion.
2. Open the DAG and review its graph.
3. Trigger it manually only when you intend to perform a real ingestion.
4. Open the DAG run and inspect each task's state.
5. Open a task instance to inspect logs and errors.
6. Find airflow_retry_demo to test retries without external data changes.
A successful DAG run should show the three main tasks as successful: extraction, validation, and upload. A successful retry demo should show the task's final state as Success, with the logs showing an initial failure followed by a later successful attempt.

Scheduling: why the DAG remains manual
The ingestion DAG uses:
schedule=None
This is intentional.
Common scheduling options in Airflow include:
Setting	Typical use
schedule=None:	Manual runs; best for the current learning stage.
schedule="@daily":	One scheduled run per day.
schedule="@weekly":	One scheduled run per week.
schedule="0 2 * * 0":	Cron schedule: Sunday at 02:00 in the DAG's timezone.
schedule=timedelta(hours=6):	Run on a six-hour interval.

Troubleshooting checklist
DAG does not appear
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags list-import-errors
docker compose logs airflow
If Airflow isn't dicovering any file in dag then add something like this to the top of file:
# Airflow DAG definition for testing retry behavior.