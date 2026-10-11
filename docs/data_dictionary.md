Dataset	                    Source	        Important fields
Package metadata	        PyPI	        package name, version, summary, project URLs
Repository metadata	        GitHub	        repository name, stars, forks, open issues
Dependency information	    deps.dev        package version, direct dependencies
Vulnerability information   OSV.dev	        vulnerability ID, affected versions, severity where available
Ingestion metadata	        Your pipeline	source, ingestion timestamp, run ID, status

common ingestion record:
source: API or data provider name.
package_name: package identifier supplied to the extraction task.
ingested_at: UTC timestamp for when the response was collected.
run_id: identifier shared by tasks in one pipeline run.
request_url: endpoint used, excluding secrets or sensitive query parameters.
http_status: response status when available.
payload: original response body, stored as JSON.
error_message: error details when collection fails.