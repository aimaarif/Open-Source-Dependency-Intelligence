Open Source Dependency Intelligence Platform
1. Overall workflow

PyPI extraction
100 packages collected across two snapshots
⬇️
Bronze → Silver → Gold
Structure package metadata, track versions and extract declared dependencies
⬇️
OSV vulnerability extraction
Query OSV.dev for the tracked package/version combinations and fetch full advisory records
⬇️
OSV Silver datasets
Store vulnerability details and separate affected-package/version information
⬇️
Gold vulnerability matching and assessment
Match advisories to tracked packages and assess observed versions against available version rules
⬇️
Package risk dataset
Aggregate affected, unknown and not-affected assessments into package-level risk indicators
⬇️
Dependency graph and reach
Identify direct declared dependencies and measure which dependencies are used by other tracked packages
⬇️
Vulnerability impact analysis
Combine package risk with direct dependency reach to prioritize potentially impactful vulnerabilities
⬇️
Snowflake integration
Load the Gold datasets into analytical tables for SQL queries and dashboarding


2. PyPI branch — what each file did

src/extract/pypi.py:	Fetches package metadata from the PyPI API and saves raw JSON in Bronze.
src/transform/bronze_to_silver.py:	Reads raw JSON and converts it into structured Parquet records containing package metadata, versions, dependencies and snapshot information.
src/transform/validate_silver.py:	Checks row counts, required fields, duplicate package/run records and snapshot consistency.
src/transform/silver_to_gold.py:	Creates one latest package-health record per package, including version history, snapshot counts and metadata/dependency metrics.
src/transform/silver_to_gold_dependencies.py:	Extracts declared dependencies into a separate Gold dataset, including package requirements and environment markers.
src/transform/validate_gold_dependencies.py:	Checks dependency counts, missing fields, invalid extracted names and declaration consistency.

Result: A package-health dataset containing 100 packages and a separate dataset of their declared dependencies. The Silver layer retains 200 package snapshots across two ingestion runs.

3. OSV branch — extraction and Silver datasets

src/extract/osv.py:	Sends batch queries to OSV.dev for tracked package/version combinations and saves raw responses in data/raw/osv.
src/extract/osv_details.py:	Extracts unique vulnerability IDs from batch results, fetches full advisory records and saves them as raw JSON.
src/transform/osv_to_silver.py:	Converts full OSV records into Parquet while preserving descriptions, dates, aliases, severity, affected-version data and extraction metadata.
src/transform/validate_silver_osv.py:	Checks vulnerability record counts, required fields, duplicate ID/run combinations and HTTP status codes.
src/transform/osv_to_silver_affected.py:	Extracts nested affected-package entries into a separate dataset, preserving ecosystems, package names, version ranges and explicit affected versions.
src/transform/validate_silver_osv_affected.py:	Checks required fields, HTTP status, ecosystem information, version-rule availability and repeated vulnerability/package combinations.
src/transform/match_osv_to_packages.py:	Matches OSV affected-package entries against the tracked Gold package inventory and adds advisory details to candidate matches.

Result: 33 Unique vulnerability records were fetched successfully, converted into Silver datasets and validated. The matching step connects advisory candidates to the tracked package inventory.


4. Gold vulnerability assessment and risk

src/transform/assess_package_vulnerabilities.py:	Evaluates candidate advisories against observed package versions using explicit affected versions and supported OSV version-range rules. Produces affected, not_affected or unknown assessments.
src/transform/validate_gold_vulnerability_assessment.py:	Validates the assessment dataset, including required fields, duplicate records and assessment consistency.
src/transform/build_package_risk.py:	Aggregates vulnerability assessments into one package-level record, counting affected, unknown and not-affected vulnerabilities and assigning a risk status.

The risk categories distinguish packages with an affected-version assessment, packages needing review, packages with no affected-version match, and packages with no known OSV match.

Important limitation: The version assessment supports the range rules implemented so far; it is not a complete implementation of every OSV range type and ecosystem. An affected result is a rule-based assessment, not proof of exploitability.


5. Dependency graph and reach

src/transform/build_dependency_graph.py:	Builds a graph from the latest declared dependencies, normalizes package names, and identifies whether each dependency source and target belongs to the tracked inventory.
src/transform/validate_dependency_graph.py:	Checks graph structure, tracked/untracked classifications and dependency-reach calculations.

Two Gold datasets are produced:
- data/gold/pypi/dependency_graph — edge-level relationships between a source package and its declared dependency.
- data/gold/pypi/dependency_reach — direct dependent counts for dependency targets, including counts limited to tracked source packages.
The latest validation showed 92 dependency edges: 25 point to targets inside the tracked inventory and 67 point to targets outside it. All source packages are tracked.

This is a direct declared-dependency graph, not a transitive dependency graph. Environment markers and extras are preserved but not evaluated, and the dataset does not establish which dependencies are actually installed at runtime.


6. Vulnerability impact analysis

src/transform/build_vulnerability_impact.py:	Combines package-level risk with direct dependency reach to estimate which affected packages have tracked direct dependents.
src/transform/validate_vulnerability_impact.py:	Checks required fields, duplicate package records, negative counts and consistency with package risk and dependency-reach datasets.

The impact statuses are:
- affected_with_direct_dependents — an affected-version assessment exists and tracked packages directly depend on the package.
- affected_no_tracked_direct_dependents — an affected-version assessment exists, but no tracked package directly declares it as a dependency.
- needs_review — the risk assessment requires further investigation.
- no_confirmed_affected_assessment — no affected-version assessment was confirmed by the implemented rules.