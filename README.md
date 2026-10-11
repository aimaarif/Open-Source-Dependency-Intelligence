# Open Source Dependency Intelligence Platform

## Overview
A cloud data engineering project that collects open-source
package metadata, dependency relationships, and vulnerability
information to identify potentially fragile dependencies.

## Objectives
- Ingest data from public APIs.
- Store raw data in Amazon S3.
- Process data using Apache Spark and Delta Lake.
- Orchestrate workflows with Apache Airflow.
- Analyze results using Snowflake.

## Initial Scope
Python packages only, with a sample of 100 packages.

## Architecture
To be documented in docs/architecture.md.

## Data Sources
PyPI, GitHub, deps.dev, and OSV.dev.



This platform should eventually answer questions such as:
1. Which packages have not released a new version for a long time?
2. Which packages have few active contributors?
3. Which packages are depended on by many other packages?
4. Which package versions have known vulnerabilities?
5. Which widely used packages appear to have weak maintenance signals?