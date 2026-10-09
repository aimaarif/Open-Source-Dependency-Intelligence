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
Python packages only, with a sample of 20–50 packages.

## Architecture
To be documented in docs/architecture.md.

## Data Sources
PyPI, GitHub, deps.dev, and OSV.dev.
