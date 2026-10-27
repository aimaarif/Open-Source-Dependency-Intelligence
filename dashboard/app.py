
import os

import pandas as pd
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv



load_dotenv()

st.set_page_config(
    page_title="Open Source Dependency Intelligence",
    page_icon="",
    layout="wide",
)

st.title("Open Source Dependency Intelligence")
st.caption(
    "Package vulnerability assessments and dependency analysis "
    "from the existing Snowflake Gold layer."
)



@st.cache_resource
def get_connection():
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")

    if not account or not user:
        raise ValueError(
            "SNOWFLAKE_ACCOUNT or SNOWFLAKE_USER is missing "
            "from the project-root .env file."
        )

    return snowflake.connector.connect(
        account=account,
        user=user,
        authenticator="OAUTH_AUTHORIZATION_CODE",
        warehouse="OSI_WH",
        database="OSI_DATABASE",
        schema="GOLD",
    )



@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql, params=()):
    connection = get_connection()

    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetch_pandas_all()



@st.cache_data(ttl=300, show_spinner=False)
def load_risk_summary():
    return run_query("""
        SELECT
            PACKAGE_NAME,
            VERSION,
            RISK_STATUS,
            ASSESSED_VULNERABILITY_COUNT,
            AFFECTED_VULNERABILITY_COUNT,
            UNKNOWN_ASSESSMENT_COUNT,
            NOT_AFFECTED_COUNT,
            DECLARED_DEPENDENCY_COUNT
        FROM OSI_DATABASE.GOLD.VW_PACKAGE_VULNERABILITY_SUMMARY
        ORDER BY AFFECTED_VULNERABILITY_COUNT DESC, PACKAGE_NAME
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_dependency_reach():
    return run_query("""
        SELECT
            DEPENDENCY_NAME,
            NORMALIZED_DEPENDENCY_NAME,
            DIRECT_DEPENDENT_COUNT,
            TRACKED_SOURCE_DEPENDENT_COUNT,
            DEPENDENCY_IS_TRACKED
        FROM OSI_DATABASE.GOLD.VW_DEPENDENCY_REACH_SUMMARY
        ORDER BY DIRECT_DEPENDENT_COUNT DESC, DEPENDENCY_NAME
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_vulnerability_findings():
    return run_query("""
        SELECT *
        FROM OSI_DATABASE.GOLD.PACKAGE_VULNERABILITIES
        LIMIT 100
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_package_dependencies(package_name):
    return run_query(
        """
        SELECT
            PACKAGE_NAME,
            VERSION,
            SNAPSHOT_DATE,
            DEPENDENCY_NAME,
            NORMALIZED_DEPENDENCY_NAME,
            DEPENDENCY_REQUIREMENT,
            ENVIRONMENT_MARKER,
            SOURCE_IS_TRACKED,
            DEPENDENCY_IS_TRACKED
        FROM OSI_DATABASE.GOLD.DEPENDENCY_GRAPH
        WHERE LOWER(PACKAGE_NAME) = LOWER(%s)
        ORDER BY SNAPSHOT_DATE DESC, DEPENDENCY_NAME
        """,
        (package_name,),
    )



st.sidebar.header("Dashboard controls")
st.sidebar.caption("Data is cached for five minutes.")

if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()

try:
    with st.spinner("Loading existing Gold-layer data..."):
        risk_df = load_risk_summary()
        reach_df = load_dependency_reach()

except Exception as exc:
    st.error("Could not load data from Snowflake.")
    st.code(str(exc))
    st.info(
        "Check your .env values, OAuth authentication, warehouse access, "
        "and whether both summary views exist."
    )
    st.stop()



risk_df.columns = [column.upper() for column in risk_df.columns]
reach_df.columns = [column.upper() for column in reach_df.columns]

risk_df["RISK_STATUS"] = (
    risk_df["RISK_STATUS"].astype(str).str.lower()
)

numeric_columns = [
    "ASSESSED_VULNERABILITY_COUNT",
    "AFFECTED_VULNERABILITY_COUNT",
    "UNKNOWN_ASSESSMENT_COUNT",
    "NOT_AFFECTED_COUNT",
    "DECLARED_DEPENDENCY_COUNT",
]

for column in numeric_columns:
    risk_df[column] = pd.to_numeric(
        risk_df[column], errors="coerce"
    ).fillna(0)


total_packages = len(risk_df)
affected_packages = int(
    (risk_df["RISK_STATUS"] == "affected").sum()
)
affected_findings = int(
    risk_df["AFFECTED_VULNERABILITY_COUNT"].sum()
)
total_dependency_records = len(reach_df)



overview_tab, vulnerability_tab, dependency_tab = st.tabs(
    [
        "Overview",
        "Vulnerability Findings",
        "Dependencies & Package Explorer",
    ]
)



with overview_tab:
    st.subheader("Package health overview")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Tracked packages", total_packages)
    col2.metric("Packages marked affected", affected_packages)
    col3.metric("Affected findings", affected_findings)
    col4.metric("Dependency reach records", total_dependency_records)

    st.subheader("Package assessment distribution")

    risk_counts = (
        risk_df["RISK_STATUS"]
        .value_counts()
        .rename_axis("Risk status")
        .reset_index(name="Package count")
    )

    st.bar_chart(
        risk_counts,
        x="Risk status",
        y="Package count",
    )

    st.subheader("Packages with the most affected findings")

    affected_df = risk_df[
        risk_df["AFFECTED_VULNERABILITY_COUNT"] > 0
    ].sort_values(
        "AFFECTED_VULNERABILITY_COUNT",
        ascending=False,
    )

    st.dataframe(
        affected_df,
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "'No known OSV match' means no matching record was found "
        "by the current assessment. It does not establish that "
        "a package is vulnerability-free."
    )



with vulnerability_tab:
    st.subheader("Vulnerability findings")

    try:
        findings_df = load_vulnerability_findings()
        findings_df.columns = [
            column.upper() for column in findings_df.columns
        ]

        st.write(
            f"Showing up to 100 records from "
            f"PACKAGE_VULNERABILITIES ({len(findings_df)} returned)."
        )

        if not findings_df.empty:
            if "PACKAGE_NAME" in findings_df.columns:
                package_options = ["All"] + sorted(
                    findings_df["PACKAGE_NAME"]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()
                )

                selected_package = st.selectbox(
                    "Filter by package",
                    package_options,
                )

                if selected_package != "All":
                    findings_df = findings_df[
                        findings_df["PACKAGE_NAME"].astype(str)
                        == selected_package
                    ]

            st.dataframe(
                findings_df,
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No vulnerability finding records were returned.")

    except Exception as exc:
        st.error("Could not load the vulnerability findings table.")
        st.code(str(exc))



with dependency_tab:
    st.subheader("Most depended-on packages")

    top_n = st.slider(
        "Number of dependencies to display",
        min_value=5,
        max_value=20,
        value=10,
    )

    top_dependencies = reach_df.head(top_n).copy()

    if not top_dependencies.empty:
        chart_df = top_dependencies[
            ["NORMALIZED_DEPENDENCY_NAME", "DIRECT_DEPENDENT_COUNT"]
        ].copy()

        chart_df = chart_df.rename(
            columns={
                "NORMALIZED_DEPENDENCY_NAME": "Dependency",
                "DIRECT_DEPENDENT_COUNT": "Direct dependents",
            }
        )

        st.bar_chart(
            chart_df,
            x="Dependency",
            y="Direct dependents",
        )

        st.dataframe(
            top_dependencies,
            use_container_width=True,
            hide_index=True,
        )

    st.divider()
    st.subheader("Package explorer")

    package_names = sorted(
        risk_df["PACKAGE_NAME"].dropna().astype(str).unique().tolist()
    )

    selected_name = st.selectbox(
        "Select a tracked source package",
        package_names,
    )

    selected_rows = risk_df[
        risk_df["PACKAGE_NAME"].astype(str) == selected_name
    ]

    if not selected_rows.empty:
        package = selected_rows.iloc[0]

        st.write(
            f"**{package['PACKAGE_NAME']}** — version "
            f"{package['VERSION']}"
        )

        metric1, metric2, metric3 = st.columns(3)

        metric1.metric("Assessment status", package["RISK_STATUS"])
        metric2.metric(
            "Affected findings",
            int(package["AFFECTED_VULNERABILITY_COUNT"]),
        )
        metric3.metric(
            "Declared dependency count",
            int(package["DECLARED_DEPENDENCY_COUNT"]),
        )

        try:
            dependencies_df = load_package_dependencies(selected_name)
            dependencies_df.columns = [
                column.upper() for column in dependencies_df.columns
            ]

            st.write("Declared dependency edges in the tracked snapshots")

            st.dataframe(
                dependencies_df,
                use_container_width=True,
                hide_index=True,
            )

            if dependencies_df.empty:
                st.info(
                    "No outgoing dependency edges were found for this "
                    "package in the available dependency graph."
                )

        except Exception as exc:
            st.error("Could not load this package's dependency records.")
            st.code(str(exc))


st.divider()
st.caption(
    "Data source: OSI_DATABASE.GOLD. "
    "Results reflect the snapshots and assessments currently loaded."
)
