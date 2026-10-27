
import os
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

def main():
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")

    connection = snowflake.connector.connect(
        account=account,
        user=user,
        authenticator="OAUTH_AUTHORIZATION_CODE",
        warehouse="OSI_WH",
        database="OSI_DATABASE",
        schema="GOLD",
        login_timeout=60,
        enable_server_session_keep_alive_auto_detection=True,
    )

    
    try:
        with connection.cursor() as cursor:
            cursor.execute("USE WAREHOUSE OSI_WH")
            cursor.execute("USE DATABASE OSI_DATABASE")
            cursor.execute("USE SCHEMA GOLD")

            cursor.execute("""
                SELECT
                    CURRENT_USER(),
                    CURRENT_ROLE(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA()
            """)

            result = cursor.fetchone()

            print("Snowflake connection successful.")
            print("User:", result[0])
            print("Role:", result[1])
            print("Warehouse:", result[2])
            print("Database:", result[3])
            print("Schema:", result[4])

    finally:
        connection.close()


if __name__ == "__main__":
    main()
