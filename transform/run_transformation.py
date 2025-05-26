import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from transform.utils import get_engine
from sqlalchemy.types import Integer

def run_transformation():
    connection = None  # For finally block

    try:
        print("🔌 Attempting to connect to PostgreSQL...")
        engine = get_engine()
        connection = engine.connect()
        transaction = connection.begin()
        print("✅ Successfully connected to PostgreSQL.\n")

        # Step 1: Read from source table
        print("📥 Reading data from 'attryb.order_day_wise'...")
        input_df = pd.read_sql("SELECT * FROM attryb.order_day_wise", connection)

        print("\n📦 Original Data (First 5 Rows):")
        print(input_df.head())

        # Step 2: Apply transformation
        print("\n🔧 Performing transformation on 'created_orders'...")
        input_df["transformed_created_orders"] = input_df["created_orders"] * 100

        print("\n🚀 Transformed Data (First 5 Rows):")
        print(input_df.head())

        # Step 3: Save to a new table
        target_table = "order_day_wise_transformed"
        print(f"\n💾 Saving transformed data to '{target_table}' in schema 'attryb'...")

        try:
            # Pass the connection here since we started the transaction on it
            input_df.to_sql(
                name=target_table,
                con=connection,
                schema="attryb",
                index=False,
                if_exists="replace",
                dtype={
                    "created_orders": Integer(),
                    "transformed_created_orders": Integer(),
                },
            )
            transaction.commit()
            print(f"✅ Transformation complete. Data saved to 'attryb.{target_table}' table.\n")
        except Exception:
            transaction.rollback()
            raise

    except SQLAlchemyError as e:
        print("❌ Database operation failed:")
        print(str(e))

    except Exception as e:
        print("❌ Unexpected error:")
        print(str(e))

    finally:
        try:
            if connection:
                connection.close()
                print("🔒 PostgreSQL connection closed.")
        except Exception as close_error:
            print("⚠️ Failed to close the connection:")
            print(str(close_error))
