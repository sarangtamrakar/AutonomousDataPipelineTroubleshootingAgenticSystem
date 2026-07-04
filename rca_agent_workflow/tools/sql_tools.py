
import boto3
import time


try:
    from config import cfg
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    from rca_agent_workflow.config import cfg



# Make sure your AWS credentials and region are correctly configured
athena_client = boto3.client('athena', region_name="us-east-1")
database = cfg.ATHENA_DB
s3_output = cfg.ATHENA_OUTPUT

def execute_athena_query(query: str) -> dict:
    """Executes a query on AWS Athena and returns a Python dictionary."""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': s3_output}
    )
    
    execution_id = response['QueryExecutionId']
    
    # Poll for query completion
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = status['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED']:
            break
        elif state in ['FAILED', 'CANCELLED']:
            reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            raise Exception(f"Athena Query Failed: {reason}")
        time.sleep(1)
        
    return _parse_athena_results(execution_id)

def _parse_athena_results(execution_id: str) -> dict:
    """Helper to convert Athena response rows into a clean dictionary."""
    results = athena_client.get_query_results(QueryExecutionId=execution_id)
    column_info = results['ResultSet']['ResultSetMetadata']['ColumnInfo']
    headers = [col['Name'] for col in column_info]
    
    # Skip the first row as it contains headers
    data_rows = results['ResultSet']['Rows'][1:]
    
    rows_list = []
    records_list = []
    
    for row in data_rows:
        # Extract the raw values
        values = [val.get('VarCharValue', None) for val in row['Data']]
        rows_list.append(values)
        
        # Zip headers with values to create key-value pairs (Highly recommended for LLMs)
        records_list.append(dict(zip(headers, values)))
        
    # Fixed the dictionary syntax error here
    return {
        "records": records_list  # e.g., [{"region": "North", "total_revenue": "100.00"}]
    }

if __name__ == "__main__":
    test_query = """ 
    SELECT 
        c.region,
        round(SUM(f.total_sale_amount),2) as total_revenue
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY c.region
    ORDER BY total_revenue DESC;
    """
    
    print("Executing query...")
    try:
        final_result = execute_athena_query(test_query)
        print("\n--- Raw Rows Output ---")
        print(final_result["rows"])
        
        print("\n--- LLM Friendly Records Output ---")
        print(final_result["records"])
        
    except Exception as e:
        print(f"Error: {e}")