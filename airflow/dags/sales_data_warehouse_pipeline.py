"""
DAG: sales_data_warehouse
Purpose: Execute the sales data transformation Python script
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os
import logging

# Configure logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,  # Disabled as requested
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sales_data_warehouse',
    default_args=default_args,
    description='Run sales data transformation script',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['sales', 'transformation'],
    max_active_runs=1,
)

def run_transformation(**context):
    """
    Execute the transformation script
    """
    script_path = '/opt/airflow/scripts/data_transformation.py'
    
    logger.info("=" * 60)
    logger.info("🚀 STARTING TRANSFORMATION SCRIPT")
    logger.info("=" * 60)
    
    # Check if script exists
    if not os.path.exists(script_path):
        logger.error(f"❌ Script not found at: {script_path}")
        # List available files for debugging
        logger.info("📁 Available scripts:")
        for file in os.listdir('/opt/airflow/scripts/'):
            logger.info(f"   - {file}")
        raise FileNotFoundError(f"Script not found at {script_path}")
    
    logger.info(f"✅ Found script: {script_path}")
    
    # Execute the script
    start_time = datetime.now()
    logger.info(f"▶️ Execution started at: {start_time}")
    
    try:
        result = subprocess.run(
            [sys.executable, script_path], 
            capture_output=True, 
            text=True,
            timeout=1800  # 30 minute timeout
        )
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # Log output (summary only to avoid log flooding)
        if result.stdout:
            # Log first and last few lines
            lines = result.stdout.strip().split('\n')
            if len(lines) > 20:
                logger.info("📝 Script output (first 10 lines):")
                for line in lines[:10]:
                    if line.strip():
                        logger.info(f"   {line}")
                logger.info("   ...")
                logger.info("📝 Script output (last 10 lines):")
                for line in lines[-10:]:
                    if line.strip():
                        logger.info(f"   {line}")
            else:
                logger.info("📝 Script output:")
                for line in lines:
                    if line.strip():
                        logger.info(f"   {line}")
        
        if result.stderr:
            logger.warning("⚠️ Script warnings:")
            for line in result.stderr.strip().split('\n'):
                if line.strip():
                    logger.warning(f"   {line}")
        
        if result.returncode != 0:
            logger.error(f"❌ Script failed with exit code: {result.returncode}")
            raise Exception(f"Script failed: {result.stderr[:500]}")
        
        logger.info(f"✅ Script completed successfully in {execution_time:.2f} seconds")
        
        # Push to XCom for potential use by other tasks
        context['ti'].xcom_push(key='execution_time', value=execution_time)
        
        return f"Success - {execution_time:.2f}s"
        
    except subprocess.TimeoutExpired:
        logger.error("❌ Script timed out after 30 minutes")
        raise
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        raise

def verify_completion(**context):
    """
    Quick verification that the script ran (checks if key tables have data)
    Optional - can be removed if you don't need verification
    """
    logger.info("=" * 60)
    logger.info("🔍 QUICK VERIFICATION")
    logger.info("=" * 60)
    
    try:
        from sqlalchemy import create_engine, text
        
        # Database connection
        engine = create_engine('postgresql://source_user:source_pass@postgres-source:5432/sales_dw')
        
        with engine.connect() as conn:
            # Check fact table has rows
            result = conn.execute(text("SELECT COUNT(*) FROM fact_sales"))
            count = result.scalar()
            
            logger.info(f"📊 Fact table has {count:,} rows")
            
            if count == 0:
                logger.warning("⚠️ Fact table is empty!")
            else:
                logger.info("✅ Verification passed")
                
    except Exception as e:
        logger.warning(f"⚠️ Verification skipped: {e}")
    
    return "Verification complete"

# Define tasks
run_task = PythonOperator(
    task_id='run_transformation',
    python_callable=run_transformation,
    dag=dag,
)

verify_task = PythonOperator(
    task_id='verify_results',
    python_callable=verify_completion,
    dag=dag,
)

# Set task dependencies
run_task >> verify_task