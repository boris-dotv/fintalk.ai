#!/usr/bin/env python3
"""
FinTalk.AI - Simplified Docker-based OSWorld Adapter
A lightweight OSWorld-like environment using Docker for sandboxed execution.

This provides:
1. Docker container isolation
2. Sandboxed Python execution
3. Secure SQL queries in container
4. File system isolation
"""

import docker
import logging
import sqlite3
import pandas as pd
import os
import json
import time
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class DockerOSWorldAdapter:
    """
    Docker-based OSWorld adapter for FinTalk.AI.
    Provides a sandboxed environment for secure code execution.
    """

    def __init__(self,
                 data_dir: str = None,
                 image_name: str = "python:3.10-slim",
                 container_name: str = "fintalk-osworld-sandbox"):
        """
        Initialize Docker OSWorld adapter.

        Args:
            data_dir: Directory containing CSV data files
            image_name: Docker image to use
            container_name: Name for the container
        """
        self.data_dir = data_dir or os.path.join(os.path.dirname(__file__), "..", "data")
        self.image_name = image_name
        self.container_name = container_name
        self.client: Optional[docker.DockerClient] = None
        self.container: Optional[docker.models.containers.Container] = None
        self.local_db: Optional[sqlite3.Connection] = None

        logger.info("🐳 Initializing Docker-based OSWorld adapter...")

        try:
            # Initialize Docker client
            self.client = docker.from_env()
            self.client.ping()  # Test connection

            logger.info("✅ Docker connection established")
            self._init_docker_container()

        except Exception as e:
            logger.warning(f"⚠️  Docker initialization failed: {e}")
            logger.info("Falling back to local mode...")
            self._init_local_db()

    def _init_docker_container(self):
        """Initialize and start Docker container for OSWorld."""
        try:
            # Check if container already exists
            try:
                existing = self.client.containers.get(self.container_name)
                existing.stop()
                existing.remove()
                logger.info("Removed existing container")
            except:
                pass  # Container doesn't exist

            # Create and start new container
            logger.info(f"📦 Starting container: {self.container_name}")

            # Mount data directory
            volumes = {
                os.path.abspath(self.data_dir): {
                    'bind': '/data',
                    'mode': 'ro'  # Read-only
                }
            }

            self.container = self.client.containers.run(
                self.image_name,
                name=self.container_name,
                volumes=volumes,
                command="tail -f /dev/null",  # Keep container running
                detach=True,
                remove=False,
                mem_limit='512m',
                cpu_period=100000,
                cpu_quota=50000,  # 0.5 CPU
                network_disabled=False  # Allow network for API calls
            )

            # Wait for container to be ready
            time.sleep(2)

            # Install dependencies in container
            self._install_container_deps()

            # Setup database in container
            self._setup_container_db()

            logger.info("✅ Docker OSWorld container ready")

        except Exception as e:
            logger.error(f"❌ Failed to setup Docker container: {e}")
            raise

    def _install_container_deps(self):
        """Install required dependencies in the container."""
        logger.info("📦 Installing dependencies in container...")

        commands = [
            "apt-get update",
            "apt-get install -y sqlite3",
            "pip install pandas requests"
        ]

        for cmd in commands:
            try:
                exit_code, output = self.container.exec_run(cmd)
                if exit_code != 0:
                    logger.warning(f"Command failed: {cmd}")
            except Exception as e:
                logger.warning(f"Failed to run command: {cmd} - {e}")

    def _setup_container_db(self):
        """Setup SQLite database in the container."""
        logger.info("🗄️  Setting up database in container...")

        # Python script to create database
        setup_script = f"""
import sqlite3
import pandas as pd
import os
import base64

csv_files = {{
    'companies': '/data/company.csv',
    'management': '/data/management.csv',
    'shareholders': '/data/shareholder.csv'
}}

conn = sqlite3.connect('/tmp/fintalk.db')

for table_name, csv_path in csv_files.items():
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, encoding='utf-8', encoding_errors='ignore')
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Loaded {{len(df)}} rows into {{table_name}}")

conn.commit()
conn.close()
print("Database created: /tmp/fintalk.db")
"""

        # Encode script to base64 and execute directly
        import base64
        script_b64 = base64.b64encode(setup_script.encode('utf-8')).decode('utf-8')

        # Execute setup by decoding and running
        exec_cmd = f'python -c "import base64; exec(base64.b64decode(\'{script_b64}\').decode(\'utf-8\'))"'

        exit_code, output = self.container.exec_run(exec_cmd)

        if exit_code == 0:
            logger.info(f"✅ {output.decode('utf-8').strip()}")
        else:
            logger.warning(f"⚠️  Container DB setup had issues: {output.decode('utf-8')}")

    def _init_local_db(self):
        """Fallback to local SQLite database."""
        self.local_db = sqlite3.connect(':memory:', check_same_thread=False)

        csv_files = {
            "companies": os.path.join(self.data_dir, "company.csv"),
            "management": os.path.join(self.data_dir, "management.csv"),
            "shareholders": os.path.join(self.data_dir, "shareholder.csv")
        }

        for table_name, file_path in csv_files.items():
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, encoding='utf-8', encoding_errors='ignore')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='latin-1')
                df.to_sql(table_name, self.local_db, if_exists='replace', index=False)
                logger.info(f"   Loaded {len(df)} rows into '{table_name}'")

        logger.info("✅ Local database initialized (fallback mode)")

    def execute_sql(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute SQL in the sandbox environment.

        Args:
            sql: SQL query to execute

        Returns:
            Query results as list of dictionaries
        """
        if self.container:
            return self._execute_sql_in_container(sql)
        else:
            return self._execute_sql_local(sql)

    def _execute_sql_in_container(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL in Docker container."""
        try:
            import base64

            # Encode SQL to base64 to avoid escaping issues
            sql_b64 = base64.b64encode(sql.encode('utf-8')).decode('utf-8')

            # Create Python script that decodes and executes SQL
            script = f"""
import sqlite3
import json
import base64

# Decode SQL from base64
sql = base64.b64decode('{sql_b64}').decode('utf-8')

conn = sqlite3.connect('/tmp/fintalk.db')
cursor = conn.cursor()
cursor.execute(sql)

columns = [desc[0] for desc in cursor.description]
rows = cursor.fetchall()

results = [dict(zip(columns, row)) for row in rows]
print(json.dumps(results))

conn.close()
"""

            # Execute in container
            exit_code, output = self.container.exec_run(f'python -c "{script}"')

            if exit_code == 0:
                results = json.loads(output.decode('utf-8'))
                logger.info(f"✅ Container SQL executed: {len(results)} rows")
                return results
            else:
                logger.error(f"Container SQL error: {output.decode('utf-8')}")
                return None

        except Exception as e:
            logger.error(f"Container execution error: {e}")
            return None

    def _execute_sql_local(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL locally (fallback)."""
        try:
            cursor = self.local_db.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Local SQL error: {e}")
            return None

    def execute_python(self, code: str) -> Any:
        """
        Execute Python code in Docker sandbox.

        Args:
            code: Python code to execute

        Returns:
            Execution result
        """
        if self.container:
            return self._execute_python_in_container(code)
        else:
            # Local execution (not sandboxed)
            try:
                exec_locals = {"result": None}
                exec(code, globals(), exec_locals)
                return exec_locals.get('result')
            except Exception as e:
                logger.error(f"Local Python error: {e}")
                return None

    def _execute_python_in_container(self, code: str) -> Any:
        """Execute Python code in Docker container."""
        try:
            # Execute code in container
            exit_code, output = self.container.exec_run(f'python -c "{code}"')

            if exit_code == 0:
                return output.decode('utf-8')
            else:
                logger.error(f"Container Python error: {output.decode('utf-8')}")
                return None

        except Exception as e:
            logger.error(f"Container Python execution error: {e}")
            return None

    def get_container_info(self) -> Dict[str, Any]:
        """Get information about the Docker container."""
        if self.container:
            self.container.reload()
            return {
                "id": self.container.short_id,
                "name": self.container.name,
                "image": str(self.container.image),
                "status": self.container.status,
                "is_running": self.container.status == "running"
            }
        return None

    def close(self):
        """Cleanup Docker resources."""
        if self.container:
            try:
                self.container.stop()
                self.container.remove()
                logger.info("✅ Docker container stopped and removed")
            except:
                pass

        if self.local_db:
            self.local_db.close()
            logger.info("✅ Local database closed")


# ============== Demo ==============
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("="*60)
    print("🐳 Docker OSWorld Adapter Demo")
    print("="*60)

    # Initialize adapter
    adapter = DockerOSWorldAdapter()

    # Show container info
    if adapter.container:
        info = adapter.get_container_info()
        print(f"\n📦 Container Info:")
        print(f"   Name: {info['name']}")
        print(f"   ID: {info['id']}")
        print(f"   Status: {info['status']}")
        print(f"   Running: {info['is_running']}")

    # Test SQL execution
    print("\n🔧 Testing SQL execution in container...")
    result = adapter.execute_sql("SELECT name, employee_size FROM companies WHERE name LIKE '%ZA Bank%' LIMIT 1")

    if result:
        print(f"✅ Result: {result[0]}")
    else:
        print("❌ SQL execution failed")

    # Cleanup
    print("\n🧹 Cleaning up...")
    adapter.close()
    print("✅ Demo complete!")
