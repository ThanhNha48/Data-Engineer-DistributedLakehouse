# ./dags/dbt_operator.py
import os
import json
import logging
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException

from dbt.cli.main import dbtRunner, dbtRunnerResult


class DbtOperator(BaseOperator):
    """
    Custom Airflow Operator chạy lệnh dbt một cách đáng tin cậy.
    Hỗ trợ: run, seed, test, docs generate, ls, snapshot,...
    """

    template_fields = ("select", "exclude", "dbt_vars")

    def __init__(
        self,
        dbt_root_dir: str,
        dbt_command: str,
        target: Optional[str] = None,
        select: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        dbt_vars: Optional[Dict[str, Any]] = None,
        full_refresh: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_root_dir = dbt_root_dir
        self.dbt_command = dbt_command.strip()
        self.target = target
        self.select = select or []
        self.exclude = exclude or []
        self.dbt_vars = dbt_vars or {}
        self.full_refresh = full_refresh
        self.runner = dbtRunner()

    def execute(self, context: Context) -> Any:
        logger = logging.getLogger("airflow.task")

        if not os.path.exists(self.dbt_root_dir):
            raise AirflowException(f"dbt project directory không tồn tại: {self.dbt_root_dir}")

        if not os.path.exists(os.path.join(self.dbt_root_dir, "dbt_project.yml")):
            raise AirflowException(f"Không tìm thấy dbt_project.yml trong {self.dbt_root_dir}")

        log_dir = os.path.join(self.dbt_root_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)

        base_cmd = self.dbt_command.split()

        args: List[str] = base_cmd + [
            "--project-dir", self.dbt_root_dir,
            "--profiles-dir", self.dbt_root_dir,
        ]

        if self.target:
            args += ["--target", self.target]
        if self.full_refresh:
            args += ["--full-refresh"]
        if self.select:
            args += ["--select"] + self.select
        if self.exclude:
            args += ["--exclude"] + self.exclude
        if self.dbt_vars:
            args += ["--vars", json.dumps(self.dbt_vars)]

        logger.info(f"Running dbt command: dbt {' '.join(args)}")

        res: dbtRunnerResult = self.runner.invoke(args)

        if res.success:
            logger.info(f"dbt {' '.join(base_cmd)} executed successfully!")

            if res.result is not None:
                if isinstance(res.result, list) and res.result:
                    for item in res.result:
                        node_name = getattr(item.node, "name", "unknown")
                        status = getattr(item, "status", "success")
                        message = getattr(item, "message", "")
                        logger.info(f"Model: {node_name} | Status: {status} | {message}")
                else:
                    logger.info(f"Command completed (result: {res.result})")
            else:
                logger.info("Command completed with no detailed results.")

            return res.result

        else:
            exception_msg = str(res.exception) if res.exception else "Unknown error"
            logger.error(f"dbt command failed: {exception_msg}")

            if hasattr(res, "result") and res.result is not None:
                logger.error(f"Partial result: {res.result}")

            raise AirflowException(f"dbt {' '.join(base_cmd)} failed: {exception_msg}")