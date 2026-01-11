#!/usr/bin/env python3
"""
Parallel Executor - 并行模型调用执行器
实现MCP架构中的并行模型调用功能
"""

import logging
import time
from typing import Dict, Any, List, Callable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class TaskResult:
    """任务执行结果"""
    task_name: str
    result: Any = None
    error: Optional[str] = None
    execution_time: float = 0.0


class ParallelExecutor:
    """
    并行执行器

    功能：
    1. 同时提交多个模型调用任务
    2. 并行执行，节省总响应时间
    3. 统一管理所有任务结果
    4. 支持超时控制
    """

    def __init__(self, max_workers: int = 10):
        """
        初始化并行执行器

        Args:
            max_workers: 最大并行任务数
        """
        self.max_workers = max_workers
        logger.info(f"✅ ParallelExecutor initialized (max_workers={max_workers})")

    def execute_parallel(self,
                         tasks: Dict[str, Callable],
                         timeout: int = 30) -> Dict[str, TaskResult]:
        """
        并行执行多个任务

        Args:
            tasks: 任务字典 {task_name: callable_function}
            timeout: 超时时间（秒）

        Returns:
            执行结果字典 {task_name: TaskResult}
        """
        if not tasks:
            logger.warning("No tasks to execute")
            return {}

        start_time = time.time()
        logger.info(f"🚀 Starting parallel execution of {len(tasks)} tasks")

        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_task = {}
            for task_name, task_func in tasks.items():
                future = executor.submit(self._execute_single_task, task_name, task_func)
                future_to_task[future] = task_name

            # 收集结果
            for future in as_completed(future_to_task, timeout=timeout):
                task_name = future_to_task[future]
                try:
                    result = future.result()
                    results[task_name] = result
                    logger.info(f"   ✅ Task '{task_name}' completed: {result.execution_time:.3f}s")
                except Exception as e:
                    logger.error(f"   ❌ Task '{task_name}' failed: {e}")
                    results[task_name] = TaskResult(
                        task_name=task_name,
                        error=str(e)
                    )

        total_time = time.time() - start_time
        logger.info(f"✅ All {len(tasks)} tasks completed in {total_time:.3f}s")

        return results

    def _execute_single_task(self, task_name: str, task_func: Callable) -> TaskResult:
        """
        执行单个任务

        Args:
            task_name: 任务名称
            task_func: 任务函数

        Returns:
            TaskResult
        """
        start_time = time.time()
        try:
            result = task_func()
            return TaskResult(
                task_name=task_name,
                result=result,
                execution_time=time.time() - start_time
            )
        except Exception as e:
            logger.error(f"Task '{task_name}' error: {e}")
            return TaskResult(
                task_name=task_name,
                error=str(e),
                execution_time=time.time() - start_time
            )

    def execute_parallel_with_callbacks(self,
                                         tasks: Dict[str, Callable],
                                         on_complete: Optional[Callable] = None,
                                         on_error: Optional[Callable] = None,
                                         timeout: int = 30) -> Dict[str, TaskResult]:
        """
        带回调的并行执行

        Args:
            tasks: 任务字典
            on_complete: 任务完成回调
            on_error: 任务错误回调
            timeout: 超时时间

        Returns:
            执行结果字典
        """
        if not tasks:
            return {}

        logger.info(f"🚀 Starting parallel execution with callbacks ({len(tasks)} tasks)")

        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {}
            for task_name, task_func in tasks.items():
                future = executor.submit(self._execute_single_task_with_callbacks,
                                         task_name, task_func, on_complete, on_error)
                future_to_task[future] = task_name

            for future in as_completed(future_to_task, timeout=timeout):
                task_name = future_to_task[future]
                try:
                    result = future.result()
                    results[task_name] = result
                except Exception as e:
                    logger.error(f"Task '{task_name}' callback error: {e}")

        return results

    def _execute_single_task_with_callbacks(self,
                                             task_name: str,
                                             task_func: Callable,
                                             on_complete: Optional[Callable],
                                             on_error: Optional[Callable]) -> TaskResult:
        """执行单个任务（带回调）"""
        start_time = time.time()
        try:
            result = task_func()
            task_result = TaskResult(
                task_name=task_name,
                result=result,
                execution_time=time.time() - start_time
            )

            if on_complete:
                on_complete(task_result)

            return task_result

        except Exception as e:
            task_result = TaskResult(
                task_name=task_name,
                error=str(e),
                execution_time=time.time() - start_time
            )

            if on_error:
                on_error(task_result)

            return task_result


# ============== Usage Example ==============
if __name__ == "__main__":
    # 示例：并行执行多个模型调用

    def dummy_llm_call_1():
        time.sleep(1)
        return "Result from model 1"

    def dummy_llm_call_2():
        time.sleep(1.5)
        return "Result from model 2"

    def dummy_llm_call_3():
        time.sleep(0.8)
        return "Result from model 3"

    # 定义任务
    tasks = {
        "arbitration": dummy_llm_call_1,
        "rewrite": dummy_llm_call_2,
        "rejection": dummy_llm_call_3
    }

    # 并行执行
    executor = ParallelExecutor(max_workers=5)
    results = executor.execute_parallel(tasks)

    # 打印结果
    for task_name, result in results.items():
        print(f"{task_name}: {result.result}")
