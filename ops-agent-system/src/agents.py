"""Six specialized agents for DevOps automation."""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import List, Optional

from src.core import AgentCapability, BaseAgent, MessageBus, TaskContext
from src.ai_backends import BaseLLM, MockLLM


# ─── Orchestrator Agent ───
class OrchestratorAgent(BaseAgent):
    CAPABILITY = AgentCapability.ORCHESTRATION
    INPUT_STREAM = "stream:orchestrator"

    def __init__(self, llm: Optional[BaseLLM] = None, **kwargs):
        super().__init__(**kwargs)
        self.llm = llm or MockLLM()
        self._pending_workflows: dict = {}  # workflow_id -> list of subtask contexts

    async def handle(self, ctx: TaskContext) -> Optional[TaskContext]:
        print(f"  [Orchestrator] Received: {ctx.task_id} | status={ctx.status}")

        # Sub-任务结果回来了 → 聚合
        if ctx.params.get("is_subtask_result"):
            wf_id = ctx.workflow_id
            if wf_id not in self._pending_workflows:
                self._pending_workflows[wf_id] = {"results": [], "total": 0}
            self._pending_workflows[wf_id]["results"].append(ctx)
            self._pending_workflows[wf_id].setdefault("total", 3)

            pending = self._pending_workflows[wf_id]
            if len(pending["results"]) >= pending["total"]:
                results = pending["results"]
                all_ok = all(r.status == "success" for r in results)
                ctx.result = {
                    "subtask_results": [r.result for r in results],
                    "all_success": all_ok,
                    "summary": f"{len(results)} subtasks done, all_ok={all_ok}",
                }
                ctx.status = "success" if all_ok else "partial_failure"
                ctx.params = {}
                del self._pending_workflows[wf_id]
                print(f"  [Orchestrator] Workflow {wf_id} complete: {ctx.status}")
                return ctx
            return None  # 等待更多结果

        # 新任务 → 分解
        response = await self.llm.chat(
            prompt=f"Decompose this ops task into subtasks: {ctx.params}",
            system="You are an ops orchestrator. Output JSON with 'subtasks' list.",
        )

        subtask_descriptions = [
            {"capability": "log_analysis", "desc": "Analyze logs"},
            {"capability": "monitoring", "desc": "Check services"},
            {"capability": "deployment", "desc": "Check deployments"},
        ]

        wf_id = ctx.workflow_id
        self._pending_workflows[wf_id] = {"results": [], "total": len(subtask_descriptions)}

        for st in subtask_descriptions:
            sub_ctx = TaskContext(
                task_id=f"sub-{uuid.uuid4().hex[:8]}",
                workflow_id=wf_id,
                parent_task_id=ctx.task_id,
                capability=st["capability"],
                params={"description": st["desc"], **ctx.params},
            )
            stream = f"stream:{st['capability']}"
            await self.bus.publish(stream, sub_ctx)
            print(f"  [Orchestrator] Dispatched subtask → {stream}")

        ctx.status = "awaiting_subtasks"
        return None

    def route_to(self, ctx: TaskContext) -> List[str]:
        return ["stream:reporting"]


# ─── Monitor Agent ───
class MonitorAgent(BaseAgent):
    CAPABILITY = AgentCapability.MONITORING
    INPUT_STREAM = "stream:monitoring"
    MONITOR_INTERVAL = 30  # seconds

    def __init__(self, llm: Optional[BaseLLM] = None, **kwargs):
        super().__init__(**kwargs)
        self.llm = llm or MockLLM()
        self._check_tasks: dict = {}

    async def handle(self, ctx: TaskContext) -> Optional[TaskContext]:
        print(f"  [Monitor] Checking: {ctx.task_id}")

        # Simulate checking Prometheus/external metrics
        await asyncio.sleep(0.5)

        metric_value = 0.92  # mock: CPU 92%
        severity = "critical" if metric_value > 0.9 else "warning" if metric_value > 0.7 else "info"

        ctx.result = {
            "cpu_usage": f"{metric_value*100}%",
            "memory_usage": "67%",
            "disk_usage": "45%",
            "dependent_services": {"database": "healthy", "redis": "healthy"},
            "severity": severity,
        }
        ctx.status = "success"

        # If this is a monitoring-triggered check, escalate to orchestrator
        if ctx.params.get("escalate_on_critical") and severity == "critical":
            ctx.result["escalated"] = True
            print(f"  [Monitor] CRITICAL: escalating to orchestrator")

        print(f"  [Monitor] Done: CPU={metric_value*100}%, severity={severity}")
        return ctx

    def route_to(self, ctx: TaskContext) -> List[str]:
        if ctx.result.get("escalated"):
            return ["stream:orchestrator"]
        if ctx.params.get("is_subtask_result"):
            return ["stream:orchestrator"]
        return []

    async def run_periodic_check(self) -> None:
        """Periodic health check (call externally or via cron)."""
        while not self._stop_event.is_set():
            alert_ctx = TaskContext(
                task_id=f"monitor-{uuid.uuid4().hex[:8]}",
                workflow_id=f"wf-auto-{uuid.uuid4().hex[:6]}",
                capability="monitoring",
                params={"auto_check": True, "escalate_on_critical": True},
            )
            await self.bus.publish(self.INPUT_STREAM, alert_ctx)
            await asyncio.sleep(self.MONITOR_INTERVAL)


# ─── Deploy Agent ───
class DeployAgent(BaseAgent):
    CAPABILITY = AgentCapability.DEPLOYMENT
    INPUT_STREAM = "stream:deployment"

    async def handle(self, ctx: TaskContext) -> Optional[TaskContext]:
        action = ctx.params.get("action", "deploy")
        service = ctx.params.get("service_name", "unknown-service")
        print(f"  [Deploy] {action} for {service}")

        await asyncio.sleep(0.8)  # Simulate deployment steps

        if action == "rollback":
            ctx.result = {
                "status": "rolled_back",
                "from_version": ctx.params.get("from_version", "unknown"),
                "to_version": "v2.3.0",
                "health_check": "passed",
                "duration_seconds": 12.5,
            }
        else:
            ctx.result = {
                "status": "deployed",
                "version": ctx.params.get("version", "unknown"),
                "strategy": ctx.params.get("strategy", "rolling"),
                "health_check": "passed",
                "duration_seconds": 45.2,
            }

        ctx.status = "success"
        print(f"  [Deploy] Done: {ctx.result['status']}")
        return ctx

    def route_to(self, ctx: TaskContext) -> List[str]:
        if ctx.params.get("is_subtask_result"):
            return ["stream:orchestrator"]
        return []


# ─── Log Analyzer Agent ───
class LogAnalyzerAgent(BaseAgent):
    CAPABILITY = AgentCapability.LOG_ANALYSIS
    INPUT_STREAM = "stream:log_analysis"

    def __init__(self, llm: Optional[BaseLLM] = None, **kwargs):
        super().__init__(**kwargs)
        self.llm = llm or MockLLM()

    async def handle(self, ctx: TaskContext) -> Optional[TaskContext]:
        service = ctx.params.get("service_name", "unknown")
        print(f"  [LogAnalyzer] Analyzing logs for {service}")

        await asyncio.sleep(0.6)

        response = await self.llm.chat(
            prompt=f"Analyze recent error logs for service: {service}",
            system="You are a log analyzer. Find anomalies and error patterns.",
        )

        ctx.result = {
            "service": service,
            "anomalies": ["OutOfMemoryError spike at T+2m", "GC overhead limit reached"],
            "error_count": 12,
            "suspected_root_cause": "Memory leak in v2.3.1 caching layer",
            "analysis": response.content,
        }
        ctx.status = "success"
        print(f"  [LogAnalyzer] Found {ctx.result['error_count']} errors, root cause: memory leak")
        return ctx

    def route_to(self, ctx: TaskContext) -> List[str]:
        if ctx.params.get("is_subtask_result"):
            return ["stream:orchestrator"]
        return ["stream:orchestrator"]


# ─── Remediation Agent ───
class RemediationAgent(BaseAgent):
    CAPABILITY = AgentCapability.REMEDIATION
    INPUT_STREAM = "stream:remediation"

    DANGEROUS_COMMANDS = {"rm -rf /", "DROP TABLE", "DELETE FROM", "mkfs."}

    async def handle(self, ctx: TaskContext) -> Optional[TaskContext]:
        issue = ctx.params.get("description", "unknown issue")
        print(f"  [Remediation] Fixing: {issue}")

        # Safety check
        for cmd in ctx.params.get("commands", []):
            for dangerous in self.DANGEROUS_COMMANDS:
                if dangerous.lower() in cmd.lower():
                    ctx.result = {"status": "blocked", "reason": f"Dangerous command: {cmd}"}
                    ctx.status = "failed"
                    return ctx

        await asyncio.sleep(0.7)

        ctx.result = {
            "status": "fixed",
            "actions_taken": [
                "Rolled back to v2.3.0",
                "Restarted affected pods",
                "Cleared leaked temp files",
            ],
            "verified": True,
            "duration_seconds": 8.3,
        }
        ctx.status = "success"
        print(f"  [Remediation] Done: {len(ctx.result['actions_taken'])} actions taken")
        return ctx

    def route_to(self, ctx: TaskContext) -> List[str]:
        return ["stream:reporting"]


# ─── Report Agent ───
class ReportAgent(BaseAgent):
    CAPABILITY = AgentCapability.REPORTING
    INPUT_STREAM = "stream:reporting"

    def __init__(self, llm: Optional[BaseLLM] = None, **kwargs):
        super().__init__(**kwargs)
        self.llm = llm or MockLLM()

    async def handle(self, ctx: TaskContext) -> Optional[TaskContext]:
        print(f"  [Report] Generating report for workflow: {ctx.workflow_id}")

        response = await self.llm.chat(
            prompt=f"Generate incident report from: {ctx.result}",
            system="You generate ops incident reports.",
        )

        report = f"""
{'='*60}
INCIDENT REPORT
{'='*60}
Workflow ID : {ctx.workflow_id}
Status      : {ctx.status}
Root Cause  : Memory leak in v2.3.1 caching layer
MTTA        : 5s (Mean Time To Acknowledge)
MTTR        : 30s (Mean Time To Resolve)

Timeline:
  T+0s   - Monitor detected CPU 92% on api-gateway
  T+5s   - Orchestrator dispatched analysis subtasks
  T+20s  - DeployAgent rolled back to v2.3.0
  T+25s  - RemediationAgent cleaned up leaked resources
  T+30s  - System fully recovered

Actions Taken:
  1. Rolled back api-gateway from v2.3.1 to v2.3.0
  2. Restarted affected pods
  3. Verified health metrics back to normal

Recommendations:
  - Fix memory leak in v2.3.1 caching layer before re-deploy
  - Add OOM alert threshold at 85% memory
  - Implement canary deployment for risk reduction

{response.content[:300]}
{'='*60}
"""
        ctx.result["report"] = report
        ctx.status = "success"
        print(report)
        return ctx

    def route_to(self, ctx: TaskContext) -> List[str]:
        return []
