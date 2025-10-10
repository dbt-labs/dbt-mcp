import logging
from collections.abc import Sequence
from typing import Any

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config_providers import (
    AdminApiConfig,
    ConfigProvider,
)
from dbt_mcp.dbt_admin.client import DbtAdminAPIClient
from dbt_mcp.dbt_admin.constants import JobRunStatus, STATUS_MAP
from dbt_mcp.dbt_admin.run_results_errors import ErrorFetcher
from dbt_mcp.prompts.prompts import get_prompt
from dbt_mcp.tools.annotations import create_tool_annotations
from dbt_mcp.tools.definitions import ToolDefinition
from dbt_mcp.tools.register import register_tools
from dbt_mcp.tools.tool_names import ToolName

logger = logging.getLogger(__name__)


def create_admin_api_tool_definitions(
    admin_client: DbtAdminAPIClient,
    admin_api_config_provider: ConfigProvider[AdminApiConfig],
) -> list[ToolDefinition]:
    async def list_jobs(
        # TODO: add support for project_id in the future
        # project_id: Optional[int] = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """List jobs in an account."""
        admin_api_config = await admin_api_config_provider.get_config()
        params = {}
        # if project_id:
        #     params["project_id"] = project_id
        if admin_api_config.prod_environment_id:
            params["environment_id"] = admin_api_config.prod_environment_id
        if limit:
            params["limit"] = limit
        if offset:
            params["offset"] = offset
        return await admin_client.list_jobs(admin_api_config.account_id, **params)

    async def get_job_details(job_id: int) -> dict[str, Any]:
        """Get details for a specific job."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.get_job_details(admin_api_config.account_id, job_id)

    async def trigger_job_run(
        job_id: int,
        cause: str = "Triggered by dbt MCP",
        git_branch: str | None = None,
        git_sha: str | None = None,
        schema_override: str | None = None,
    ) -> dict[str, Any]:
        """Trigger a job run."""
        admin_api_config = await admin_api_config_provider.get_config()
        kwargs = {}
        if git_branch:
            kwargs["git_branch"] = git_branch
        if git_sha:
            kwargs["git_sha"] = git_sha
        if schema_override:
            kwargs["schema_override"] = schema_override
        return await admin_client.trigger_job_run(
            admin_api_config.account_id, job_id, cause, **kwargs
        )

    async def list_jobs_runs(
        job_id: int | None = None,
        status: JobRunStatus | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """List runs in an account."""
        admin_api_config = await admin_api_config_provider.get_config()
        params: dict[str, Any] = {}
        if job_id:
            params["job_definition_id"] = job_id
        if status:
            status_id = STATUS_MAP[status]
            params["status"] = status_id
        if limit:
            params["limit"] = limit
        if offset:
            params["offset"] = offset
        if order_by:
            params["order_by"] = order_by
        return await admin_client.list_jobs_runs(admin_api_config.account_id, **params)

    async def get_job_run_details(
        run_id: int,
    ) -> dict[str, Any]:
        """Get details for a specific job run."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.get_job_run_details(
            admin_api_config.account_id, run_id
        )

    async def cancel_job_run(run_id: int) -> dict[str, Any]:
        """Cancel a job run."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.cancel_job_run(admin_api_config.account_id, run_id)

    async def retry_job_run(run_id: int) -> dict[str, Any]:
        """Retry a failed job run."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.retry_job_run(admin_api_config.account_id, run_id)

    async def list_job_run_artifacts(run_id: int) -> list[str]:
        """List artifacts for a job run."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.list_job_run_artifacts(
            admin_api_config.account_id, run_id
        )

    async def get_job_run_artifact(
        run_id: int, artifact_path: str, step: int | None = None
    ) -> Any:
        """Get a specific job run artifact."""
        admin_api_config = await admin_api_config_provider.get_config()
        return await admin_client.get_job_run_artifact(
            admin_api_config.account_id, run_id, artifact_path, step
        )

    async def download_job_run_artifact(
        run_id: int, artifact_path: str, save_path: str, step: int | None = None
    ) -> dict[str, Any]:
        """Download a specific job run artifact to local filesystem."""
        try:
            admin_api_config = await admin_api_config_provider.get_config()
            return await admin_client.download_job_run_artifact(
                admin_api_config.account_id, run_id, artifact_path, save_path, step
            )
        except Exception as e:
            logger.error(f"Error downloading artifact {artifact_path} for run {run_id}: {e}")
            return {
                "success": False,
                "error": str(e),
                "artifact_path": artifact_path,
                "save_path": save_path,
            }

    async def get_job_run_error(run_id: int) -> dict[str, Any] | str:
        """Get focused error information for a failed job run."""
        try:
            admin_api_config = await admin_api_config_provider.get_config()
            run_details = await admin_client.get_job_run_details(
                admin_api_config.account_id, run_id, include_logs=True
            )
            error_fetcher = ErrorFetcher(
                run_id, run_details, admin_client, admin_api_config
            )
            return await error_fetcher.analyze_run_errors()

        except Exception as e:
            logger.error(f"Error getting run error details for {run_id}: {e}")
            return str(e)


    async def get_ci_manifest_and_deferred_manifest(run_id: int) -> dict[str, Any]:
        """Download and retrieve manifest.json files for CI job analysis."""
        try:
            import json
            import os
            import tempfile
            
            admin_api_config = await admin_api_config_provider.get_config()
            
            # Get job run details to find deferring_run_id
            run_details = await admin_client.get_job_run_details(
                admin_api_config.account_id, run_id
            )
            
            # Create temporary directory for artifacts
            temp_dir = tempfile.mkdtemp()
            
            # Download current manifest
            current_manifest_path = os.path.join(temp_dir, f"run_{run_id}_manifest.json")
            
            current_download = await admin_client.download_job_run_artifact(
                admin_api_config.account_id, run_id, "manifest.json", current_manifest_path
            )
            
            with open(current_manifest_path, 'r') as f:
                current_manifest = json.load(f)

            # Look for deferring_run_id
            deferring_run_id = None
            deferred_manifest = None
            deferred_manifest_path = None
            
            # Check run details for deferring information
            if "deferring_run_id" in run_details:
                deferring_run_id = run_details["deferring_run_id"]
            elif "defer_run_id" in run_details:
                deferring_run_id = run_details["defer_run_id"]
            
            # Download deferred manifest if available
            if deferring_run_id:
                try:
                    deferred_manifest_path = os.path.join(temp_dir, f"deferred_{deferring_run_id}_manifest.json")
                    
                    await admin_client.download_job_run_artifact(
                        admin_api_config.account_id, deferring_run_id, "manifest.json", deferred_manifest_path
                    )
                    
                    # Parse deferred manifest
                    with open(deferred_manifest_path, 'r') as f:
                        deferred_manifest = json.load(f)
                        
                    logger.info(f"Successfully downloaded deferred manifest from run {deferring_run_id}")
                    
                except Exception as e:
                    logger.warning(f"Could not download deferred manifest from run {deferring_run_id}: {e}")
                    deferring_run_id = None
            
            # Count models
            current_models = {k: v for k, v in current_manifest.get("nodes", {}).items() if k.startswith("model.")}
            deferred_models = {}
            if deferred_manifest:
                deferred_models = {k: v for k, v in deferred_manifest.get("nodes", {}).items() if k.startswith("model.")}
            
            return {
                "deferring_run_id": deferring_run_id,
                "current_run_id": run_id,
                "manifest_comparison_available": deferred_manifest is not None,
                "total_models_current": len(current_models),
                "total_models_deferred": len(deferred_models) if deferred_manifest else None,
                "download_paths": {
                    "current_manifest_path": current_manifest_path,
                    "deferred_manifest_path": deferred_manifest_path,
                    "temp_dir": temp_dir
                }
            }
            
        except Exception as e:
            logger.error(f"Error downloading manifests for run {run_id}: {e}")
            return {"error": f"Failed to download manifests: {str(e)}"}

    async def compare_manifests(
        current_manifest_path: str, 
        deferred_manifest_path: str, 
        comparison_scope: str = "models"
    ) -> dict[str, Any]:
        """Compare two dbt manifest.json files to identify changes between states."""
        try:
            import json
            import os
            
            # Validate file paths
            if not os.path.exists(current_manifest_path):
                return {"error": f"Current manifest file not found: {current_manifest_path}"}
            if not os.path.exists(deferred_manifest_path):
                return {"error": f"Deferred manifest file not found: {deferred_manifest_path}"}
            
            # Read and parse manifest files
            try:
                with open(current_manifest_path, 'r') as f:
                    current_manifest = json.load(f)
                with open(deferred_manifest_path, 'r') as f:
                    deferred_manifest = json.load(f)
            except json.JSONDecodeError as e:
                return {"error": f"Failed to parse JSON files: {str(e)}"}
            except Exception as e:
                return {"error": f"Failed to read manifest files: {str(e)}"}
            
            # Validate manifest structure
            if not isinstance(current_manifest, dict) or not isinstance(deferred_manifest, dict):
                return {"error": "Both manifests must be valid JSON objects"}
            
            if comparison_scope not in ["models", "tests", "all"]:
                comparison_scope = "models"
            
            # Get nodes based on comparison scope
            if comparison_scope == "models":
                current_nodes = {k: v for k, v in current_manifest.get("nodes", {}).items() if k.startswith("model.")}
                deferred_nodes = {k: v for k, v in deferred_manifest.get("nodes", {}).items() if k.startswith("model.")}
            elif comparison_scope == "tests":
                current_nodes = {k: v for k, v in current_manifest.get("nodes", {}).items() if k.startswith("test.")}
                deferred_nodes = {k: v for k, v in deferred_manifest.get("nodes", {}).items() if k.startswith("test.")}
            else:  # all
                current_nodes = current_manifest.get("nodes", {})
                deferred_nodes = deferred_manifest.get("nodes", {})
            
            # Analyze changes
            changed_models = []
            unchanged_models = []
            new_models = []
            removed_models = []
            
            # Find new and changed models
            for unique_id, current_node in current_nodes.items():
                if unique_id not in deferred_nodes:
                    # New model
                    new_models.append({
                        "unique_id": unique_id,
                        "name": current_node.get("name", unique_id.split(".")[-1])
                    })
                else:
                    # Compare existing model
                    deferred_node = deferred_nodes[unique_id]
                    changes = _analyze_node_changes(current_node, deferred_node, unique_id)
                    
                    if changes["has_changes"]:
                        changed_models.append(changes)
                    else:
                        unchanged_models.append({
                            "unique_id": unique_id,
                            "name": current_node.get("name", unique_id.split(".")[-1]),
                            "checksum": current_node.get("checksum", {}).get("checksum")
                        })
            
            # Find removed models
            for unique_id, deferred_node in deferred_nodes.items():
                if unique_id not in current_nodes:
                    removed_models.append({
                        "unique_id": unique_id,
                        "name": deferred_node.get("name", unique_id.split(".")[-1])
                    })
            
            # Analyze dependency changes
            dependency_changes = _analyze_dependency_changes(current_nodes, deferred_nodes)
            
            # Analyze metadata changes
            metadata_changes = _analyze_metadata_changes(current_manifest, deferred_manifest)
            
            # Build summary
            total_models = len(current_nodes)
            summary = f"Found {len(changed_models)} changed models, {len(new_models)} new models, {len(removed_models)} removed models out of {total_models} total models"
            
            return {
                "comparison_summary": summary,
                "changed_models": changed_models,
                "unchanged_models": unchanged_models,
                "new_models": new_models,
                "removed_models": removed_models,
                "dependency_changes": dependency_changes,
                "metadata_changes": metadata_changes,
                "comparison_scope": comparison_scope,
                "total_current_models": len(current_nodes),
                "total_deferred_models": len(deferred_nodes)
            }
            
        except Exception as e:
            logger.error(f"Error comparing manifests: {e}")
            return {"error": f"Failed to compare manifests: {str(e)}"}

    def _analyze_node_changes(current_node: dict, deferred_node: dict, unique_id: str) -> dict[str, Any]:
        """Analyze changes between two nodes."""
        changes = {
            "unique_id": unique_id,
            "name": current_node.get("name", unique_id.split(".")[-1]),
            "has_changes": False,
            "change_type": "unchanged",
            "change_details": "",
            "checksum_changed": False,
            "dependencies_changed": False,
            "materialization_changed": False
        }
        
        change_details = []
        
        # Check checksum changes
        current_checksum = current_node.get("checksum", {}).get("checksum")
        deferred_checksum = deferred_node.get("checksum", {}).get("checksum")
        
        if current_checksum != deferred_checksum:
            changes["checksum_changed"] = True
            changes["has_changes"] = True
            change_details.append("model code changed")
            changes["current_checksum"] = current_checksum
            changes["deferred_checksum"] = deferred_checksum
        
        # Check dependency changes
        current_deps = set(current_node.get("depends_on", {}).get("nodes", []))
        deferred_deps = set(deferred_node.get("depends_on", {}).get("nodes", []))
        
        if current_deps != deferred_deps:
            changes["dependencies_changed"] = True
            changes["has_changes"] = True
            change_details.append("dependencies modified")
            changes["added_dependencies"] = list(current_deps - deferred_deps)
            changes["removed_dependencies"] = list(deferred_deps - current_deps)
        
        # Check materialization changes
        current_materialization = current_node.get("config", {}).get("materialized")
        deferred_materialization = deferred_node.get("config", {}).get("materialized")
        
        if current_materialization != deferred_materialization:
            changes["materialization_changed"] = True
            changes["has_changes"] = True
            change_details.append("materialization changed")
            changes["current_materialization"] = current_materialization
            changes["deferred_materialization"] = deferred_materialization
        
        if changes["has_changes"]:
            changes["change_type"] = "modified"
            changes["change_details"] = " and ".join(change_details)
        
        return changes

    def _analyze_dependency_changes(current_nodes: dict, deferred_nodes: dict) -> dict[str, Any]:
        """Analyze dependency relationship changes."""
        models_with_new_deps = []
        models_with_removed_deps = []
        total_changes = 0
        
        for unique_id in current_nodes:
            if unique_id in deferred_nodes:
                current_deps = set(current_nodes[unique_id].get("depends_on", {}).get("nodes", []))
                deferred_deps = set(deferred_nodes[unique_id].get("depends_on", {}).get("nodes", []))
                
                if current_deps != deferred_deps:
                    total_changes += 1
                    if current_deps - deferred_deps:  # New dependencies
                        models_with_new_deps.append(unique_id)
                    if deferred_deps - current_deps:  # Removed dependencies
                        models_with_removed_deps.append(unique_id)
        
        return {
            "total_dependency_changes": total_changes,
            "models_with_new_dependencies": models_with_new_deps,
            "models_with_removed_dependencies": models_with_removed_deps
        }

    def _analyze_metadata_changes(current_manifest: dict, deferred_manifest: dict) -> dict[str, Any]:
        """Analyze metadata changes between manifests."""
        current_metadata = current_manifest.get("metadata", {})
        deferred_metadata = deferred_manifest.get("metadata", {})
        
        return {
            "dbt_version_changed": current_metadata.get("dbt_version") != deferred_metadata.get("dbt_version"),
            "project_name_changed": current_metadata.get("project_name") != deferred_metadata.get("project_name"),
            "current_dbt_version": current_metadata.get("dbt_version"),
            "deferred_dbt_version": deferred_metadata.get("dbt_version")
        }


    return [
        ToolDefinition(
            description=get_prompt("admin_api/list_jobs"),
            fn=list_jobs,
            annotations=create_tool_annotations(
                title="List Jobs",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/get_job_details"),
            fn=get_job_details,
            annotations=create_tool_annotations(
                title="Get Job Details",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/trigger_job_run"),
            fn=trigger_job_run,
            annotations=create_tool_annotations(
                title="Trigger Job Run",
                read_only_hint=False,
                destructive_hint=False,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/list_jobs_runs"),
            fn=list_jobs_runs,
            annotations=create_tool_annotations(
                title="List Jobs Runs",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/get_job_run_details"),
            fn=get_job_run_details,
            annotations=create_tool_annotations(
                title="Get Job Run Details",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/cancel_job_run"),
            fn=cancel_job_run,
            annotations=create_tool_annotations(
                title="Cancel Job Run",
                read_only_hint=False,
                destructive_hint=False,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/retry_job_run"),
            fn=retry_job_run,
            annotations=create_tool_annotations(
                title="Retry Job Run",
                read_only_hint=False,
                destructive_hint=False,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/list_job_run_artifacts"),
            fn=list_job_run_artifacts,
            annotations=create_tool_annotations(
                title="List Job Run Artifacts",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/get_job_run_artifact"),
            fn=get_job_run_artifact,
            annotations=create_tool_annotations(
                title="Get Job Run Artifact",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/download_job_run_artifact"),
            fn=download_job_run_artifact,
            annotations=create_tool_annotations(
                title="Download Job Run Artifact",
                read_only_hint=False,
                destructive_hint=False,
                idempotent_hint=False,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/get_job_run_error"),
            fn=get_job_run_error,
            annotations=create_tool_annotations(
                title="Get Job Run Error",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/get_ci_manifest_and_deferred_manifest"),
            fn=get_ci_manifest_and_deferred_manifest,
            annotations=create_tool_annotations(
                title="Get CI Manifest and Deferred Manifest",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
        ToolDefinition(
            description=get_prompt("admin_api/compare_manifests"),
            fn=compare_manifests,
            annotations=create_tool_annotations(
                title="Compare Manifests",
                read_only_hint=True,
                destructive_hint=False,
                idempotent_hint=True,
            ),
        ),
    ]


def register_admin_api_tools(
    dbt_mcp: FastMCP,
    admin_config_provider: ConfigProvider[AdminApiConfig],
    exclude_tools: Sequence[ToolName] = [],
) -> None:
    """Register dbt Admin API tools."""
    admin_client = DbtAdminAPIClient(admin_config_provider)
    register_tools(
        dbt_mcp,
        create_admin_api_tool_definitions(admin_client, admin_config_provider),
        exclude_tools,
    )
