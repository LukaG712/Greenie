"""Trapper provider — delegates web scraping tasks to the Trapper service.

This provider sends scraping, crawling, and search tasks to a running Trapper
HTTP service.  It returns the scraped content as the agent's answer.

Configuration via environment variables:
    Trapper_URL     — Base URL of the Trapper service (default: http://localhost:3000)
    Trapper_API_KEY — Bearer token for authentication

The task format is detected automatically:
    - If the task starts with "SCRAPE:", it scrapes a single URL.
    - If the task starts with "CRAWL:", it starts a crawl job.
    - If the task starts with "SEARCH:", it performs a web search.
    - If the task starts with "MAP:", it discovers URLs on a site.
    - If the task starts with "BATCH:", it batch scrapes multiple URLs (comma-separated).
    - If the task starts with "INTERACT:", it interacts with a page (JSON payload).
    - If the task starts with "EXTRACT:", it extracts structured data (JSON payload).
    - If the task starts with "AGENT:", it runs a research agent (prompt).
    - Otherwise, the task is treated as a SCRAPE command.
"""

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import httpx

from hub.providers import AgentPermissions, AgentProvider, ProviderRunResult
from hub.retry import retry_sync, SCRAPING_RETRY

logger = logging.getLogger(__name__)


class TrapperProvider(AgentProvider):
    """Provider that delegates web scraping tasks to an Trapper HTTP service.

    Supports scraping single pages, crawling sites, searching the web, and
    mapping site URLs.  The task string is parsed to determine the operation.
    """

    @property
    def name(self) -> str:
        return "Trapper"

    def _get_base_url(self) -> str:
        return os.environ.get("Trapper_URL", "http://localhost:3000").rstrip("/")

    def _get_api_key(self) -> str:
        return os.environ.get("Trapper_API_KEY", "")

    def check_available(self) -> bool:
        """Check if the Trapper service is reachable."""
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(f"{self._get_base_url()}/health")
                return resp.status_code == 200
        except Exception as e:
            logger.warning(f"Trapper health check failed: {e}")
            return False

    def build_command(
        self,
        task: str,
        project_dir: str,
        permissions: AgentPermissions,
        extra_args: Optional[List[str]] = None,
    ) -> List[str]:
        """Build a placeholder command — execution happens via HTTP."""
        return ["Trapper", "scrape", "--task", task]

    def _parse_task(self, task: str) -> tuple[str, str]:
        """Parse the task string to determine the operation and target.

        Returns:
            Tuple of (operation, target) where operation is one of
            'scrape', 'crawl', 'search', 'map', 'batch', 'interact', 'extract', 'agent'.
        """
        task_upper = task.upper().strip()
        if task_upper.startswith("SCRAPE:"):
            return "scrape", task[len("SCRAPE:"):].strip()
        if task_upper.startswith("CRAWL:"):
            return "crawl", task[len("CRAWL:"):].strip()
        if task_upper.startswith("SEARCH:"):
            return "search", task[len("SEARCH:"):].strip()
        if task_upper.startswith("MAP:"):
            return "map", task[len("MAP:"):].strip()
        if task_upper.startswith("BATCH:"):
            return "batch", task[len("BATCH:"):].strip()
        if task_upper.startswith("INTERACT:"):
            return "interact", task[len("INTERACT:"):].strip()
        if task_upper.startswith("EXTRACT:"):
            return "extract", task[len("EXTRACT:"):].strip()
        if task_upper.startswith("AGENT:"):
            return "agent", task[len("AGENT:"):].strip()
        # Default: treat as scrape
        return "scrape", task

    def run(
        self,
        task: str,
        project_dir: str,
        permissions: AgentPermissions,
        timeout: int = 1800,
        extra_args: Optional[List[str]] = None,
    ) -> ProviderRunResult:
        """Execute a web scraping task via the Trapper HTTP service."""
        api_key = self._get_api_key()
        base_url = self._get_base_url()

        operation, target = self._parse_task(task)

        headers = {
            "Content-Type": "application/json",
        }
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        try:
            async def _execute() -> ProviderRunResult:
                async with httpx.AsyncClient(timeout=min(timeout, 120)) as client:
                    if operation == "scrape":
                        return await self._scrape(client, base_url, target, headers)
                    elif operation == "crawl":
                        return await self._crawl(client, base_url, target, headers, timeout)
                    elif operation == "search":
                        return await self._search(client, base_url, target, headers)
                    elif operation == "map":
                        return await self._map(client, base_url, target, headers)
                    elif operation == "batch":
                        return await self._batch(client, base_url, target, headers, timeout)
                    elif operation == "interact":
                        return await self._interact(client, base_url, target, headers)
                    elif operation == "extract":
                        return await self._extract(client, base_url, target, headers, timeout)
                    elif operation == "agent":
                        return await self._agent(client, base_url, target, headers, timeout)
                    else:
                        return ProviderRunResult(
                            exit_code=-1,
                            raw_output="",
                            error=f"Unknown operation: {operation}",
                        )

            try:
                loop = asyncio.get_running_loop()
                # Already in async context — run in a separate thread to avoid blocking
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, _execute())
                    return future.result()
            except RuntimeError:
                # No running event loop — run directly
                return asyncio.run(_execute())

        except httpx.ConnectError as e:
            return ProviderRunResult(
                exit_code=-1,
                raw_output="",
                error=f"Cannot connect to Trapper at {base_url}: {e}",
            )
        except httpx.TimeoutException as e:
            return ProviderRunResult(
                exit_code=-1,
                raw_output="",
                error=f"Trapper request timed out: {e}",
            )
        except Exception as e:
            return ProviderRunResult(
                exit_code=-1,
                raw_output="",
                error=f"Trapper execution failed: {e}",
            )

    async def _scrape(
        self, client: httpx.AsyncClient, base_url: str, url: str, headers: Dict[str, str]
    ) -> ProviderRunResult:
        """Scrape a single URL and return the content."""
        payload = {
            "url": url,
            "formats": ["markdown", "links"],
            "onlyMainContent": True,
        }

        resp = await client.post(
            f"{base_url}/scrape",
            json=payload,
            headers=headers,
        )

        if resp.status_code != 200:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=resp.text,
                error=f"Trapper scrape failed with HTTP {resp.status_code}: {resp.text}",
            )

        data = resp.json()
        if not data.get("success"):
            return ProviderRunResult(
                exit_code=-1,
                raw_output=str(data),
                error=data.get("error", "Scrape failed"),
            )

        result_data = data.get("data", {})
        markdown = result_data.get("markdown", "")
        links = result_data.get("links", [])
        metadata = result_data.get("metadata", {})

        # Build structured answer
        answer_parts = [markdown]
        if links:
            answer_parts.append(f"\n\n## Links found ({len(links)})")
            for link in links[:20]:  # Limit to 20 links
                answer_parts.append(f"- {link}")
        if metadata:
            title = metadata.get("title", "")
            description = metadata.get("description", "")
            if title or description:
                answer_parts.insert(0, f"# {title}\n")
                if description:
                    answer_parts.insert(1, f"> {description}\n\n")

        answer = "\n".join(answer_parts)
        return ProviderRunResult(
            exit_code=0,
            raw_output=answer,
            answer=answer,
        )

    async def _crawl(
        self, client: httpx.AsyncClient, base_url: str, url: str, headers: Dict[str, str], timeout: int
    ) -> ProviderRunResult:
        """Start a crawl job and poll for results."""
        payload = {
            "url": url,
            "maxDiscoveryDepth": 3,
            "limit": 20,
            "scrapeOptions": {
                "formats": ["markdown"],
                "onlyMainContent": True,
            },
        }

        resp = await client.post(
            f"{base_url}/crawl",
            json=payload,
            headers=headers,
        )

        if resp.status_code != 201:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=resp.text,
                error=f"Trapper crawl failed with HTTP {resp.status_code}: {resp.text}",
            )

        data = resp.json()
        job_id = data.get("jobId")
        if not job_id:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=str(data),
                error="No job ID in crawl response",
            )

        # Poll for results
        poll_interval = 2
        max_polls = timeout // poll_interval
        for _ in range(max_polls):
            await asyncio.sleep(poll_interval)
            status_resp = await client.get(
                f"{base_url}/crawl/{job_id}",
                headers=headers,
            )

            if status_resp.status_code != 200:
                continue

            status_data = status_resp.json()
            status = status_data.get("status", "")

            if status == "completed":
                pages = status_data.get("data", [])
                answer_parts = [f"# Crawl Results for {url}\n"]
                answer_parts.append(f"**Total pages scraped:** {len(pages)}\n")

                for i, page in enumerate(pages[:10], 1):  # Limit to 10 pages
                    page_url = page.get("url", "")
                    page_data = page.get("data", {})
                    markdown = page_data.get("markdown", "")
                    answer_parts.append(f"## Page {i}: {page_url}")
                    if markdown:
                        answer_parts.append(markdown[:2000])  # Limit per page
                    answer_parts.append("")

                answer = "\n".join(answer_parts)
                return ProviderRunResult(
                    exit_code=0,
                    raw_output=answer,
                    answer=answer,
                )

            if status == "failed":
                errors = status_data.get("errors", [])
                return ProviderRunResult(
                    exit_code=-1,
                    raw_output=str(errors),
                    error=f"Crawl failed: {errors}",
                )

        return ProviderRunResult(
            exit_code=-1,
            raw_output="",
            error="Crawl job timed out",
        )

    async def _search(
        self, client: httpx.AsyncClient, base_url: str, query: str, headers: Dict[str, str]
    ) -> ProviderRunResult:
        """Search the web and return results."""
        payload = {
            "query": query,
            "limit": 5,
            "scrapeOptions": {
                "formats": ["markdown"],
                "onlyMainContent": True,
            },
        }

        resp = await client.post(
            f"{base_url}/search",
            json=payload,
            headers=headers,
        )

        if resp.status_code != 200:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=resp.text,
                error=f"Trapper search failed with HTTP {resp.status_code}: {resp.text}",
            )

        data = resp.json()
        if not data.get("success"):
            return ProviderRunResult(
                exit_code=-1,
                raw_output=str(data),
                error=data.get("error", "Search failed"),
            )

        results = data.get("data", [])
        answer_parts = [f"# Search Results for: {query}\n"]
        answer_parts.append(f"**Found {len(results)} results**\n")

        for i, result in enumerate(results, 1):
            url = result.get("url", "")
            title = result.get("title", "")
            description = result.get("description", "")
            answer_parts.append(f"## {i}. {title}")
            answer_parts.append(f"**URL:** {url}")
            if description:
                answer_parts.append(f"**Description:** {description}")
            page_data = result.get("data", {})
            markdown = page_data.get("markdown", "")
            if markdown:
                answer_parts.append(markdown[:1500])  # Limit per result
            answer_parts.append("")

        answer = "\n".join(answer_parts)
        return ProviderRunResult(
            exit_code=0,
            raw_output=answer,
            answer=answer,
        )

    async def _map(
        self, client: httpx.AsyncClient, base_url: str, url: str, headers: Dict[str, str]
    ) -> ProviderRunResult:
        """Discover URLs on a site."""
        payload = {
            "url": url,
            "ignoreSitemap": False,
            "limit": 100,
        }

        resp = await client.post(
            f"{base_url}/map",
            json=payload,
            headers=headers,
        )

        if resp.status_code != 200:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=resp.text,
                error=f"Trapper map failed with HTTP {resp.status_code}: {resp.text}",
            )

        data = resp.json()
        if not data.get("success"):
            return ProviderRunResult(
                exit_code=-1,
                raw_output=str(data),
                error=data.get("error", "Map failed"),
            )

        links = data.get("links", [])
        answer_parts = [f"# Site Map for {url}\n"]
        answer_parts.append(f"**Found {len(links)} URLs**\n")

        for link_info in links:
            link_url = link_info.get("url", "") if isinstance(link_info, dict) else link_info
            answer_parts.append(f"- {link_url}")

        answer = "\n".join(answer_parts)
        return ProviderRunResult(
            exit_code=0,
            raw_output=answer,
            answer=answer,
        )

    async def _batch(
        self, client: httpx.AsyncClient, base_url: str, target: str, headers: Dict[str, str], timeout: int
    ) -> ProviderRunResult:
        """Batch scrape multiple URLs and poll for results.

        Target can be a comma-separated list of URLs or a JSON array.
        """
        # Parse URLs from target
        urls: List[str] = []
        try:
            parsed = json.loads(target)
            if isinstance(parsed, list):
                urls = [str(u).strip() for u in parsed]
            else:
                urls = [u.strip() for u in target.split(",")]
        except json.JSONDecodeError:
            urls = [u.strip() for u in target.split(",")]

        if not urls:
            return ProviderRunResult(
                exit_code=-1,
                raw_output="",
                error="No URLs provided for batch scrape",
            )

        payload = {
            "urls": urls,
            "scrapeOptions": {
                "formats": ["markdown"],
                "onlyMainContent": True,
            },
        }

        resp = await client.post(
            f"{base_url}/batch_scrape",
            json=payload,
            headers=headers,
        )

        if resp.status_code != 201:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=resp.text,
                error=f"Trapper batch failed with HTTP {resp.status_code}: {resp.text}",
            )

        data = resp.json()
        if not data.get("success"):
            return ProviderRunResult(
                exit_code=-1,
                raw_output=str(data),
                error=data.get("error", "Batch scrape failed"),
            )

        job_id = data.get("jobId", "")
        if not job_id:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=str(data),
                error="No job ID returned",
            )

        # Poll for results
        poll_start = time.time()
        while time.time() - poll_start < timeout:
            await asyncio.sleep(2)
            status_resp = await client.get(
                f"{base_url}/batch_scrape/{job_id}",
                headers=headers,
            )
            if status_resp.status_code != 200:
                break

            status_data = status_resp.json()
            status = status_data.get("status", "")
            if status in ("completed", "failed"):
                results = status_data.get("data", [])
                errors = status_data.get("errors", [])
                answer_parts = ["# Batch Scrape Results\n"]
                answer_parts.append(f"**Status:** {status}")
                answer_parts.append(f"**Processed:** {len(results)} pages\n")

                for i, result in enumerate(results, 1):
                    url = result.get("url", "")
                    page_data = result.get("data", {})
                    markdown = page_data.get("markdown", "")
                    answer_parts.append(f"## {i}. {url}")
                    if markdown:
                        answer_parts.append(markdown[:2000])
                    answer_parts.append("")

                if errors:
                    answer_parts.append(f"\n## Errors ({len(errors)})")
                    for err in errors[:10]:
                        answer_parts.append(f"- {err}")

                answer = "\n".join(answer_parts)
                return ProviderRunResult(
                    exit_code=0 if status == "completed" else -1,
                    raw_output=answer,
                    answer=answer,
                )

        return ProviderRunResult(
            exit_code=-1,
            raw_output="",
            error="Batch scrape timed out",
        )

    async def _interact(
        self, client: httpx.AsyncClient, base_url: str, target: str, headers: Dict[str, str]
    ) -> ProviderRunResult:
        """Interact with a page in a live browser session.

        Target should be a JSON object with scrapeId and actions/prompt/code.
        """
        payload: Dict[str, Any]
        try:
            payload = json.loads(target)
        except json.JSONDecodeError:
            return ProviderRunResult(
                exit_code=-1,
                raw_output="",
                error=f"Invalid JSON for interact: {target}",
            )

        resp = await client.post(
            f"{base_url}/interact",
            json=payload,
            headers=headers,
        )

        if resp.status_code != 200:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=resp.text,
                error=f"Trapper interact failed with HTTP {resp.status_code}: {resp.text}",
            )

        data = resp.json()
        if not data.get("success"):
            return ProviderRunResult(
                exit_code=-1,
                raw_output=str(data),
                error=data.get("error", "Interact failed"),
            )

        result_data = data.get("data", {})
        markdown = result_data.get("markdown", "")
        scrape_id = data.get("scrapeId", "")

        answer_parts = []
        if scrape_id:
            answer_parts.append(f"Session ID: {scrape_id}\n")
        if markdown:
            answer_parts.append(markdown)

        answer = "\n".join(answer_parts) if answer_parts else "Interaction completed."
        return ProviderRunResult(
            exit_code=0,
            raw_output=answer,
            answer=answer,
        )

    async def _extract(
        self, client: httpx.AsyncClient, base_url: str, target: str, headers: Dict[str, str], timeout: int
    ) -> ProviderRunResult:
        """Extract structured data from multiple URLs.

        Target should be a JSON object with urls, prompt, and optional schema.
        """
        payload: Dict[str, Any]
        try:
            payload = json.loads(target)
        except json.JSONDecodeError:
            return ProviderRunResult(
                exit_code=-1,
                raw_output="",
                error=f"Invalid JSON for extract: {target}",
            )

        resp = await client.post(
            f"{base_url}/extract",
            json=payload,
            headers=headers,
        )

        if resp.status_code != 201:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=resp.text,
                error=f"Trapper extract failed with HTTP {resp.status_code}: {resp.text}",
            )

        data = resp.json()
        if not data.get("success"):
            return ProviderRunResult(
                exit_code=-1,
                raw_output=str(data),
                error=data.get("error", "Extract failed"),
            )

        job_id = data.get("jobId", "")
        if not job_id:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=str(data),
                error="No job ID returned",
            )

        # Poll for results
        poll_start = time.time()
        while time.time() - poll_start < timeout:
            await asyncio.sleep(2)
            status_resp = await client.get(
                f"{base_url}/extract/{job_id}",
                headers=headers,
            )
            if status_resp.status_code != 200:
                break

            status_data = status_resp.json()
            status = status_data.get("status", "")
            if status in ("completed", "failed"):
                results = status_data.get("data", [])
                answer_parts = ["# Extract Results\n"]
                answer_parts.append(f"**Status:** {status}\n")

                for result in results:
                    url = result.get("url", "")
                    extracted = result.get("data", {})
                    answer_parts.append(f"## {url}")
                    answer_parts.append(f"```json\n{json.dumps(extracted, indent=2)}\n```")
                    answer_parts.append("")

                answer = "\n".join(answer_parts)
                return ProviderRunResult(
                    exit_code=0 if status == "completed" else -1,
                    raw_output=answer,
                    answer=answer,
                )

        return ProviderRunResult(
            exit_code=-1,
            raw_output="",
            error="Extract job timed out",
        )

    async def _agent(
        self, client: httpx.AsyncClient, base_url: str, target: str, headers: Dict[str, str], timeout: int
    ) -> ProviderRunResult:
        """Run a prompt-driven research agent.

        Target can be a plain prompt or a JSON object with prompt, search, urls, schema.
        """
        payload: Dict[str, Any]
        try:
            payload = json.loads(target)
        except json.JSONDecodeError:
            payload = {"prompt": target}

        resp = await client.post(
            f"{base_url}/agent",
            json=payload,
            headers=headers,
        )

        if resp.status_code != 201:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=resp.text,
                error=f"Trapper agent failed with HTTP {resp.status_code}: {resp.text}",
            )

        data = resp.json()
        if not data.get("success"):
            return ProviderRunResult(
                exit_code=-1,
                raw_output=str(data),
                error=data.get("error", "Agent failed"),
            )

        job_id = data.get("jobId", "")
        if not job_id:
            return ProviderRunResult(
                exit_code=-1,
                raw_output=str(data),
                error="No job ID returned",
            )

        # Poll for results
        poll_start = time.time()
        while time.time() - poll_start < timeout:
            await asyncio.sleep(2)
            status_resp = await client.get(
                f"{base_url}/agent/{job_id}",
                headers=headers,
            )
            if status_resp.status_code != 200:
                break

            status_data = status_resp.json()
            status = status_data.get("status", "")
            if status in ("completed", "failed"):
                result_data = status_data.get("data", {})
                answer_parts = ["# Agent Research Results\n"]
                answer_parts.append(f"**Status:** {status}\n")

                if isinstance(result_data, dict):
                    answer_parts.append(f"```json\n{json.dumps(result_data, indent=2)}\n```")
                elif isinstance(result_data, str):
                    answer_parts.append(result_data)
                else:
                    answer_parts.append(str(result_data))

                answer = "\n".join(answer_parts)
                return ProviderRunResult(
                    exit_code=0 if status == "completed" else -1,
                    raw_output=answer,
                    answer=answer,
                )

        return ProviderRunResult(
            exit_code=-1,
            raw_output="",
            error="Agent job timed out",
        )


# Auto-register the provider
def _register() -> None:
    from hub.providers import register_provider
    register_provider(TrapperProvider())


_register()
