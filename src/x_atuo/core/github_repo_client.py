"""GitHub repository metadata fetch helpers for deterministic repo-post flows."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


class GitHubRepoError(RuntimeError):
    """Raised when GitHub repo metadata or README cannot be fetched."""


@dataclass(slots=True, frozen=True)
class GitHubRepoContext:
    repo_url: str
    repo_name: str
    description: str | None = None
    readme_excerpt: str | None = None
    stars: int | None = None
    metadata: dict | None = None


def parse_repo_url(repo_url: str) -> tuple[str, str]:
    parsed = urlparse(repo_url)
    if parsed.netloc not in {"github.com", "www.github.com"}:
        raise GitHubRepoError(f"Unsupported repo host in URL: {repo_url}")
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        raise GitHubRepoError(f"Invalid GitHub repo URL: {repo_url}")
    return parts[0], parts[1]


def _read_json(url: str) -> dict:
    request = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "x-atuo/0.1",
        },
    )
    try:
        with urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError) as exc:
        raise GitHubRepoError(f"Failed to fetch {url}: {exc}") from exc


def _read_text(url: str) -> str | None:
    request = Request(url, headers={"User-Agent": "x-atuo/0.1"})
    try:
        with urlopen(request, timeout=20) as response:
            return response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        if exc.code == 404:
            return None
        raise GitHubRepoError(f"Failed to fetch {url}: {exc}") from exc
    except (URLError, TimeoutError) as exc:
        raise GitHubRepoError(f"Failed to fetch {url}: {exc}") from exc


def _extract_html_description(html: str | None) -> str | None:
    if not html:
        return None
    patterns = [
        r'<meta\s+name="description"\s+content="([^"]+)"',
        r'<meta\s+property="og:description"\s+content="([^"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def _clean_description(text: str | None, repo_name: str) -> str | None:
    if not text:
        return None
    cleaned = text.strip()
    prefix = f"GitHub - {repo_name}:"
    if cleaned.startswith(prefix):
        cleaned = cleaned[len(prefix) :].strip()
    return cleaned


def _excerpt_markdown(markdown: str | None, *, max_length: int = 160) -> str | None:
    if not markdown:
        return None
    lines = [line.strip() for line in markdown.splitlines() if line.strip()]
    cleaned: list[str] = []
    for line in lines:
        if line.startswith("#") or line.startswith("![") or line.startswith("[!["):
            continue
        cleaned.append(line)
        if len(" ".join(cleaned)) >= max_length:
            break
    if not cleaned:
        return None
    excerpt = " ".join(cleaned)
    return excerpt[:max_length].rstrip()


def fetch_repo_context(repo_url: str) -> GitHubRepoContext:
    owner, repo = parse_repo_url(repo_url)
    repo_api = f"https://api.github.com/repos/{owner}/{repo}"
    metadata: dict = {}
    default_branch = "main"
    description: str | None = None
    stars: int | None = None
    try:
        metadata = _read_json(repo_api)
        default_branch = metadata.get("default_branch") or "main"
        description = metadata.get("description")
        stars = int(metadata["stargazers_count"]) if metadata.get("stargazers_count") is not None else None
    except GitHubRepoError:
        page_html = _read_text(repo_url)
        description = _clean_description(_extract_html_description(page_html), f"{owner}/{repo}")

    readme = None
    for branch in (default_branch, "main", "master"):
        if not branch:
            continue
        readme_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/README.md"
        readme = _read_text(readme_url)
        if readme:
            break

    return GitHubRepoContext(
        repo_url=repo_url,
        repo_name=f"{owner}/{repo}",
        description=description,
        readme_excerpt=_excerpt_markdown(readme),
        stars=stars,
        metadata=metadata,
    )


def render_repo_post_text(context: GitHubRepoContext, *, max_length: int = 280) -> str:
    base = f"{context.repo_name} is an open-source project"
    if context.description:
        base += f". {context.description.rstrip('.') }."
    elif context.readme_excerpt:
        base += f". {context.readme_excerpt.rstrip('.') }."
    if context.stars:
        base += f" {context.stars:,} GitHub stars."
    base += f"\n\nLink: {context.repo_url}\n#OpenSource #GitHub #DevTools"
    if len(base) <= max_length:
        return base
    trimmed = base[: max_length - 1].rstrip()
    return trimmed + "…"
