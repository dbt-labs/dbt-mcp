"""Client for fetching, caching, and searching docs.getdbt.com content.

Provides ``ProductDocsClient``, which handles HTTP requests, in-memory
caching, and keyword search/ranking over the llms.txt index and
llms-full.txt full-text corpus.

Caches live for the lifetime of the MCP server process; restart the
server to refresh.
"""

import logging
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)

LLMS_TXT_URL = "https://docs.getdbt.com/llms.txt"
LLMS_FULL_TXT_URL = "https://docs.getdbt.com/llms-full.txt"
DOCS_BASE_URL = "https://docs.getdbt.com"

# Cap page content length in tool responses to avoid Cursor IDE freezing.
MAX_CONTENT_CHARS_PER_PAGE = 28_000

_STOP_WORDS = frozenset(
    {
        "a",
        "an",
        "the",
        "in",
        "on",
        "of",
        "for",
        "to",
        "and",
        "or",
        "is",
        "it",
        "how",
        "do",
        "i",
        "my",
        "what",
        "with",
        "from",
        "can",
        "does",
        "about",
        "using",
        "use",
        "set",
        "up",
        "dbt",
    }
)

_ABBREVIATION_EXPANSIONS: dict[str, list[str]] = {
    "udf": ["user-defined function", "user-defined functions"],
    "udfs": ["user-defined function", "user-defined functions"],
    "ci": ["continuous integration"],
    "cd": ["continuous deployment"],
    "ci/cd": ["continuous integration", "continuous deployment"],
    "sl": ["semantic layer"],
    "ide": ["studio ide", "ide"],
    "cli": [
        "command line",
        "dbt cli",
        "dbt platform cli",
        "fusion cli",
        "dbt core cli",
    ],
    "vc": ["version control"],
    "pr": ["pull request"],
    "mr": ["merge request"],
    "prs": ["pull requests"],
    "env": ["environment"],
    "envs": ["environments"],
    "sso": ["single sign-on"],
    "oauth": ["open authentication"],
    "repo": ["repository"],
    "gh": ["github"],
    "aws": ["amazon web services"],
    "gcp": ["google cloud platform"],
}

_LLMS_TXT_ENTRY_RE = re.compile(r"^-\s+\[([^\]]+)\]\(([^)]+)\)(?::\s*(.+))?$")

# Relevance scoring weights for search_index ranking.
# Higher weights push results toward the top when terms appear in
# more-specific fields (title > description > section).
SCORE_KEYWORD_IN_TITLE = 10
SCORE_KEYWORD_IN_DESCRIPTION = 5
SCORE_KEYWORD_IN_SECTION = 1
SCORE_BIGRAM_IN_TITLE = 20
SCORE_BIGRAM_IN_DESCRIPTION = 12
SCORE_EXACT_QUERY_IN_TITLE = 40
SCORE_EXACT_QUERY_IN_DESCRIPTION = 25
SCORE_ALL_KEYWORDS_MATCHED = 15
SCORE_EXACT_TITLE_MATCH = 50
SCORE_TITLE_FOCUS_MAX = 15  # scaled by len(query) / len(title)


# ---------------------------------------------------------------------------
# Pure helper functions (no state needed)
# ---------------------------------------------------------------------------


def truncate_content(content: str, max_chars: int, url: str) -> str:
    """Truncate page content to *max_chars* and append a note with the URL."""
    if len(content) <= max_chars:
        return content
    return (
        content[:max_chars].rstrip()
        + f"\n\n---\n*Content truncated for length. Full page: {url}*"
    )


def display_url(url: str) -> str:
    """Convert an internal ``.md`` URL to a browser-friendly URL."""
    if url.endswith(".md"):
        return url[:-3]
    return url


def normalize_doc_url(path: str) -> str:
    """Turn a path or URL into a full ``docs.getdbt.com`` ``.md`` URL.

    Raises ``ValueError`` if the resulting URL is not on docs.getdbt.com.
    """
    url = path.strip()

    if not url.startswith("http"):
        url = url.lstrip("/")
        url = f"{DOCS_BASE_URL}/{url}"

    url = url.rstrip("/")

    if not url.endswith(".md"):
        url = f"{url}.md"

    if not url.startswith(f"{DOCS_BASE_URL}/"):
        raise ValueError(f"URL must be on docs.getdbt.com, got: {url}")

    return url


def parse_llms_full_txt(text: str) -> list[dict[str, str]]:
    """Parse ``llms-full.txt`` into a list of page entries with full content."""
    pages: list[dict[str, str]] = []
    url_pattern = re.compile(r"https://docs\.getdbt\.com/[^\s\]\)]+")

    chunks = re.split(r"\n---\n", text)

    for chunk in chunks:
        lines = chunk.strip().splitlines()
        if not lines:
            continue

        title = ""
        url = ""
        for line in lines[:10]:
            stripped = line.strip()
            if stripped.startswith("### ") and not title:
                title = stripped.removeprefix("### ").strip()
            if not url:
                url_match = url_pattern.search(stripped)
                if url_match:
                    url = url_match.group(0)
            if title and url:
                break

        if not url:
            continue

        content_lower = chunk.lower()
        pages.append({"url": url, "title": title, "content_lower": content_lower})

    return pages


def parse_llms_txt(text: str) -> list[dict[str, str]]:
    """Parse ``llms.txt`` markdown into a list of page entries."""
    entries: list[dict[str, str]] = []
    current_section = ""

    for line in text.splitlines():
        stripped = line.strip()

        if stripped.startswith("## "):
            current_section = stripped.removeprefix("## ").strip()
            continue

        match = _LLMS_TXT_ENTRY_RE.match(stripped)
        if not match:
            continue

        title = match.group(1).strip()
        url = match.group(2).strip()
        description = (match.group(3) or "").strip()

        entries.append(
            {
                "title": title,
                "url": url,
                "description": description,
                "section": current_section,
                "title_lower": title.lower(),
                "description_lower": description.lower(),
                "section_lower": current_section.lower(),
            }
        )

    return entries


def score_index_entry(
    entry: dict[str, str],
    keywords: list[str],
    bigrams: list[str],
    query_lower: str,
) -> float | None:
    """Score a single llms.txt index entry against the search terms.

    Returns the relevance score, or ``None`` if no keyword matched at all.

    The scoring heuristic ranks entries by *where* a match occurs
    (title > description > section) and gives bonuses for bigram
    matches, full-query substring matches, and exact title matches.
    """
    title_lower = entry["title_lower"]
    desc_lower = entry["description_lower"]
    section_lower = entry["section_lower"]
    searchable = f"{title_lower} {desc_lower} {section_lower}"

    matching = [kw for kw in keywords if kw in searchable]
    if not matching:
        return None

    score: float = 0

    for kw in matching:
        if kw in title_lower:
            score += SCORE_KEYWORD_IN_TITLE
        if kw in desc_lower:
            score += SCORE_KEYWORD_IN_DESCRIPTION
        if kw in section_lower:
            score += SCORE_KEYWORD_IN_SECTION

    for bigram in bigrams:
        if bigram in title_lower:
            score += SCORE_BIGRAM_IN_TITLE
        if bigram in desc_lower:
            score += SCORE_BIGRAM_IN_DESCRIPTION

    if query_lower in title_lower:
        score += SCORE_EXACT_QUERY_IN_TITLE
    if query_lower in desc_lower:
        score += SCORE_EXACT_QUERY_IN_DESCRIPTION

    if len(matching) == len(keywords):
        score += SCORE_ALL_KEYWORDS_MATCHED

    if query_lower == title_lower:
        score += SCORE_EXACT_TITLE_MATCH

    if title_lower and query_lower in title_lower:
        focus_ratio = len(query_lower) / len(title_lower)
        score += focus_ratio * SCORE_TITLE_FOCUS_MAX

    return score


def expand_keywords(query: str) -> list[str]:
    """Extract keywords from *query*, filtering stop words and expanding abbreviations."""
    all_words = query.lower().split()
    if not all_words:
        return []

    keywords = [w for w in all_words if w not in _STOP_WORDS]
    if not keywords:
        keywords = list(all_words)

    expanded: list[str] = []
    for kw in keywords:
        if kw in _ABBREVIATION_EXPANSIONS:
            expanded.extend(
                term for exp in _ABBREVIATION_EXPANSIONS[kw] for term in exp.split()
            )
    for term in expanded:
        if term not in keywords and term not in _STOP_WORDS:
            keywords.append(term)

    return keywords


# ---------------------------------------------------------------------------
# Client class
# ---------------------------------------------------------------------------


class ProductDocsClient:
    """Async client for fetching and searching docs.getdbt.com content.

    Caches are simple dicts that live for the lifetime of the instance
    (and thus the MCP server process).  Restart the server to refresh.
    """

    def __init__(self) -> None:
        self._cache: dict[str, Any] = {}

    # -- fetchers ------------------------------------------------------------

    async def get_index(self) -> list[dict[str, str]]:
        """Return the cached llms.txt index, fetching on first call."""
        if "index" not in self._cache:
            logger.info("Fetching llms.txt index from %s", LLMS_TXT_URL)
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(LLMS_TXT_URL)
                response.raise_for_status()
            self._cache["index"] = parse_llms_txt(response.text)
            logger.info("Cached llms.txt index: %d pages", len(self._cache["index"]))
        return self._cache["index"]

    async def get_full_text_index(self) -> list[dict[str, str]]:
        """Return the cached llms-full.txt page index, fetching on first call."""
        if "full_text" not in self._cache:
            logger.info("Fetching llms-full.txt from %s", LLMS_FULL_TXT_URL)
            async with httpx.AsyncClient(
                timeout=120.0, follow_redirects=True
            ) as client:
                response = await client.get(LLMS_FULL_TXT_URL)
                response.raise_for_status()
            self._cache["full_text"] = parse_llms_full_txt(response.text)
            logger.info("Cached llms-full.txt: %d pages", len(self._cache["full_text"]))
        return self._cache["full_text"]

    async def get_page(self, url: str) -> str:
        """Fetch a page with caching.

        Returns the page content as a string on success.
        Raises httpx.HTTPStatusError on 4xx/5xx responses.
        Raises httpx.RequestError on network/connection failures.
        """
        if url not in self._cache:
            logger.info("Fetching product doc page: %s", url)
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(url)
                response.raise_for_status()
            self._cache[url] = response.text
        return self._cache[url]

    # -- search --------------------------------------------------------------

    async def search_index(self, query: str) -> list[dict[str, str]]:
        """Keyword search over the llms.txt index with relevance ranking."""
        index = await self.get_index()
        query_lower = query.lower()
        all_words = query_lower.split()

        if not all_words:
            return []

        keywords = expand_keywords(query)

        bigrams = []
        for i in range(len(all_words) - 1):
            bigram = f"{all_words[i]} {all_words[i + 1]}"
            if not all(w in _STOP_WORDS for w in [all_words[i], all_words[i + 1]]):
                bigrams.append(bigram)

        scored: list[tuple[float, dict[str, str]]] = []

        for entry in index:
            score = score_index_entry(entry, keywords, bigrams, query_lower)
            if score is not None:
                scored.append((score, entry))

        scored.sort(key=lambda x: x[0], reverse=True)

        results = []
        for _, entry in scored[:10]:
            result: dict[str, str] = {
                "title": entry["title"],
                "url": display_url(entry["url"]),
            }
            if entry["description"]:
                result["description"] = entry["description"]
            if entry["section"]:
                result["section"] = entry["section"]
            results.append(result)

        return results

    async def search_full_text(
        self, keywords: list[str], max_results: int = 10
    ) -> list[dict[str, str]]:
        """Search llms-full.txt page content for keyword matches (OR logic)."""
        pages = await self.get_full_text_index()
        scored: list[tuple[int, dict[str, str]]] = []

        keywords_lower = [kw.lower() for kw in keywords]

        for page in pages:
            content = page["content_lower"]
            hit_count = 0
            keywords_matched = 0
            for kw in keywords_lower:
                count = content.count(kw)
                if count > 0:
                    keywords_matched += 1
                    hit_count += count

            if keywords_matched == 0:
                continue

            score = hit_count + (keywords_matched * 50)
            scored.append((score, page))

        scored.sort(key=lambda x: x[0], reverse=True)

        return [
            {"url": display_url(entry["url"]), "title": entry["title"]}
            for _, entry in scored[:max_results]
        ]
