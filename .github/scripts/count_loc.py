import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path

import requests
from upstash_redis import Redis

ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
USER_NAME = os.environ.get("USER_NAME", "rexdotsh")
OWNER_ID = {"id": "MDQ6VXNlcjY1OTQyNzUz"}
UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

MANDATORY_ENV_VARS = {
    "ACCESS_TOKEN": ACCESS_TOKEN,
    "UPSTASH_REDIS_REST_URL": UPSTASH_URL,
    "UPSTASH_REDIS_REST_TOKEN": UPSTASH_TOKEN,
}
missing_vars = [name for name, value in MANDATORY_ENV_VARS.items() if not value]
if missing_vars:
    for var_name in missing_vars:
        print(f"Error: Mandatory environment variable {var_name} is not set.")
    sys.exit(1)

# Determine script's directory and construct paths
SCRIPT_DIR = Path(__file__).parent.resolve()
CACHE_PATH = SCRIPT_DIR / "cache.txt"
README_PATH = SCRIPT_DIR.parent.parent / "README.md"  # ../../README.md

HEADERS = {"authorization": f"token {ACCESS_TOKEN}"}
AFFILIATIONS = ["OWNER", "COLLABORATOR", "ORGANIZATION_MEMBER"]
CACHE_FILENAME = "cache.txt"
FORCE_CACHE_REFRESH = False
REQUEST_TIMEOUT = 10

redis_client = None
try:
    redis_client = Redis(url=UPSTASH_URL, token=UPSTASH_TOKEN)
except Exception as init_err:
    print(f"Error initializing Upstash Redis client: {init_err}")
    redis_client = None


def _graphql_request(query, variables):
    try:
        response = requests.post(
            "https://api.github.com/graphql",
            json={"query": query, "variables": variables},
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()  # Raise HTTPError for bad status codes (4xx or 5xx)
        data = response.json()
        if "errors" in data:
            # Check for rate limit specifically
            if any("RATE_LIMITED" in error.get("type", "") for error in data["errors"]):
                print("Warning: GraphQL query rate limited.")
                return None  # Signal rate limit
            # Raise other GraphQL errors
            raise Exception(f"GraphQL Error: {data['errors']}")
        return data.get("data", {})
    except requests.exceptions.HTTPError as http_err:
        status_code = http_err.response.status_code
        if status_code == 401:
            print("Fatal: HTTP 401 Unauthorized. Check ACCESS_TOKEN.")
        elif status_code == 403:
            print(
                "Warning: HTTP 403 Forbidden. Check token permissions or rate limits.",
            )
        elif status_code >= 500:
            print(
                f"Warning: HTTP {status_code} Server Error from GitHub API.",
            )
        else:  # Other 4xx errors
            print(f"HTTP Error {status_code}: {http_err}")
        # For HTTP errors that might be recoverable (like 403/5xx), return None
        if status_code == 403 or status_code >= 500:
            return None
        raise
    except requests.exceptions.RequestException as req_err:
        print(f"Network Error: {req_err}")
        raise


def _fetch_commit_loc(
    owner, repo_name, author_id, cursor=None, adds=0, dels=0, commits=0
):
    query = """
    query ($repo: String!, $owner: String!, $cursor: String, $author: ID!) {
        repository(name: $repo, owner: $owner) {
            defaultBranchRef { target { ... on Commit {
                history(first: 100, after: $cursor, author: {id: $author}) {
                    pageInfo { endCursor hasNextPage }
                    nodes { additions deletions }
                }
            }}}
        }
    }"""
    variables = {
        "repo": repo_name,
        "owner": owner,
        "cursor": cursor,
        "author": author_id,
    }
    data = _graphql_request(query, variables)

    if data is None:  # API error or rate limited
        print(
            f"Warning: Skipping further history fetch for {owner}/{repo_name} due to API issue.",
        )
        return adds, dels, commits

    try:
        history = (
            data.get("repository", {})
            .get("defaultBranchRef", {})
            .get("target", {})
            .get("history", {})
        )
        nodes = history.get("nodes", [])
        for commit in nodes:
            adds += commit.get("additions", 0)
            dels += commit.get("deletions", 0)
            commits += 1

        page_info = history.get("pageInfo", {})
        if page_info.get("hasNextPage") and page_info.get("endCursor"):
            return _fetch_commit_loc(
                owner, repo_name, author_id, page_info["endCursor"], adds, dels, commits
            )
        else:
            return adds, dels, commits
    except Exception as e:
        print(
            f"Error processing history for {owner}/{repo_name}: {e}. Returning partial counts.",
        )
        return adds, dels, commits


def _fetch_repos(owner_affiliation, author_id, cursor=None, edges=None):
    """Fetches all repository edges for the user using pagination."""
    if edges is None:
        edges = []
    query = """
    query ($aff: [RepositoryAffiliation], $login: String!, $cursor: String, $author: ID!) {
        user(login: $login) {
            repositories(first: 100, after: $cursor, ownerAffiliations: $aff, orderBy: {field: NAME, direction: ASC}) {
                pageInfo { endCursor hasNextPage }
                edges { node { ... on Repository {
                    nameWithOwner
                    defaultBranchRef { target { ... on Commit {
                        history(first: 1, author: {id: $author}) { totalCount }
                    }}}
                }}}
            }
        }
    }"""
    variables = {
        "aff": owner_affiliation,
        "login": USER_NAME,
        "cursor": cursor,
        "author": author_id,
    }
    print(f"Fetching repository page... (Cursor: {cursor})")
    data = _graphql_request(query, variables)

    if data is None:  # API error or rate limit
        print(
            "Warning: Failed to fetch a repository page. Results may be incomplete.",
        )
        return edges  # Return whatever was fetched so far

    repo_data = data.get("user", {}).get("repositories", {})
    edges.extend(repo_data.get("edges", []))
    page_info = repo_data.get("pageInfo", {})

    if page_info.get("hasNextPage") and page_info.get("endCursor"):
        return _fetch_repos(owner_affiliation, author_id, page_info["endCursor"], edges)
    else:
        print(f"Fetched all {len(edges)} repositories.")
        return edges


def calculate_loc_stats(affiliations, author_id_dict):
    """Main function to calculate LOC, handling cache."""
    author_id = author_id_dict["id"]
    cache_needs_update = False
    cached_data = {}  # { repo_hash: [user_commit_count, additions, deletions] }
    final_cache_data = {}  # { repo_hash: [user_commit_count, additions, deletions] }
    total_add = 0
    total_del = 0

    # Fetch current repository list and user commit counts
    repo_edges = _fetch_repos(affiliations, author_id)
    current_repos = {}  # {repo_hash: (name_with_owner, user_commit_count)}
    for edge in repo_edges:
        node = edge.get("node", {})
        if node.get("nameWithOwner"):
            name_owner = node["nameWithOwner"]
            repo_hash = hashlib.sha256(name_owner.encode("utf-8")).hexdigest()
            count = 0
            try:
                history = (
                    node.get("defaultBranchRef", {})
                    .get("target", {})
                    .get("history", {})
                )
                if history:
                    count = history.get("totalCount", 0)
            except AttributeError:
                pass
            current_repos[repo_hash] = (name_owner, count)

    # Try reading existing cache
    try:
        with open(CACHE_PATH, "r") as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 4:
                    h, c, a, d = parts
                    try:
                        cached_data[h] = [int(c), int(a), int(d)]
                    except ValueError:
                        cache_needs_update = True  # Mark cache dirty if corrupt
                elif line.strip():
                    cache_needs_update = True
    except FileNotFoundError:
        print(f"Cache file {CACHE_PATH} not found. Creating fresh cache.")
        cache_needs_update = True
    except IOError as e:
        print(f"Error reading cache file {CACHE_PATH}: {e}")
        cache_needs_update = True  # Treat read error as needing update

    # Check if repo list differs from cache, forcing update if needed
    if set(current_repos.keys()) != set(cached_data.keys()):
        print("Repository list changed. Updating cache structure.")
        cache_needs_update = True

    if FORCE_CACHE_REFRESH:
        print("Forcing cache refresh.")
        cache_needs_update = True
        cached_data = {}

    print(f"Processing {len(current_repos)} repositories...")
    start_time = time.time()
    processed_count = 0
    for repo_hash, (name_owner, current_commit_count) in current_repos.items():
        cached_info = cached_data.get(repo_hash)
        needs_recalc = True

        if cached_info and not FORCE_CACHE_REFRESH:
            cached_count, cached_adds, cached_dels = cached_info
            if cached_count == current_commit_count:
                # Cache hit
                total_add += cached_adds
                total_del += cached_dels
                final_cache_data[repo_hash] = [cached_count, cached_adds, cached_dels]
                needs_recalc = False
            else:
                # Commit count changed, mark for update but don't print warning yet
                cache_needs_update = True

        if needs_recalc:
            if not cache_needs_update:  # Only print if this is the *reason* for update
                print(
                    f"Cache outdated for {name_owner}. Recalculating...",
                )
            cache_needs_update = True  # Ensure cache is marked for update
            owner, repo = name_owner.split("/")
            adds, dels, user_commits = _fetch_commit_loc(owner, repo, author_id)
            total_add += adds
            total_del += dels
            final_cache_data[repo_hash] = [user_commits, adds, dels]

        processed_count += 1
        if processed_count % 50 == 0 or processed_count == len(current_repos):
            elapsed = time.time() - start_time
            rate = processed_count / elapsed if elapsed > 0 else 0
            print(
                f"Processed {processed_count}/{len(current_repos)}... ({elapsed:.1f}s, {rate:.1f} repo/s)",
            )

    if cache_needs_update:
        print(f"Writing updated cache to {CACHE_PATH}")
        try:
            with open(CACHE_PATH, "w") as f:
                for h, data in final_cache_data.items():
                    f.write(f"{h} {data[0]} {data[1]} {data[2]}\n")
        except IOError as e:
            print(f"Error writing cache file {CACHE_PATH}: {e}")

    return total_add, total_del


def update_readme(additions, deletions, net, timestamp):
    try:
        with open(README_PATH, "r") as f:
            content = f.read()

        add_str, del_str, net_str = f"{additions:,}", f"{deletions:,}", f"{net:,}"

        replacements = [
            (
                r"i've written \*\*[\d,]+\*\* lines of code and deleted \*\*[\d,]+\*\* lines\.",
                f"i've written **{add_str}** lines of code and deleted **{del_str}** lines.",
            ),
            (
                r"that's a net of \*\*[\d,]+\*\* lines still running somewhere in the world\.",
                f"that's a net of **{net_str}** lines still running somewhere in the world.",
            ),
            (
                r"<!-- last updated: \d+ -->",
                f"<!-- last updated: {int(timestamp)} -->",
            ),
        ]

        for pattern, replacement in replacements:
            content = re.sub(pattern, replacement, content)

        with open(README_PATH, "w") as f:
            f.write(content)

        print("Successfully updated LOC stats")

    except Exception as e:
        print(f"Error updating README: {e}")


if __name__ == "__main__":
    print(f"Calculating LOC for user: {USER_NAME} ({OWNER_ID['id']})")
    overall_start_time = time.time()
    try:
        additions, deletions = calculate_loc_stats(AFFILIATIONS, OWNER_ID)
        net = additions - deletions
        overall_end_time = time.time()
        duration = overall_end_time - overall_start_time

        print(
            f"Added: {additions:,} | Deleted: {deletions:,} | Net: {net:,} | Time: {duration:.2f}s"
        )

        update_readme(additions, deletions, net, overall_end_time)

        if redis_client:
            try:
                redis_key = "loc_stats"
                redis_data = {
                    "user_name": USER_NAME,
                    "additions": additions,
                    "deletions": deletions,
                    "net": net,
                    "duration_seconds": f"{duration:.2f}",
                    "last_updated_unix": int(overall_end_time),
                }
                redis_client.set(redis_key, json.dumps(redis_data))
                print(f"Stored results in Upstash Redis key: {redis_key}")
            except Exception as redis_write_err:
                print(
                    f"Warning: Failed to write results to Upstash Redis - {redis_write_err}"
                )

    except Exception as e:
        overall_end_time = time.time()
        print(e)
        print(
            f"Time elapsed before error: {overall_end_time - overall_start_time:.2f}s",
        )
        sys.exit(1)
