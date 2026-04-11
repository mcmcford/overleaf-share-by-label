import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus, urlencode
from urllib.request import Request, urlopen

from pymongo import MongoClient
from pymongo.errors import PyMongoError


@dataclass(frozen=True)
class TeamRoleDefinition:
    key: str
    project_ref_field: str
    precedence: int


ROLE_DEFINITIONS = (
    TeamRoleDefinition("Read", "readOnly_refs", 1),
    TeamRoleDefinition("Review", "reviewer_refs", 2),
    TeamRoleDefinition("Collab", "collaberator_refs", 3),
)
TEAM_ROLES = tuple(role.key for role in ROLE_DEFINITIONS)
PROJECT_ACCESS_FIELDS = tuple(role.project_ref_field for role in ROLE_DEFINITIONS)
DEFAULT_TAG_COLOR = "#1f75cb"
PROJECT_TAG_SOURCE_MODE_OWNER_ONLY = "owner-only"
PROJECT_TAG_SOURCE_MODE_ANY_ACCESS = "any-access"


@dataclass(frozen=True)
class TeamRoleMapping:
    team_name: str
    role_key: str
    tag_name: str
    authentik_group_name: str
    project_ref_field: str
    precedence: int


@dataclass(frozen=True)
class ManagedTagDocument:
    tag_object_id: Any
    user_id: str
    tag_name: str
    current_project_ids: list[str]
    current_raw_project_ids: list[Any]


@dataclass(frozen=True)
class TagSyncPlan:
    tag_object_id: Any | None
    user_id: str
    tag_name: str
    current_project_ids: list[str]
    desired_project_ids: list[str]
    desired_raw_project_ids: list[Any]

    def needs_create(self) -> bool:
        return self.tag_object_id is None

    def needs_update(self) -> bool:
        return (
            self.tag_object_id is not None
            and self.current_project_ids != self.desired_project_ids
        )


@dataclass
class ProjectAccessPlan:
    project_object_id: Any
    project_id: str
    project_name: str
    owner_ref: str
    applied_tags: list[str]
    suppressed_tags: list[str]
    selected_mappings: list[TeamRoleMapping]
    current_refs_by_field: dict[str, list[str]]
    desired_refs_by_field: dict[str, list[str]]
    desired_raw_refs_by_field: dict[str, list[Any]]

    def has_changes(self) -> bool:
        for field in PROJECT_ACCESS_FIELDS:
            if self.current_refs_by_field.get(
                field, []
            ) != self.desired_refs_by_field.get(field, []):
                return True
        return False


def load_dotenv(dotenv_path: Path) -> None:
    """Load simple KEY=VALUE pairs from a local .env file if present."""
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def get_required_env(name: str, aliases: tuple[str, ...] = ()) -> str:
    for candidate in (name, *aliases):
        value = os.getenv(candidate)
        if value:
            return value

    alias_text = f" (or {', '.join(aliases)})" if aliases else ""
    raise SystemExit(f"Missing required environment variable: {name}{alias_text}")


def api_get(
    base_url: str, token: str, path: str, params: dict[str, Any]
) -> dict[str, Any]:
    query = urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
    url = f"{base_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{query}"

    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="GET",
    )

    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(
            f"Authentik API returned HTTP {exc.code} for {url}\n{body}"
        ) from exc
    except URLError as exc:
        raise SystemExit(
            f"Could not connect to Authentik at {url}: {exc.reason}"
        ) from exc


def fetch_all_results(
    base_url: str, token: str, path: str, extra_params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    params = dict(extra_params or {})
    page_size = int(os.getenv("AUTHENTIK_PAGE_SIZE", "100"))
    page = 1
    results: list[dict[str, Any]] = []

    while True:
        payload = api_get(
            base_url,
            token,
            path,
            {
                **params,
                "page": page,
                "page_size": page_size,
            },
        )

        batch = payload.get("results", [])
        if not isinstance(batch, list):
            raise SystemExit(
                f"Unexpected response format from {path}: missing results list"
            )

        results.extend(batch)

        pagination = payload.get("pagination", {})
        total_pages = pagination.get("total_pages")
        next_page = pagination.get("next")

        if total_pages is not None and page >= total_pages:
            break
        if next_page in (None, "", False) and len(batch) < page_size:
            break
        if not batch:
            break

        page += 1

    return results


def create_group(base_url: str, token: str, name: str) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v3/core/groups/"
    data = json.dumps({"name": name}).encode("utf-8")

    request = Request(
        url,
        data=data,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(
            f"Authentik API returned HTTP {exc.code} for {url}\n{body}"
        ) from exc
    except URLError as exc:
        raise SystemExit(
            f"Could not connect to Authentik at {url}: {exc.reason}"
        ) from exc


def env_bool(name: str, default: bool = False) -> bool:
    """
    Convert the various ways of expressing true/false in environment variables into a boolean value,
    default to false if the variable is not set or cannot be interpreted, and allow an optional default to override that.
    """

    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def get_project_tag_source_mode() -> str:
    """Resolve who is allowed to drive managed project tags."""

    aliases = {
        "owner-only": PROJECT_TAG_SOURCE_MODE_OWNER_ONLY,
        "owner_only": PROJECT_TAG_SOURCE_MODE_OWNER_ONLY,
        "owner": PROJECT_TAG_SOURCE_MODE_OWNER_ONLY,
        "any-access": PROJECT_TAG_SOURCE_MODE_ANY_ACCESS,
        "any_access": PROJECT_TAG_SOURCE_MODE_ANY_ACCESS,
        "collaborative": PROJECT_TAG_SOURCE_MODE_ANY_ACCESS,
        "shared": PROJECT_TAG_SOURCE_MODE_ANY_ACCESS,
    }
    configured_value = os.getenv(
        "PROJECT_TAG_SOURCE_MODE", PROJECT_TAG_SOURCE_MODE_OWNER_ONLY
    )
    normalized_value = aliases.get(configured_value.strip().lower())

    if normalized_value is None:
        raise SystemExit(
            "PROJECT_TAG_SOURCE_MODE must be one of: "
            f"{PROJECT_TAG_SOURCE_MODE_OWNER_ONLY}, {PROJECT_TAG_SOURCE_MODE_ANY_ACCESS}"
        )

    return normalized_value


def build_overleaf_mongo_uri() -> str:
    """Resolve the Overleaf MongoDB URI from env vars, mirroring the bridge config style."""

    mongo_uri = os.getenv("OVERLEAF_MONGO_URI")
    if mongo_uri:
        return mongo_uri

    host = os.getenv("OVERLEAF_MONGO_HOST", "localhost")
    port = os.getenv("OVERLEAF_MONGO_PORT", "27017")
    username = os.getenv("OVERLEAF_MONGO_USERNAME")
    password = os.getenv("OVERLEAF_MONGO_PASSWORD")
    auth_db = os.getenv("OVERLEAF_MONGO_AUTH_DB", "admin")
    auth_mechanism = os.getenv("OVERLEAF_MONGO_AUTH_MECHANISM")
    use_tls = env_bool("OVERLEAF_MONGO_TLS")

    if bool(username) != bool(password):
        raise SystemExit(
            "OVERLEAF_MONGO_USERNAME and OVERLEAF_MONGO_PASSWORD must be set together."
        )

    auth_prefix = ""
    query_params: dict[str, str] = {}

    if username and password:
        auth_prefix = f"{quote_plus(username)}:{quote_plus(password)}@"
        query_params["authSource"] = auth_db

    if auth_mechanism:
        query_params["authMechanism"] = auth_mechanism
    if use_tls:
        query_params["tls"] = "true"

    base_uri = f"mongodb://{auth_prefix}{host}:{port}/"
    query_string = urlencode(query_params)
    return f"{base_uri}?{query_string}" if query_string else base_uri


def fetch_overleaf_users(
    mongo_uri: str, mongo_database: str, collection_name: str = "users"
) -> list[dict[str, Any]]:
    """Connect to MongoDB and return all documents from the users collection."""

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)

    try:
        client.admin.command("ping")
        collection = client[mongo_database][collection_name]
        return list(collection.find({}))
    except PyMongoError as exc:
        raise SystemExit(f"Could not read users from MongoDB: {exc}") from exc
    finally:
        client.close()


def fetch_overleaf_tags(
    mongo_uri: str, mongo_database: str, collection_name: str = "tags"
) -> list[dict[str, Any]]:
    """Connect to MongoDB and return all documents from the tags collection."""

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)

    try:
        client.admin.command("ping")
        collection = client[mongo_database][collection_name]
        return list(collection.find({}))
    except PyMongoError as exc:
        raise SystemExit(f"Could not read tags from MongoDB: {exc}") from exc
    finally:
        client.close()


def fetch_overleaf_projects(
    mongo_uri: str, mongo_database: str, collection_name: str = "projects"
) -> list[dict[str, Any]]:
    """Connect to MongoDB and return all documents from the projects collection."""

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)

    try:
        client.admin.command("ping")
        collection = client[mongo_database][collection_name]
        return list(collection.find({}))
    except PyMongoError as exc:
        raise SystemExit(f"Could not read projects from MongoDB: {exc}") from exc
    finally:
        client.close()


def create_overleaf_tags(
    mongo_uri: str,
    mongo_database: str,
    tags_to_create: list[dict[str, Any]],
    collection_name: str = "tags",
) -> int:
    """Insert new tag documents into Overleaf's tags collection."""

    if not tags_to_create:
        return 0

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)

    try:
        client.admin.command("ping")
        collection = client[mongo_database][collection_name]
        result = collection.insert_many(tags_to_create, ordered=True)
        return len(result.inserted_ids)
    except PyMongoError as exc:
        raise SystemExit(f"Could not create tags in MongoDB: {exc}") from exc
    finally:
        client.close()


def apply_project_access_plans(
    mongo_uri: str,
    mongo_database: str,
    project_access_plans: list[ProjectAccessPlan],
    collection_name: str = "projects",
) -> int:
    """Apply planned project access updates to Overleaf's projects collection."""

    changed_plans = [plan for plan in project_access_plans if plan.has_changes()]
    if not changed_plans:
        return 0

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)

    try:
        client.admin.command("ping")
        collection = client[mongo_database][collection_name]

        for plan in changed_plans:
            collection.update_one(
                {"_id": plan.project_object_id},
                {
                    "$set": {
                        field: plan.desired_raw_refs_by_field[field]
                        for field in PROJECT_ACCESS_FIELDS
                    }
                },
            )

        return len(changed_plans)
    except PyMongoError as exc:
        raise SystemExit(f"Could not update projects in MongoDB: {exc}") from exc
    finally:
        client.close()


def print_overleaf_users(users: list[dict[str, Any]]) -> None:
    """Print a concise summary of Overleaf users pulled from MongoDB."""

    print(f"Found {len(users)} Overleaf user(s):")

    for user in users:
        print(user)


class ol_saml_identity:
    """
    'samlIdentifiers': [{'providerId': '1', 'externalUserId': 'ca2854xxxxxxxxxxxxf734445d5d9cccccccccccccccbcd2319d832e', 'userIdAttribute': 'nameID'}]
    """

    def __init__(
        self,
        providerId: str,
        externalUserId: str,
        userIdAttribute: str,
    ) -> None:
        self.providerId = providerId
        self.externalUserId = externalUserId
        self.userIdAttribute = userIdAttribute


class User:
    """
    {'pk': 10, 'username': 'mcford.morgan', 'name': 'Morgan McFord', 'is_active': True, 'last_login': '2026-04-02T23:56:05.789102Z',
    'email': 'morgan.mcford@cgi.com', 'attributes': {}, 'uid': 'ca28541618d3ddcf734445d5d906f84f43e89ff76d472dab6d54bcd2319d832e'}
    """

    def __init__(
        self,
        ol_objectid_raw: Any,
        ol_objectid: str,
        ol_email: str | None,
        ol_saml_identities: list[ol_saml_identity],
        authentik_pk: int | None,
        authentik_username: str | None,
        authentik_name: str | None,
        authentik_email: str | None,
        authentik_is_active: bool | None,
        authentik_uid: str | None,
        authentik_group_names: list[str],
        expected_tags: list[str],
        current_tags: list[str],
        missing_tags: list[str],
    ) -> None:
        self.ol_objectid_raw = ol_objectid_raw
        self.ol_objectid = ol_objectid
        self.ol_email = ol_email
        self.ol_saml_identities = ol_saml_identities
        self.authentik_pk = authentik_pk
        self.authentik_username = authentik_username
        self.authentik_name = authentik_name
        self.authentik_email = authentik_email
        self.authentik_is_active = authentik_is_active
        self.authentik_uid = authentik_uid
        self.authentik_group_names = authentik_group_names
        self.expected_tags = expected_tags
        self.current_tags = current_tags
        self.missing_tags = missing_tags

    def is_correlated(self) -> bool:
        return self.authentik_pk is not None

    def __str__(self) -> str:
        return f"Users(ol_objectid={self.ol_objectid}, ol_email={self.ol_email}, ol_saml_identities=[{', '.join(str(saml.__dict__) for saml in self.ol_saml_identities)}], authentik_pk={self.authentik_pk}, authentik_username={self.authentik_username}, authentik_name={self.authentik_name}, authentik_email={self.authentik_email}, authentik_is_active={self.authentik_is_active}, authentik_uid={self.authentik_uid}, authentik_group_names={self.authentik_group_names}, expected_tags={self.expected_tags}, current_tags={self.current_tags}, missing_tags={self.missing_tags})"


def build_authentik_team_group_name(team_name: str) -> str:
    """Return the single Authentik group name used for all roles in a team."""

    return f"overleaf-teams-{team_name}"


def build_team_role_mappings(team_names: list[str]) -> list[TeamRoleMapping]:
    """Build the canonical mapping objects linking Overleaf role tags to team groups."""

    mappings: list[TeamRoleMapping] = []

    for team_name in team_names:
        for role in ROLE_DEFINITIONS:
            mappings.append(
                TeamRoleMapping(
                    team_name=team_name,
                    role_key=role.key,
                    tag_name=f"{team_name} - {role.key}",
                    # Authentik only stores team membership; Overleaf tags decide access level.
                    authentik_group_name=build_authentik_team_group_name(team_name),
                    project_ref_field=role.project_ref_field,
                    precedence=role.precedence,
                )
            )

    return mappings


def initialise_users(overleaf_users: list[dict[str, Any]]):
    """
    Initialise all our user objects based on the overleaf data.

    This should be called before we correlate with authentik users so we can disregard the majority of authentik users that won't
    have any association with overleaf users, and also so we can fill in the authentik fields of the user objects as we correlate them.
    """

    for user in overleaf_users:
        ol_objectid_raw = user.get("_id")
        ol_objectid = str(ol_objectid_raw)
        ol_email = user.get("email")
        ol_saml_identities = []
        for saml in user.get("samlIdentifiers", []):
            ol_saml_identities.append(
                ol_saml_identity(
                    providerId=saml.get("providerId", ""),
                    externalUserId=saml.get("externalUserId", ""),
                    userIdAttribute=saml.get("userIdAttribute", ""),
                )
            )

        authentik_pk = None
        authentik_username = None
        authentik_name = None
        authentik_email = None
        authentik_is_active = None
        authentik_uid = None
        authentik_group_names: list[str] = []
        expected_tags: list[str] = []
        current_tags: list[str] = []
        missing_tags: list[str] = []

        yield User(
            ol_objectid_raw=ol_objectid_raw,
            ol_objectid=ol_objectid,
            ol_email=ol_email,
            ol_saml_identities=ol_saml_identities,
            authentik_pk=authentik_pk,
            authentik_username=authentik_username,
            authentik_name=authentik_name,
            authentik_email=authentik_email,
            authentik_is_active=authentik_is_active,
            authentik_uid=authentik_uid,
            authentik_group_names=authentik_group_names,
            expected_tags=expected_tags,
            current_tags=current_tags,
            missing_tags=missing_tags,
        )


def correlate_users(
    overleaf_users: list[dict[str, Any]], authentik_users: list[dict[str, Any]]
) -> list[User]:
    """
    Correlate Overleaf users with Authentik users based on the samlIdentifiers.externalUserId field in Overleaf and the uid field in Authentik.

    If we can't find a correlation based on that, we can fall back to correlating based on email address, however this is less reliable as email addresses can change and may not be unique.
    """

    correlated_users: list[User] = []

    for user in initialise_users(overleaf_users):
        matched_authentik_user = None
        for saml_identity in user.ol_saml_identities:
            for authentik_user in authentik_users:
                if saml_identity.externalUserId == authentik_user.get("uid"):
                    matched_authentik_user = authentik_user
                    break
            if matched_authentik_user:
                break

        if not matched_authentik_user and user.ol_email:
            for authentik_user in authentik_users:
                if user.ol_email == authentik_user.get("email"):
                    matched_authentik_user = authentik_user
                    break

        if matched_authentik_user:
            user.authentik_pk = matched_authentik_user.get("pk")
            user.authentik_username = matched_authentik_user.get("username")
            user.authentik_name = matched_authentik_user.get("name")
            user.authentik_email = matched_authentik_user.get("email")
            user.authentik_is_active = matched_authentik_user.get("is_active")
            user.authentik_uid = matched_authentik_user.get("uid")
            user.authentik_group_names = sorted(
                str(group_name)
                for group_name in matched_authentik_user.get("group_names", [])
                if group_name
            )

        correlated_users.append(user)

    return correlated_users


def extract_authentik_users_from_groups(
    groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Flatten unique Authentik user records from the nested group payloads."""

    users_by_key: dict[str, dict[str, Any]] = {}

    for group in groups:
        group_users = group.get("users_obj") or group.get("users") or []
        group_name = str(group.get("name") or "").strip()

        for user in group_users:
            if not isinstance(user, dict):
                continue

            user_pk = user.get("pk")
            user_uid = user.get("uid")
            if user_pk is not None:
                key = f"pk:{user_pk}"
            elif user_uid:
                key = f"uid:{user_uid}"
            else:
                continue

            existing = users_by_key.get(key)
            if not existing:
                existing = dict(user)
                existing["group_names"] = []
                users_by_key[key] = existing

            if group_name and group_name not in existing["group_names"]:
                existing["group_names"].append(group_name)

    flattened_users = list(users_by_key.values())

    return sorted(
        flattened_users, key=lambda item: (item.get("username") or "").lower()
    )


def check_groups_exist(
    groups: list[dict[str, Any]], team_role_mappings: list[TeamRoleMapping]
) -> list[str]:
    """Return the Authentik groups required by the configured team-role mappings."""

    existing_group_names = {
        str(group.get("name") or "").strip() for group in groups if group.get("name")
    }

    return sorted(
        {
            mapping.authentik_group_name
            for mapping in team_role_mappings
            if mapping.authentik_group_name not in existing_group_names
        }
    )


def build_expected_tag_names(team_role_mappings: list[TeamRoleMapping]) -> list[str]:
    """Return the canonical list of enforced tag names."""

    return sorted({mapping.tag_name for mapping in team_role_mappings})


def build_tags_by_user_id(tags: list[dict[str, Any]]) -> dict[str, set[str]]:
    """Index Overleaf tags by their owning user id."""

    tags_by_user_id: dict[str, set[str]] = {}

    for tag in tags:
        user_id = str(tag.get("user_id") or "").strip()
        tag_name = str(tag.get("name") or "").strip()
        if not user_id or not tag_name:
            continue
        tags_by_user_id.setdefault(user_id, set()).add(tag_name)

    return tags_by_user_id


def build_tags_by_project_id(tags: list[dict[str, Any]]) -> dict[str, set[str]]:
    """Index Overleaf tags by project id using the tag documents' project_ids field."""

    tags_by_project_id: dict[str, set[str]] = {}

    for tag in tags:
        tag_name = str(tag.get("name") or "").strip()
        if not tag_name:
            continue

        for project_id in tag.get("project_ids", []):
            project_id_str = str(project_id).strip()
            if not project_id_str:
                continue
            tags_by_project_id.setdefault(project_id_str, set()).add(tag_name)

    return tags_by_project_id


def build_project_tag_sources_by_project(
    tags: list[dict[str, Any]], managed_tag_names: set[str]
) -> dict[str, dict[str, set[str]]]:
    """Index managed project tags by project id and the user who applied them."""

    project_tag_sources: dict[str, dict[str, set[str]]] = {}

    for tag in tags:
        user_id = str(tag.get("user_id") or "").strip()
        tag_name = str(tag.get("name") or "").strip()
        if not user_id or tag_name not in managed_tag_names:
            continue

        seen_project_ids: set[str] = set()
        for raw_project_id in tag.get("project_ids", []):
            project_id = str(raw_project_id).strip()
            if not project_id or project_id in seen_project_ids:
                continue

            seen_project_ids.add(project_id)
            project_tag_sources.setdefault(project_id, {}).setdefault(
                tag_name, set()
            ).add(user_id)

    return project_tag_sources


def build_correlated_users_by_authentik_group(
    correlated_users: list[User],
) -> dict[str, list[User]]:
    """Index correlated Overleaf users by the Authentik groups they belong to."""

    users_by_group: dict[str, list[User]] = {}

    for user in correlated_users:
        if not user.is_correlated():
            continue

        for group_name in user.authentik_group_names:
            users_by_group.setdefault(group_name, []).append(user)

    for group_name in users_by_group:
        users_by_group[group_name].sort(
            key=lambda item: (
                item.authentik_username or item.ol_email or item.ol_objectid
            )
        )

    return users_by_group


def find_overleaf_users_for_mapping(
    mapping: TeamRoleMapping, users_by_group: dict[str, list[User]]
) -> list[User]:
    """Return correlated Overleaf users who belong to a mapping's Authentik group."""

    return list(users_by_group.get(mapping.authentik_group_name, []))


def select_project_role_mappings(
    tag_names: list[str], mappings_by_tag_name: dict[str, TeamRoleMapping]
) -> tuple[list[TeamRoleMapping], list[str]]:
    """Resolve project tags into one effective mapping per team."""

    selected_by_team: dict[str, TeamRoleMapping] = {}
    suppressed_tags: set[str] = set()

    for tag_name in sorted(set(tag_names)):
        mapping = mappings_by_tag_name.get(tag_name)
        if mapping is None:
            continue

        existing = selected_by_team.get(mapping.team_name)
        if existing is None or mapping.precedence > existing.precedence:
            if existing is not None:
                suppressed_tags.add(existing.tag_name)
            selected_by_team[mapping.team_name] = mapping
            continue

        suppressed_tags.add(mapping.tag_name)

    selected_mappings = sorted(
        selected_by_team.values(), key=lambda item: (item.team_name, item.precedence)
    )
    return selected_mappings, sorted(suppressed_tags)


def normalize_ref_values(raw_refs: list[Any]) -> tuple[list[Any], list[str]]:
    """Deduplicate MongoDB reference values while preserving stable order."""

    unique_raw_refs: list[Any] = []
    unique_ref_strings: list[str] = []
    seen: set[str] = set()

    for raw_ref in raw_refs:
        ref_string = str(raw_ref).strip()
        if not ref_string or ref_string in seen:
            continue
        seen.add(ref_string)
        unique_raw_refs.append(raw_ref)
        unique_ref_strings.append(ref_string)

    return unique_raw_refs, unique_ref_strings


def audit_user_tags(
    correlated_users: list[User],
    tags: list[dict[str, Any]],
    team_role_mappings: list[TeamRoleMapping],
) -> list[User]:
    """Attach expected/current/missing Overleaf tags to each correlated user."""

    tags_by_user_id = build_tags_by_user_id(tags)
    expected_tag_names = build_expected_tag_names(team_role_mappings)

    for user in correlated_users:
        if not user.is_correlated():
            continue

        user.expected_tags = expected_tag_names
        user.current_tags = sorted(tags_by_user_id.get(user.ol_objectid, set()))
        user.missing_tags = sorted(set(user.expected_tags) - set(user.current_tags))

    return correlated_users


def build_project_access_plans(
    projects: list[dict[str, Any]],
    tags: list[dict[str, Any]],
    team_role_mappings: list[TeamRoleMapping],
    correlated_users: list[User],
    project_tag_source_mode: str,
) -> list[ProjectAccessPlan]:
    """Resolve project tags into desired Overleaf access lists."""

    users_by_group = build_correlated_users_by_authentik_group(correlated_users)
    managed_tag_names = set(build_expected_tag_names(team_role_mappings))
    project_tag_sources_by_project = build_project_tag_sources_by_project(
        tags, managed_tag_names
    )
    mappings_by_tag_name = {mapping.tag_name: mapping for mapping in team_role_mappings}
    all_mappings_by_field: dict[str, list[TeamRoleMapping]] = {
        field: [
            mapping
            for mapping in team_role_mappings
            if mapping.project_ref_field == field
        ]
        for field in PROJECT_ACCESS_FIELDS
    }

    project_access_plans: list[ProjectAccessPlan] = []

    for project in projects:
        project_object_id = project.get("_id")
        project_id = str(project_object_id)
        project_name = str(project.get("name") or project_id)
        owner_ref = str(project.get("owner_ref") or "").strip()

        current_raw_refs_by_field: dict[str, list[Any]] = {
            field: list(project.get(field, [])) for field in PROJECT_ACCESS_FIELDS
        }
        current_refs_by_field: dict[str, list[str]] = {}
        desired_refs_by_field: dict[str, list[str]] = {}
        desired_raw_refs_by_field: dict[str, list[Any]] = {}

        current_access_user_ids: set[str] = {owner_ref} if owner_ref else set()

        for field in PROJECT_ACCESS_FIELDS:
            _, current_ref_strings = normalize_ref_values(
                current_raw_refs_by_field[field]
            )
            current_refs_by_field[field] = current_ref_strings
            current_access_user_ids.update(current_ref_strings)

        applied_tags = sorted(project_tag_sources_by_project.get(project_id, {}).keys())
        allowed_tag_source_user_ids = (
            {owner_ref}
            if project_tag_source_mode == PROJECT_TAG_SOURCE_MODE_OWNER_ONLY
            else current_access_user_ids
        )
        effective_tag_names = sorted(
            tag_name
            for tag_name, source_user_ids in project_tag_sources_by_project.get(
                project_id, {}
            ).items()
            if source_user_ids & allowed_tag_source_user_ids
        )
        selected_mappings, suppressed_tags = select_project_role_mappings(
            effective_tag_names, mappings_by_tag_name
        )

        for field in PROJECT_ACCESS_FIELDS:
            managed_ref_strings: set[str] = set()
            desired_managed_raw_refs: list[Any] = []

            for mapping in all_mappings_by_field[field]:
                for user in find_overleaf_users_for_mapping(mapping, users_by_group):
                    if user.ol_objectid == owner_ref:
                        continue
                    managed_ref_strings.add(user.ol_objectid)

            for mapping in selected_mappings:
                if mapping.project_ref_field != field:
                    continue
                for user in find_overleaf_users_for_mapping(mapping, users_by_group):
                    if user.ol_objectid == owner_ref:
                        continue
                    desired_managed_raw_refs.append(user.ol_objectid_raw)

            preserved_manual_raw_refs = [
                raw_ref
                for raw_ref in current_raw_refs_by_field[field]
                if str(raw_ref).strip() not in managed_ref_strings
            ]
            desired_raw_refs, desired_ref_strings = normalize_ref_values(
                preserved_manual_raw_refs + desired_managed_raw_refs
            )

            desired_raw_refs_by_field[field] = desired_raw_refs
            desired_refs_by_field[field] = desired_ref_strings

        project_access_plans.append(
            ProjectAccessPlan(
                project_object_id=project_object_id,
                project_id=project_id,
                project_name=project_name,
                owner_ref=owner_ref,
                applied_tags=applied_tags,
                suppressed_tags=suppressed_tags,
                selected_mappings=selected_mappings,
                current_refs_by_field=current_refs_by_field,
                desired_refs_by_field=desired_refs_by_field,
                desired_raw_refs_by_field=desired_raw_refs_by_field,
            )
        )

    return project_access_plans


def build_project_tag_viewer_ids(project_access_plan: ProjectAccessPlan) -> list[str]:
    """Return every user who should see the managed tags on a project."""

    viewer_ids: list[str] = []
    seen: set[str] = set()

    for user_id in [project_access_plan.owner_ref] + [
        ref
        for field in PROJECT_ACCESS_FIELDS
        for ref in project_access_plan.desired_refs_by_field[field]
    ]:
        normalized_user_id = str(user_id).strip()
        if not normalized_user_id or normalized_user_id in seen:
            continue

        seen.add(normalized_user_id)
        viewer_ids.append(normalized_user_id)

    return viewer_ids


def build_tag_sync_plans(
    tags: list[dict[str, Any]],
    team_role_mappings: list[TeamRoleMapping],
    correlated_users: list[User],
    project_access_plans: list[ProjectAccessPlan],
) -> list[TagSyncPlan]:
    """Build create/update plans for managed tag documents and their project visibility."""

    managed_tag_names = build_expected_tag_names(team_role_mappings)
    managed_tag_name_set = set(managed_tag_names)
    existing_docs_by_pair: dict[tuple[str, str], list[ManagedTagDocument]] = {}

    for tag in tags:
        user_id = str(tag.get("user_id") or "").strip()
        tag_name = str(tag.get("name") or "").strip()
        if not user_id or tag_name not in managed_tag_name_set:
            continue

        current_raw_project_ids, current_project_ids = normalize_ref_values(
            list(tag.get("project_ids", []))
        )
        existing_docs_by_pair.setdefault((user_id, tag_name), []).append(
            ManagedTagDocument(
                tag_object_id=tag.get("_id"),
                user_id=user_id,
                tag_name=tag_name,
                current_project_ids=current_project_ids,
                current_raw_project_ids=current_raw_project_ids,
            )
        )

    desired_pairs: set[tuple[str, str]] = set()
    desired_raw_project_ids_by_pair: dict[tuple[str, str], list[Any]] = {}

    for user in correlated_users:
        if not user.is_correlated():
            continue

        for tag_name in managed_tag_names:
            desired_pairs.add((user.ol_objectid, tag_name))

    for plan in project_access_plans:
        selected_tag_names = [mapping.tag_name for mapping in plan.selected_mappings]
        viewer_ids = build_project_tag_viewer_ids(plan)

        for viewer_id in viewer_ids:
            for tag_name in managed_tag_names:
                desired_pairs.add((viewer_id, tag_name))

        for tag_name in selected_tag_names:
            for viewer_id in viewer_ids:
                pair = (viewer_id, tag_name)
                desired_pairs.add(pair)
                desired_raw_project_ids_by_pair.setdefault(pair, []).append(
                    plan.project_id
                )

    sync_plans: list[TagSyncPlan] = []
    all_pairs = sorted(set(existing_docs_by_pair) | desired_pairs)

    for pair in all_pairs:
        desired_raw_project_ids, desired_project_ids = normalize_ref_values(
            desired_raw_project_ids_by_pair.get(pair, [])
        )
        existing_docs = existing_docs_by_pair.get(pair, [])

        if existing_docs:
            for existing_doc in existing_docs:
                sync_plans.append(
                    TagSyncPlan(
                        tag_object_id=existing_doc.tag_object_id,
                        user_id=existing_doc.user_id,
                        tag_name=existing_doc.tag_name,
                        current_project_ids=existing_doc.current_project_ids,
                        desired_project_ids=desired_project_ids,
                        desired_raw_project_ids=desired_raw_project_ids,
                    )
                )
            continue

        sync_plans.append(
            TagSyncPlan(
                tag_object_id=None,
                user_id=pair[0],
                tag_name=pair[1],
                current_project_ids=[],
                desired_project_ids=desired_project_ids,
                desired_raw_project_ids=desired_raw_project_ids,
            )
        )

    return sync_plans


def build_tag_documents_to_create(
    tag_sync_plans: list[TagSyncPlan],
) -> list[dict[str, Any]]:
    """Build new tag documents for missing managed tags."""

    color = os.getenv("OVERLEAF_TAG_COLOR", DEFAULT_TAG_COLOR)
    tag_documents: list[dict[str, Any]] = []

    for plan in tag_sync_plans:
        if not plan.needs_create():
            continue

        tag_documents.append(
            {
                "__v": 0,
                "color": color,
                "name": plan.tag_name,
                "project_ids": plan.desired_raw_project_ids,
                "user_id": plan.user_id,
            }
        )

    return tag_documents


def apply_tag_sync_plans(
    mongo_uri: str,
    mongo_database: str,
    tag_sync_plans: list[TagSyncPlan],
    collection_name: str = "tags",
) -> tuple[int, int]:
    """Create missing managed tags and sync project_ids on existing tag documents."""

    tags_to_create = build_tag_documents_to_create(tag_sync_plans)
    plans_to_update = [plan for plan in tag_sync_plans if plan.needs_update()]

    if not tags_to_create and not plans_to_update:
        return 0, 0

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)

    try:
        client.admin.command("ping")
        collection = client[mongo_database][collection_name]

        created_count = 0
        updated_count = 0

        if tags_to_create:
            created_count = len(
                collection.insert_many(tags_to_create, ordered=True).inserted_ids
            )

        for plan in plans_to_update:
            collection.update_one(
                {"_id": plan.tag_object_id},
                {"$set": {"project_ids": plan.desired_raw_project_ids}},
            )
            updated_count += 1

        return created_count, updated_count
    except PyMongoError as exc:
        raise SystemExit(f"Could not sync tags in MongoDB: {exc}") from exc
    finally:
        client.close()


def build_missing_tag_documents(users: list[User]) -> list[dict[str, Any]]:
    """Build MongoDB tag documents for every missing tag on matched users."""

    color = os.getenv("OVERLEAF_TAG_COLOR", DEFAULT_TAG_COLOR)
    tag_documents: list[dict[str, Any]] = []

    for user in users:
        if not user.is_correlated():
            continue

        for tag_name in user.missing_tags:
            tag_documents.append(
                {
                    "__v": 0,
                    "color": color,
                    "name": tag_name,
                    "project_ids": [],
                    # Overleaf stores tag user_id as a string, not an ObjectId.
                    "user_id": user.ol_objectid,
                }
            )

    return tag_documents


def print_tag_creation_plan(tags_to_create: list[dict[str, Any]]) -> None:
    """Print the tag documents that would be inserted into MongoDB."""

    print(f"Prepared {len(tags_to_create)} tag(s) to create:")
    for tag in tags_to_create:
        print(
            f"- user_id={tag['user_id']} | name={tag['name']} | color={tag['color']} | "
            f"project_ids={[str(project_id) for project_id in tag['project_ids']]}"
        )


def print_tag_project_update_plan(tag_sync_plans: list[TagSyncPlan]) -> None:
    """Print the managed tag documents whose project visibility will change."""

    plans_to_update = [plan for plan in tag_sync_plans if plan.needs_update()]
    print(f"Prepared {len(plans_to_update)} existing tag document(s) to update:")

    for plan in plans_to_update:
        print(
            f"- user_id={plan.user_id} | name={plan.tag_name} | "
            f"current={plan.current_project_ids} | desired={plan.desired_project_ids}"
        )


def print_tag_audit(users: list[User]) -> None:
    """Print the Overleaf tag audit for matched users."""

    matched_users = [user for user in users if user.is_correlated()]
    print(f"Audited tags for {len(matched_users)} correlated user(s):")

    for user in matched_users:
        print(
            f"- {user.authentik_username or user.ol_email or user.ol_objectid} | "
            f"expected={user.expected_tags} | current={user.current_tags} | "
            f"missing={user.missing_tags}"
        )


def print_correlated_users(users: list[User]) -> None:
    """Show the Overleaf-to-Authentik correlation results."""

    print(f"Correlated {len(users)} Overleaf user(s):")
    for user in users:
        status = "matched" if user.is_correlated() else "unmatched"
        print(
            f"- {user.ol_objectid} | {user.ol_email or '<no-email>'} | "
            f"authentik={user.authentik_username or '<none>'} | {status}"
        )


def print_team_role_mappings(
    team_role_mappings: list[TeamRoleMapping], users_by_group: dict[str, list[User]]
) -> None:
    """Print the tag-to-group mappings and how many correlated users each resolves to."""

    print(f"Resolved {len(team_role_mappings)} tag-to-role mapping(s):")
    for mapping in team_role_mappings:
        matched_users = find_overleaf_users_for_mapping(mapping, users_by_group)
        print(
            f"- {mapping.tag_name} | group={mapping.authentik_group_name} | "
            f"field={mapping.project_ref_field} | matched_users={len(matched_users)}"
        )


def print_project_access_plans(project_access_plans: list[ProjectAccessPlan]) -> None:
    """Print a concise summary of project access changes derived from managed tags."""

    tagged_projects = [plan for plan in project_access_plans if plan.selected_mappings]
    changed_projects = [plan for plan in project_access_plans if plan.has_changes()]

    print(
        f"Resolved project access for {len(project_access_plans)} project(s); "
        f"{len(tagged_projects)} have managed tags and {len(changed_projects)} need updates."
    )

    for plan in changed_projects:
        mapping_text = ", ".join(mapping.tag_name for mapping in plan.selected_mappings)
        print(
            f"- {plan.project_name} ({plan.project_id}) | "
            f"tags={plan.applied_tags} | effective={mapping_text or '<none>'}"
        )
        if plan.suppressed_tags:
            print(f"  suppressed={plan.suppressed_tags}")

        for field in PROJECT_ACCESS_FIELDS:
            current_refs = plan.current_refs_by_field[field]
            desired_refs = plan.desired_refs_by_field[field]
            if current_refs == desired_refs:
                continue
            print(f"  {field}: current={current_refs} | desired={desired_refs}")


def main() -> int:
    load_dotenv(Path(__file__).with_name(".env"))

    base_url = get_required_env("AUTHENTIK_URL", aliases=("AUTHENTIK_BASE_URL",))
    token = get_required_env("AUTHENTIK_TOKEN", aliases=("AUTHENTIK_API_TOKEN",))
    overleaf_mongo_uri = build_overleaf_mongo_uri()
    overleaf_mongo_db = get_required_env("OVERLEAF_MONGO_DB")
    teams = get_required_env("TEAMS").split(",")
    create_groups = env_bool("CREATE_GROUPS", default=True)
    create_tags = env_bool("CREATE_TAGS", default=False)
    apply_project_access = env_bool("APPLY_PROJECT_ACCESS", default=False)
    project_tag_source_mode = get_project_tag_source_mode()

    teams = [team.strip() for team in teams if team.strip()]
    team_role_mappings = build_team_role_mappings(teams)

    groups = fetch_all_results(
        base_url,
        token,
        "/api/v3/core/groups/",
        extra_params={"include_users": "true"},
    )
    authentik_users = extract_authentik_users_from_groups(groups)

    missing_groups = check_groups_exist(groups, team_role_mappings)

    if missing_groups:
        print("Found missing groups")

        if not create_groups:
            print(
                "The following groups are missing but will not be created because CREATE_GROUPS is false:"
            )
            for group_name in missing_groups:
                print(f"- {group_name}")
        else:
            for group_name in missing_groups:
                created_group = create_group(base_url, token, group_name)
                print(
                    f"Created group: {created_group.get('name')} (pk: {created_group.get('pk')})"
                )

    overleaf_users = fetch_overleaf_users(overleaf_mongo_uri, overleaf_mongo_db)
    overleaf_tags = fetch_overleaf_tags(overleaf_mongo_uri, overleaf_mongo_db)
    overleaf_projects = fetch_overleaf_projects(overleaf_mongo_uri, overleaf_mongo_db)
    correlated_users = correlate_users(overleaf_users, authentik_users)
    users_by_group = build_correlated_users_by_authentik_group(correlated_users)
    audited_users = audit_user_tags(correlated_users, overleaf_tags, team_role_mappings)
    project_access_plans = build_project_access_plans(
        overleaf_projects,
        overleaf_tags,
        team_role_mappings,
        correlated_users,
        project_tag_source_mode,
    )

    print(f"Extracted {len(authentik_users)} Authentik user(s) from groups")
    print_correlated_users(correlated_users)
    print_team_role_mappings(team_role_mappings, users_by_group)
    print_tag_audit(audited_users)
    print(f"Managed project tag mode: {project_tag_source_mode}")
    print_project_access_plans(project_access_plans)

    tag_sync_plans = build_tag_sync_plans(
        overleaf_tags,
        team_role_mappings,
        correlated_users,
        project_access_plans,
    )
    tags_to_create = build_tag_documents_to_create(tag_sync_plans)
    tag_update_plans = [plan for plan in tag_sync_plans if plan.needs_update()]

    if tags_to_create:
        print_tag_creation_plan(tags_to_create)
    if tag_update_plans:
        print_tag_project_update_plan(tag_update_plans)

    if tags_to_create or tag_update_plans:
        if create_tags:
            created_count, updated_count = apply_tag_sync_plans(
                overleaf_mongo_uri,
                overleaf_mongo_db,
                tag_sync_plans,
            )
            print(
                f"Created {created_count} tag(s) and updated {updated_count} tag document(s) in Overleaf MongoDB"
            )
        else:
            print("CREATE_TAGS is false, so tag documents were not created or updated.")
    else:
        print("No tag document changes are required.")

    changed_project_count = len(
        [plan for plan in project_access_plans if plan.has_changes()]
    )
    if changed_project_count:
        if apply_project_access:
            updated_projects = apply_project_access_plans(
                overleaf_mongo_uri,
                overleaf_mongo_db,
                project_access_plans,
            )
            print(f"Applied access updates to {updated_projects} project(s).")
        else:
            print(
                "APPLY_PROJECT_ACCESS is false, so project access changes were not applied."
            )
    else:
        print("No project access updates are required.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
