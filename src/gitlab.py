import requests
import os
from urllib.parse import quote_plus

GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_API_URL = os.getenv("GITLAB_API_URL", "https://gitlab.com/api/v4")


def get_gitlab_file(pid, path, ref):
    encoded = quote_plus(path)
    url = f"{GITLAB_API_URL}/projects/{pid}/repository/files/{encoded}/raw"

    try:
        r = requests.get(url, headers={'PRIVATE-TOKEN': GITLAB_TOKEN}, params={'ref': ref})
        return r.text if r.status_code == 200 else None
    except:
        return None


def get_project_files(pid, ref):
    url = f"{GITLAB_API_URL}/projects/{pid}/repository/tree"
    files = []
    page = 1

    while True:
        r = requests.get(url, headers={'PRIVATE-TOKEN': GITLAB_TOKEN},
                         params={'ref': ref, 'recursive': True, 'per_page': 100, 'page': page})

        if r.status_code != 200 or not r.json():
            break
        files.extend(
            [i['path'] for i in r.json() if i['type'] == 'blob' and i['path'].endswith('.sql')])
        if 'next' not in r.links:
            break

        page += 1

    return files


def get_mr_diff(pid, mr_iid):
    url = f"{GITLAB_API_URL}/projects/{pid}/merge_requests/{mr_iid}/changes"
    r = requests.get(url, headers={'PRIVATE-TOKEN': GITLAB_TOKEN})

    if r.status_code != 200:
        return set(), set()

    data = r.json()
    changes = data.get('changes', [])

    return ({c['new_path'] for c in changes if not c.get('deleted_file')},
            {c['old_path'] for c in changes if c.get('deleted_file')})


def fetch_hybrid_files(pid, iid, src, tgt):
    all_paths = get_project_files(pid, tgt)
    changed_paths, deleted_paths = get_mr_diff(pid, iid)

    files = []

    for p in all_paths:
        if p in deleted_paths:
            continue
        is_mod = p in changed_paths

        content = get_gitlab_file(pid, p, src if is_mod else tgt)
        if content:
            files.append({"path": p, "content": content, "is_modified": is_mod})

    for p in changed_paths:
        if p not in [f['path'] for f in files]:
            content = get_gitlab_file(pid, p, src)
            if content:
                files.append({"path": p, "content": content, "is_modified": True})

    return files


def is_mr_creation(payload: dict) -> bool:
    object_kind = payload.get("object_kind")
    action = payload.get("object_attributes", {}).get("action")
    is_created = (object_kind == "merge_request" and action == "open")

    return is_created


def extract_mr_data(payload: dict) -> dict:
    attrs = payload.get("object_attributes", {})

    return {
        "project_id": payload.get("project", {}).get("id"),
        "mr_iid": attrs.get("iid"),
        "source_branch": attrs.get("source_branch"),
        "target_branch": attrs.get("target_branch")
    }


def post_mr_comment(project_id: int, mr_iid: int, comment: str):
    url = f"{GITLAB_API_URL}/projects/{project_id}/merge_requests/{mr_iid}/notes"
    headers = {"PRIVATE-TOKEN": GITLAB_TOKEN}

    response = requests.post(
        url,
        headers=headers,
        json={"body": comment},
        timeout=10)

    return response
