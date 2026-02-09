import os
import json
import requests
from typing import List, Dict, Optional, Any, Literal
from urllib.parse import quote_plus
from dotenv import load_dotenv

from pydantic import BaseModel, Field

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage

from fastapi import FastAPI, Body
import requests

load_dotenv()

GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_API_URL = os.getenv("GITLAB_API_URL", "https://gitlab.com/api/v4")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0,
    api_key=OPENAI_API_KEY)


# Definicja obiektow wyjsciowych z modelu
class DependencyMap(BaseModel):
    file_path: str
    defines: List[str] = Field(description="Obiekty tworzone w tym pliku (CREATE TABLE/VIEW/PROCEDURE)")
    depends_on: List[str] = Field(description="Obiekty używane/czytane w tym pliku (FROM, JOIN)")


class CodeReviewResult(BaseModel):
    severity: Literal["CRITICAL", "WARNING", "INFO"] = Field(
        description="Waga problemu. CRITICAL blokuje wdrożenie.")
    description: str = Field(description="Techniczny opis błędu lub ryzyka.")


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


# definicja agentow
def agent_dependency_mapper(file_data: dict) -> DependencyMap:
    prompt = f"""
        Jesteś ekspertem analizy przepływu danych (Data Lineage) w SQL. Twoim zadaniem jest analiza pliku: '{file_data['path']}'.

        ### ZASADY IDENTYFIKACJI:

        1. **DEFINES (Obiekty docelowe / Sinks):**
           Wypisz wszystkie tabele lub widoki, które są modyfikowane lub tworzone przez ten skrypt. Uwzględnij:
           - `CREATE TABLE / VIEW / MATERIALIZED VIEW`
           - `INSERT INTO` (lub samo `INSERT`)
           - `MERGE INTO` (cel operacji merge)
           - `UPDATE`
           - `TRUNCATE TABLE`
           - `REPLACE / CREATE OR REPLACE`

        2. **DEPENDS_ON (Obiekty źródłowe / Sources):**
           Wypisz wszystkie obiekty, z których dane są czytane. Uwzględnij:
           - Tabele/widoki w klauzulach `FROM` oraz `JOIN`.
           - Tabele w klauzuli `USING` (przy poleceniu MERGE).
           - Tabele użyte w podzapytaniach (`SELECT` wewnątrz `INSERT` lub `WHERE`).
           - Wywołania procedur/funkcji: `CALL` lub `EXECUTE`.

        3. **WAŻNE FILTROWANIE:**
           - **IGNORUJ** Common Table Expressions (CTE) zdefiniowane w klauzuli `WITH`.
           - **IGNORUJ** tabele tymczasowe (np. zaczynające się od `#` lub `temp_`), jeśli są tworzone i używane tylko w obrębie tego jednego skryptu.
           - Zwracaj **pełne nazwy** (np. `schema.table_name`), jeśli występują w kodzie.

        ### KOD SQL:
        ```sql
        {file_data['content']}
    """

    return llm.with_structured_output(DependencyMap) \
        .invoke(prompt)


def agent_logic_verifier(target_file: dict, related_files: list) -> list:
    result = []

    for rf in related_files:
        prompt = f"""
            Jesteś Senior Data Engineerem. Realizujesz code review jedynie w kontekście jednego obiektu zależnego.

            ZADANIE:
            Porównaj modyfikowany kod (TARGET) z kodem zależnym (DEPENDENCY). 
            Wskaż niespójności (np. wykorzystywanie kolumn które nie istnieją; złączenie po złych kluczach) ktore udało Ci się zaobserwować. 
            Pisz tylko to, czego jesteś pewny.

            TARGET (Plik: {target_file['path']}):
            {target_file['content']}

            DEPENDENCY (Plik: {rf['path']}):
            {rf['content']}

            Jeśli nie znajdujesz błędów w relacji między tymi konkretnymi plikami, zwróć severity='INFO' i description='Brak uwag'.
        """
        llm_result = llm.with_structured_output(CodeReviewResult) \
            .invoke(prompt)

        with open("log.txt", "w+") as f:
            f.write(prompt)

        result.append({
            "target_file_path": target_file["path"],
            "dependency": rf["path"],
            "severity": llm_result.severity,
            "description": llm_result.description
        })

    return result


def agent_holistic_review(target_file: dict) -> dict:
    prompt = f"""
    Jesteś Lead Data Engineerem i Architektem Danych.
    Twoim zadaniem jest wykonanie CODE REVIEW.

    Żródła informacji:

    KOD ŹRÓDŁOWY DO OCENY (Plik główny):
    Plik: '{target_file['path']}'
    Kod:
    ```sql
    {target_file['content']}
    ```

    ---
    INSTRUKCJA TWORZENIA RAPORTU:

    AUDYT JAKOŚCI KODU (Holistyczne Review)
    - Wskaż jeżeli występują błędu w zakresie:
      - Poprawności składniowej i logicznej
      - Wydajności

    FORMAT WYJŚCIOWY: Markdown, jedynie punkty. Bardzo zwięźle. 
    """

    return {"target_file_path": target_file["path"], "comment": llm.invoke(prompt).content}


# utils
def clean_obj_name(name: str) -> str:
    if not name:
        return ""
    return name.replace("[", "").replace("]", "") \
        .upper().strip()


def dict_list_to_md_table(data_list: list) -> str:
    if not data_list:
        return ""

    # Pobieramy nagłówki z kluczy pierwszego elementu
    headers = list(data_list[0].keys())

    # Tworzymy nagłówek tabeli (formatujemy klucze na ładniejsze nazwy)
    header_row = "| " + " | ".join(h.replace("_", " ").title() for h in headers) + " |"
    separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"

    # Tworzymy wiersze danych
    rows = []
    for item in data_list:
        # Wyciągamy wartości w kolejności nagłówków i usuwamy nowe linie
        row_values = [str(item.get(h, "")).replace("\n", " ") for h in headers]
        rows.append("| " + " | ".join(row_values) + " |")

    return "\n".join([header_row, separator_row] + rows)


def main(project_id, mr_iid, source_branch, target_branch):
    files = fetch_hybrid_files(
        project_id,
        mr_iid,
        source_branch,
        target_branch
    )

    dependency_map = []
    for f in files:
        file_dependency = agent_dependency_mapper(f)

        for defines in file_dependency.defines:
            dependency_map.append({"obj": clean_obj_name(defines), "file": file_dependency.file_path})
        for depends_on in file_dependency.depends_on:
            dependency_map.append({"obj": clean_obj_name(depends_on), "file": file_dependency.file_path})

    full_logic_review = []
    full_holistic_review = []

    changed = [f for f in files if f["is_modified"]]
    for ch in changed:
        objs = [
            dep["obj"] for dep in dependency_map if dep["file"] == ch["path"]]

        dep_files = []
        for obj in objs:
            dependencies = [dep["file"] for dep in dependency_map if dep["obj"] == obj]
            for dep in dependencies:
                dep_files.append(dep)

        dep_files = set(dep_files)
        dep_files_content = []
        for df in dep_files:
            content = next(f for f in files if f["path"] == df)
            dep_files_content.append(content)

        logic_review = agent_logic_verifier(
            ch, dep_files_content)
        holistic_review = agent_holistic_review(ch)

        full_logic_review.extend(logic_review)
        full_holistic_review.append(holistic_review)

    final_comment = "### AUTO REVIEW: \n"
    final_comment += dict_list_to_md_table(full_logic_review)
    final_comment += "\n\n"

    for fhr in full_holistic_review:
        final_comment += f"### PLIK: {fhr["target_file_path"]}"
        final_comment += "\n"
        final_comment += f"{fhr["comment"]}"
        final_comment += "\n\n"

    return final_comment


app = FastAPI()


@app.post("/webhook")
def gitlab_webhook(payload: dict = Body(...)):
    if payload.get("object_kind") != "merge_request":
        return {"status": "skipped"}

    attrs = payload.get("object_attributes", {})
    project_id = payload.get("project", {}).get("id")
    mr_iid = attrs.get("iid")
    source_branch = attrs.get("source_branch")
    target_branch = attrs.get("target_branch")

    comment = main(project_id, mr_iid, source_branch, target_branch)

    url = f"{GITLAB_API_URL}/api/v4/projects/{project_id}/merge_requests/{mr_iid}/notes"
    headers = {"PRIVATE-TOKEN": GITLAB_TOKEN}

    response = requests.post(url, headers=headers, json={"body": comment})

    return {
        "status": "ok",
        "gitlab_status": response.status_code
    }


if __name__ == "__main__":
    import uvicorn

    # Uruchomienie serwera
    uvicorn.run(app, host="0.0.0.0", port=80)



