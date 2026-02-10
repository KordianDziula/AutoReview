from pydantic import BaseModel, Field
import os
from typing import List, Literal
from langchain_openai import ChatOpenAI

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

LLM = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0,
    api_key=OPENAI_API_KEY)


class DependencyMap(BaseModel):
    file_path: str
    defines: List[str] = Field(description="Obiekty tworzone w tym pliku (CREATE TABLE/VIEW/PROCEDURE)")
    depends_on: List[str] = Field(description="Obiekty używane/czytane w tym pliku (FROM, JOIN)")


class CodeReviewResult(BaseModel):
    severity: Literal["CRITICAL", "WARNING", "INFO"] = Field(
        description="Waga problemu. CRITICAL blokuje wdrożenie.")
    description: str = Field(description="Techniczny opis błędu lub ryzyka.")


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

    return LLM.with_structured_output(DependencyMap) \
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
        llm_result = LLM.with_structured_output(CodeReviewResult) \
            .invoke(prompt)

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

    return {"target_file_path": target_file["path"], "comment": LLM.invoke(prompt).content}
