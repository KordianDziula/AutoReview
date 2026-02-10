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


def format_sql_review_comment(full_holistic_review):
    if not full_holistic_review:
        return "### 🤖 AI SQL Review\nNie wykryto zmian w plikach SQL."

    # Nagłówek tematyczny
    comment_parts = [
        "## SQL Performance & Logic Analysis",
        f"**Przeanalizowane skrypty:** `{len(full_holistic_review)}`",
        "---"
    ]

    for fhr in full_holistic_review:
        path = fhr.get("target_file_path", "unknown.sql")
        review_content = fhr.get("comment", "")

        section = [
            f"<details>",
            f"<summary><b>{path}</b> (rozwiń analizę)</summary>",
            "\n",
            "#### Uwagi Agenta:",
            f"> {review_content}",
            "\n",
            "---",
            "</details>"
        ]

        comment_parts.append("\n".join(section))

    return "\n".join(comment_parts)