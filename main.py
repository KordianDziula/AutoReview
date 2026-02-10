from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Body, BackgroundTasks
import logging

from src.gitlab import fetch_hybrid_files, is_mr_creation, post_mr_comment, extract_mr_data
from src.utils import clean_obj_name, format_sql_review_comment
from src.agents import agent_dependency_mapper, agent_logic_verifier, agent_holistic_review
import uvicorn


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("gitlab-webhook")

APP = FastAPI()


def main(project_id, mr_iid, source_branch, target_branch):
    files = fetch_hybrid_files(
        project_id,
        mr_iid,
        source_branch,
        target_branch
    )

    # full_logic_review = []
    # dependency_map = []
    # for f in files:
    #     file_dependency = agent_dependency_mapper(f)
    #
    #     for defines in file_dependency.defines:
    #         dependency_map.append({"obj": clean_obj_name(defines), "file": file_dependency.file_path})
    #     for depends_on in file_dependency.depends_on:
    #         dependency_map.append({"obj": clean_obj_name(depends_on), "file": file_dependency.file_path})

    full_holistic_review = []

    changed = [f for f in files if f["is_modified"]]
    for ch in changed:
        # objs = [
        #     dep["obj"] for dep in dependency_map if dep["file"] == ch["path"]]
        #
        # dep_files = []
        # for obj in objs:
        #     dependencies = [dep["file"] for dep in dependency_map if dep["obj"] == obj]
        #     for dep in dependencies:
        #         dep_files.append(dep)
        #
        # dep_files = set(dep_files)
        # dep_files_content = []
        # for df in dep_files:
        #     content = next(f for f in files if f["path"] == df)
        #     dep_files_content.append(content)
        #
        # logic_review = agent_logic_verifier(
        #     ch, dep_files_content)

        holistic_review = agent_holistic_review(ch)
        full_holistic_review.append(holistic_review)

    final_comment = format_sql_review_comment(
        full_holistic_review)

    return final_comment


def background_logic(project_id, mr_iid, source_branch, target_branch):
    logger.info(f"MR !{mr_iid} | REVIEW START")
    try:
        comment = main(project_id, mr_iid, source_branch, target_branch)
        logger.info(f"MR !{mr_iid} | ANALYSIS DONE")

        response = post_mr_comment(
            project_id,
            mr_iid,
            comment
        )

        if response.status_code == 201:
            logger.info(f"MR !{mr_iid} | COMMENT POSTED SUCCESS")
        else:
            logger.error(f"MR !{mr_iid} | POST FAILED (HTTP {response.status_code})")

    except Exception as e:
        logger.exception(f"MR !{mr_iid} | UNEXPECTED FATAL ERROR")


@APP.post("/webhook")
def gitlab_webhook(background_tasks: BackgroundTasks, payload: dict = Body(...)):
    is_mr = is_mr_creation(payload)

    if is_mr:
        data = extract_mr_data(payload)

        background_tasks.add_task(
            background_logic,
            data["project_id"],
            data["mr_iid"],
            data["source_branch"],
            data["target_branch"]
        )

        return {"status": "accepted"}

    else:
        return {"status": "skipped"}


if __name__ == "__main__":
    uvicorn.run(APP, host="0.0.0.0", port=80)


