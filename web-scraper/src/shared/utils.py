from datetime import datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def job_pk(job_id: str) -> str:
    return f'job#{job_id}'


def item_sk(item_id: str) -> str:
    return f'item#{item_id}'
