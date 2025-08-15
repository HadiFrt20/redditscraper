from typing import List
from google.cloud import storage

_client = None


def client() -> storage.Client:
    global _client
    if _client is None:
        _client = storage.Client()
    return _client


def bucket(name: str) -> storage.Bucket:
    return client().bucket(name)


def upload_text(
    bkt: storage.Bucket, blob_name: str, text: str, content_type="text/csv"
):
    bkt.blob(blob_name).upload_from_string(text, content_type=content_type)


def exists(bkt: storage.Bucket, blob_name: str) -> bool:
    return bkt.blob(blob_name).exists()


def compose(bkt: storage.Bucket, sources: List[str], dest: str):
    bkt.blob(dest).compose([bkt.blob(s) for s in sources])


def compose_many(bkt: storage.Bucket, sources: List[str], dest: str, tmp_prefix: str):
    """Compose >32 sources by composing in stages (GCS limit is 32 per compose)."""
    if not sources:
        upload_text(bkt, dest, "")
        return
    if len(sources) <= 32:
        compose(bkt, sources, dest)
        return
    stage = 0
    current = sources
    while len(current) > 32:
        outs = []
        for i in range(0, len(current), 32):
            chunk = current[i : i + 32]
            tmp = f"{tmp_prefix}/compose-s{stage}-{i//32}.csv"
            compose(bkt, chunk, tmp)
            outs.append(tmp)
        current = outs
        stage += 1
    compose(bkt, current, dest)
