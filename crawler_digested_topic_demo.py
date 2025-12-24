import asyncio
import datetime
import json
from typing import Any, Dict, Optional

from crawlee import Request
from crawlee._types import ConcurrencySettings
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext

from models.base import db
from models.sc_topic import SCTopic, ensure_sc_topic_schema

MAX_REQUESTS_PER_MINUTE = 20
BASE_URL = "https://scys.com/shengcai-web/client/homePage/searchTopic"
SC_TIMEZONE = datetime.timezone(datetime.timedelta(hours=8))  # Asia/Shanghai


def build_headers() -> Dict[str, str]:
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en,en-US;q=0.9,en-GB;q=0.8,zh-CN;q=0.7,zh;q=0.6",
        "content-type": "application/json",
        "dnt": "1",
        "origin": "https://scys.com",
        "priority": "u=1, i",
        "sec-ch-ua": '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
        "x-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VyIjpudWxsLCJ1c2VyX2lkIjozNjQyNzksIm5hbWUiOiJhc3RvbmUiLCJ4cV9pZCI6MzQ5MCwibnVtYmVyIjo3MjcsInhxX2dtdF9leHBpcmUiOjE3NjU0NTgwMDAsInhxX2dtdF91cGRhdGUiOjE3NjUxOTE1MzIsInRva2VuX2V4cGlyZSI6MTc2NjQwMTEzMiwiYXZhdGFyIjoiaHR0cHM6Ly9zZWFyY2gwMS5zaGVuZ2NhaXlvdXNodS5jb20vdXBsb2FkL2F2YXRhci9GdHVnVE1HQk83SkNkTE03WVM2QW1KdU93bzA1In0.Dph5r4SSwYSh8MjAYJeuVRkCafjw42u3hMrV06GkGNA",
        "cookie": "_qimei_uuid42=19c08123a1f1006640e0cdcbadc39dc6766717bcf1; _qimei_i_3=47c47583915959d99490ad3053d273e7ffbcf2f6440a50d7b0892f5b219a256e683731973c89e2b88faf; _qimei_h38=1ac9ceb440e0cdcbadc39dc603000003519c08; __user_token.v3=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VyIjpudWxsLCJ1c2VyX2lkIjozNjQyNzksIm5hbWUiOiJhc3RvbmUiLCJ4cV9pZCI6MzQ5MCwibnVtYmVyIjo3MjcsInhxX2dtdF9leHBpcmUiOjE3NjU0NTgwMDAsInhxX2dtdF91cGRhdGUiOjE3NjUxOTE1MzIsInRva2VuX2V4cGlyZSI6MTc2NjQwMTEzMiwiYXZhdGFyIjoiaHR0cHM6Ly9zZWFyY2gwMS5zaGVuZ2NhaXlvdXNodS5jb20vdXBsb2FkL2F2YXRhci9GdHVnVE1HQk83SkNkTE03WVM2QW1KdU93bzA1In0.Dph5r4SSwYSh8MjAYJeuVRkCafjw42u3hMrV06GkGNA; _qimei_fingerprint=705df28ec648d4dcf112a7b07a4d70d9; _qimei_i_1=71e04b83c409598894c5fa345bd173b3a3bdf6a04608008be0dc2e582593206c616334903980b1dcde95ff87",
    }


def _today_gmt_range(tz: datetime.tzinfo = SC_TIMEZONE) -> tuple[int, int]:
    today = datetime.datetime.now(tz)
    start = datetime.datetime(year=today.year, month=today.month, day=today.day, tzinfo=tz)
    end = start + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())


def build_payload(
    page_index: int = 1,
    page_size: int = 20,
    gmt_create_start: Optional[int] = None,
    gmt_create_end: Optional[int] = None,
) -> Dict[str, Any]:
    today_start, today_end = _today_gmt_range()
    if gmt_create_start is None:
        gmt_create_start = today_start
    if gmt_create_end is None:
        gmt_create_end = today_end

    return {
        "pageIndex": page_index,
        "pageSize": page_size,
        "isSimpleModel": False,
        "orderBy": "gmt_create",
        "orderDirection": "desc",
        "isDigested": True,
        # "gmtCreateStart": gmt_create_start,
        # "gmtCreateEnd": gmt_create_end,
        "pageScene": "homePage",
    }


def _normalize_payload(raw_payload: Any) -> Dict[str, Any]:
    if raw_payload is None:
        return {}
    if isinstance(raw_payload, (bytes, bytearray)):
        raw_payload = raw_payload.decode("utf-8")
    if isinstance(raw_payload, str):
        try:
            return json.loads(raw_payload)
        except json.JSONDecodeError:
            return {}
    if isinstance(raw_payload, dict):
        return raw_payload
    return {}


def extract_topic_id(item: Dict[str, Any]) -> Optional[str]:
    topic_dto = item.get("topicDTO") or {}
    candidates = [
        topic_dto.get("topicId"),
        topic_dto.get("entityId"),
        item.get("topicId"),
        item.get("entityId"),
    ]
    for candidate in candidates:
        if candidate:
            return str(candidate)
    return None


def extract_user_id(item: Dict[str, Any]) -> Optional[int]:
    topic_user = item.get("topicUserDTO") or {}
    topic_dto = item.get("topicDTO") or {}
   
    candidates = [
        topic_user.get("unionUserId"),
        topic_user.get("userId"),
        topic_dto.get("createUserId"),
        item.get("userId"),
        item.get("unionUserId"),
        item.get("authorId"),
    ]

    for candidate in candidates:
        if candidate is None:
            continue
        try:
            return int(candidate)
        except (TypeError, ValueError):
            continue
    return None


async def main() -> None:
    db.connect(reuse_if_open=True)
    ensure_sc_topic_schema()

    concurrency_settings = ConcurrencySettings(
        min_concurrency=1,
        desired_concurrency=1,
        max_concurrency=1,
        max_tasks_per_minute=MAX_REQUESTS_PER_MINUTE,
    )

    crawler = HttpCrawler(max_requests_per_crawl=100, concurrency_settings=concurrency_settings)

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        payload = _normalize_payload(context.request.payload)
        page_index = payload.get("pageIndex", 1)
        context.log.info(f"Fetching topics page {page_index}")

        content = await context.http_response.read()
        try:
            response = json.loads(content.decode("utf-8"))
        except json.JSONDecodeError:
            context.log.error("Failed to decode response for page %s", page_index)
            return

        if response.get("success") and response.get("data"):
            data = response["data"]
            items = data.get("items") or []
            saved_count = 0

            for item in items:
                topic_id = extract_topic_id(item)
                if not topic_id:
                    context.log.warning("Skipping topic without topic_id: %s", item)
                    continue

                user_id = extract_user_id(item)
                topic_payload = item.get("topicDTO") or item
                now = datetime.datetime.now()
                serialized_topic = json.dumps(topic_payload, ensure_ascii=False)
                
                topic_created_at = topic_payload.get("gmtCreate")
                topic_created_dt = None
                if topic_created_at:
                    try:
                        topic_created_dt = datetime.datetime.fromtimestamp(topic_created_at, tz=SC_TIMEZONE)
                    except (TypeError, ValueError):
                        pass

                (
                    SCTopic.insert(
                        topic_id=topic_id,
                        user_id=user_id,
                        topic_json=serialized_topic,
                        topic_created_dt=topic_created_dt,
                        topic_created_at=topic_created_at,
                        created_at=now,
                        updated_at=now,
                    )
                    .on_conflict(
                        conflict_target=[SCTopic.topic_id],
                        update={
                            SCTopic.user_id: user_id,
                            SCTopic.topic_json: serialized_topic,
                            SCTopic.updated_at: now,
                        },
                    )
                    .execute()
                )
                saved_count += 1

            context.log.info("Saved %s topics from page %s", saved_count, page_index)

            page_size = payload.get("pageSize", 20)
            if len(items) >= page_size:
                next_page = page_index + 1
                next_payload = payload.copy()
                next_payload["pageIndex"] = next_page
                next_request = Request.from_url(
                    url=context.request.url,
                    method="POST",
                    headers=dict(context.request.headers),
                    payload=json.dumps(next_payload).encode("utf-8"),
                    unique_key=f"topics_p{next_page}",
                )
                await context.add_requests([next_request])

        preview = content.decode("utf-8")[:500]
        context.log.debug("Content preview: %s...", preview)

        await context.push_data(
            {
                "url": context.request.url,
                "status": context.http_response.status_code,
                "content": content.decode("utf-8"),
            }
        )

    headers = build_headers()
    payload = build_payload()

    request = Request.from_url(
        url=BASE_URL,
        method="POST",
        headers=headers,
        payload=json.dumps(payload).encode("utf-8"),
        unique_key=f"topics_p{payload['pageIndex']}",
    )

    try:
        await crawler.run([request])
    finally:
        if not db.is_closed():
            db.close()


if __name__ == "__main__":
    asyncio.run(main())
