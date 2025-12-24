import asyncio
import json
from typing import Any, Dict
from urllib.parse import parse_qsl, urlencode, urlparse

from crawlee import Request
from crawlee._types import ConcurrencySettings
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext

MAX_REQUESTS_PER_MINUTE = 20
BASE_URL = "https://scys.com/search/activity"
DEFAULT_QUERY_PARAMS = {"page": 1, "page_size": 20, "timeline": 1}

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en,en-US;q=0.9,en-GB;q=0.8,zh-CN;q=0.7,zh;q=0.6",
    "cookie": "_qimei_fingerprint=25ed1f094b44d8b979d578845977748d; _qimei_h38=1ac9ceb440e0cdcbadc39dc603000003519c08; _qimei_i_1=47dc7283c409598894c5fa345bd173b3a3bdf6a04608008be0dc2e582593206c616334903980b1dcde8df7e3",
    "dnt": "1",
    "priority": "u=1, i",
    "sec-ch-ua": '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
    "x-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VyIjpudWxsLCJ1c2VyX2lkIjozNjQyNzksIm5hbWUiOiJhc3RvbmUiLCJ4cV9pZCI6MzQ5MCwibnVtYmVyIjo3MjcsInhxX2dtdF9leHBpcmUiOjE3NjU0NTgwMDAsInhxX2dtdF91cGRhdGUiOjE3NjUxOTE1MzIsInRva2VuX2V4cGlyZSI6MTc2NjQwMTEzMiwiYXZhdGFyIjoiaHR0cHM6Ly9zZWFyY2gwMS5zaGVuZ2NhaXlvdXNodS5jb20vdXBsb2FkL2F2YXRhci9GdHVnVE1HQk83SkNkTE03WVM2QW1KdU93bzA1In0.Dph5r4SSwYSh8MjAYJeuVRkCafjw42u3hMrV06GkGNA",
}


def _build_initial_url() -> str:
    return f"{BASE_URL}?{urlencode(DEFAULT_QUERY_PARAMS)}"


def _parse_query_params(url: str) -> Dict[str, str]:
    parsed = urlparse(url)
    return {key: value for key, value in parse_qsl(parsed.query, keep_blank_values=True)}


def _replace_query_params(url: str, **updates: Any) -> str:
    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in updates.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = str(value)
    new_query = urlencode(params)
    return parsed._replace(query=new_query).geturl()


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


async def main() -> None:
    concurrency_settings = ConcurrencySettings(
        min_concurrency=1,
        desired_concurrency=1,
        max_concurrency=1,
        max_tasks_per_minute=MAX_REQUESTS_PER_MINUTE,
    )
    crawler = HttpCrawler(
        max_requests_per_crawl=200,
        concurrency_settings=concurrency_settings,
    )

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        query_params = _parse_query_params(context.request.url)
        current_page = _coerce_int(query_params.get("page"), DEFAULT_QUERY_PARAMS["page"])
        page_size = _coerce_int(
            query_params.get("page_size"), DEFAULT_QUERY_PARAMS["page_size"]
        )
        timeline = query_params.get("timeline", str(DEFAULT_QUERY_PARAMS["timeline"]))

        context.log.info(
            "Processing activity page %s for timeline %s", current_page, timeline
        )

        body_bytes = await context.http_response.read()
        body_text = body_bytes.decode("utf-8")

        try:
            response = json.loads(body_text)
        except json.JSONDecodeError:
            context.log.error("Failed to decode JSON for page %s", current_page)
            await context.push_data(
                {
                    "url": context.request.url,
                    "status": context.http_response.status_code,
                    "page": current_page,
                    "timeline": timeline,
                    "content": body_text,
                }
            )
            return

        data = response.get("data") or {}
        activities = data.get("activity") or []
        total_count = data.get("total")
        total_count_int = (
            _coerce_int(total_count, 0) if total_count is not None else None
        )

        context.log.info(
            "Fetched %s activities on page %s (total=%s)",
            len(activities),
            current_page,
            total_count_int if total_count_int is not None else "unknown",
        )

        if activities:
            sample_names = ", ".join(
                activity.get("name") for activity in activities[:3] if activity.get("name")
            )
            if sample_names:
                suffix = "..." if len(activities) > 3 else ""
                context.log.debug("Sample activities: %s%s", sample_names, suffix)

        has_more = False
        if total_count_int is not None:
            has_more = current_page * page_size < total_count_int
        elif len(activities) >= page_size:
            has_more = True

        if has_more:
            next_page = current_page + 1
            next_url = _replace_query_params(context.request.url, page=next_page)
            context.log.info(
                "Queueing next activity page %s for timeline %s",
                next_page,
                timeline,
            )
            next_request = Request.from_url(
                url=next_url,
                method="GET",
                headers=dict(context.request.headers),
                unique_key=f"activity_{timeline}_p{next_page}",
            )
            await context.add_requests([next_request])

        await context.push_data(
            {
                "url": context.request.url,
                "status": context.http_response.status_code,
                "page": current_page,
                "timeline": timeline,
                "activity_count": len(activities),
                "content": body_text,
            }
        )

    initial_request = Request.from_url(
        url=_build_initial_url(),
        method="GET",
        headers=HEADERS,
        unique_key=f"activity_{DEFAULT_QUERY_PARAMS['timeline']}_p{DEFAULT_QUERY_PARAMS['page']}",
    )

    await crawler.run([initial_request])


if __name__ == "__main__":
    asyncio.run(main())
