import asyncio
import json

from crawlee import Request
from crawlee._types import ConcurrencySettings
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext

from models.base import db
from models.sc_fans import SCFans
from models.sc_profile import SCProfile

MAX_REQUESTS_PER_MINUTE = 20


async def main() -> None:
    # Initialize database and create tables if they do not exist
    db.connect()
    db.create_tables([SCProfile, SCFans], safe=True)

    try:
        # Gather all known user_ids from sc_profile table
        user_ids_query = (
            SCProfile.select(SCProfile.user_id)
            .where(SCProfile.user_id.is_null(False))
            .order_by(SCProfile.id)
        )
        target_user_ids = [profile.user_id for profile in user_ids_query]

        if not target_user_ids:
            print("No user IDs found in sc_profile table. Nothing to crawl.")
            return

        max_requests = max(len(target_user_ids) * 5, 1000)
        concurrency_settings = ConcurrencySettings(
            min_concurrency=1,
            desired_concurrency=1,
            max_concurrency=1,
            max_tasks_per_minute=MAX_REQUESTS_PER_MINUTE,
        )
        crawler = HttpCrawler(
            max_requests_per_crawl=max_requests,
            concurrency_settings=concurrency_settings,
        )

        @crawler.router.default_handler
        async def request_handler(context: HttpCrawlingContext) -> None:
            current_payload = {}
            raw_payload = context.request.payload
            if raw_payload:
                if isinstance(raw_payload, (bytes, bytearray)):
                    raw_payload = raw_payload.decode("utf-8")
                if isinstance(raw_payload, str) and raw_payload.strip():
                    try:
                        current_payload = json.loads(raw_payload)
                    except json.JSONDecodeError:
                        context.log.warning(
                            "Failed to parse request payload for %s", context.request.url
                        )

            target_user_id = current_payload.get("targetUserId")
            context.log.info(
                f"Processing {context.request.url} for user {target_user_id} ..."
            )

            content = await context.http_response.read()
            fans_result = json.loads(content)
            print(json.dumps(fans_result, ensure_ascii=False, indent=2))

            # Save fans data to database
            if fans_result.get("success") and fans_result.get("data"):
                items = fans_result["data"].get("items", [])
                saved_count = 0

                for item in items:
                    try:
                        # Insert or update fan record
                        SCFans.insert(
                            union_user_id=item.get("unionUserId"),
                            xq_group_number=item.get("xqGroupNumber"),
                            user_name=item.get("userName"),
                            avatar=item.get("avatar"),
                            introduction=item.get("introduction"),
                            follow_status=item.get("followStatus", 0),
                        ).on_conflict(
                            conflict_target=[SCFans.union_user_id],
                            preserve=[
                                SCFans.xq_group_number,
                                SCFans.user_name,
                                SCFans.avatar,
                                SCFans.introduction,
                                SCFans.follow_status,
                            ],
                        ).execute()
                        saved_count += 1
                    except Exception as e:
                        context.log.error(
                            f"Error saving fan {item.get('unionUserId')} for user {target_user_id}: {e}"
                        )

                context.log.info(
                    f"Saved {saved_count} fans to database for user {target_user_id}"
                )

                # Check if there are more pages to fetch
                if len(items) > 0 and current_payload:
                    current_page = current_payload.get("pageIndex", 1)
                    page_size = current_payload.get("pageSize", 50)

                    # If we got a full page, there might be more data
                    if len(items) >= page_size:
                        next_page = current_page + 1
                        context.log.info(
                            f"Fetching next page {next_page} for user {target_user_id}"
                        )

                        # Create request for next page
                        next_payload = current_payload.copy()
                        next_payload["pageIndex"] = next_page

                        next_request = Request.from_url(
                            url=context.request.url,
                            method="POST",
                            headers=dict(context.request.headers),
                            payload=json.dumps(next_payload).encode("utf-8"),
                            unique_key=f"fans_{target_user_id}_p{next_page}",
                        )
                        await context.add_requests([next_request])
                    else:
                        context.log.info(
                            f"Reached last page for user {target_user_id}, no more data"
                        )

            preview = content.decode("utf-8")[:500]
            context.log.info(f"Content preview: {preview}...")

            await context.push_data(
                {
                    "url": context.request.url,
                    "status": context.http_response.status_code,
                    "content": content.decode("utf-8"),
                }
            )

        headers = {
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
            "cookie": "_qimei_uuid42=19c08123a1f1006640e0cdcbadc39dc6766717bcf1; _qimei_i_3=47c47583915959d99490ad3053d273e7ffbcf2f6440a50d7b0892f5b219a256e683731973c89e2b88faf; _qimei_h38=1ac9ceb440e0cdcbadc39dc603000003519c08; __user_token.v3=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VyIjpudWxsLCJ1c2VyX2lkIjozNjQyNzksIm5hbWUiOiJhc3RvbmUiLCJ4cV9pZCI6MzQ5MCwibnVtYmVyIjo3MjcsInhxX2dtdF9leHBpcmUiOjE3NjU0NTgwMDAsInhxX2dtdF91cGRhdGUiOjE3NjUxOTE1MzIsInRva2VuX2V4cGlyZSI6MTc2NjQwMTEzMiwiYXZhdGFyIjoiaHR0cHM6Ly9zZWFyY2gwMS5zaGVuZ2NhaXlvdXNodS5jb20vdXBsb2FkL2F2YXRhci9GdHVnVE1HQk83SkNkTE03WVM2QW1KdU93bzA1In0.Dph5r4SSwYSh8MjAYJeuVRkCafjw42u3hMrV06GkGNA; _qimei_fingerprint=705df28ec648d4dcf112a7b07a4d70d9; _qimei_i_1=5fe96b83c409598894c5fa345bd173b3a3bdf6a04608008be0dc2e582593206c616334903980b1dcdef7c4dd",
        }

        base_payload = {
            "listType": "follower",
            "pageIndex": 1,
            "pageSize": 50,
        }

        initial_requests = []
        for user_id in target_user_ids:
            payload = base_payload.copy()
            payload["targetUserId"] = user_id

            request = Request.from_url(
                url="https://scys.com/shengcai-web/client/personalCenter/getUserRelationList",
                method="POST",
                headers=headers,
                payload=json.dumps(payload).encode("utf-8"),
                unique_key=f"fans_{user_id}_p{payload['pageIndex']}",
            )
            initial_requests.append(request)

        await crawler.run(initial_requests)
    finally:
        # Close database connection
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
