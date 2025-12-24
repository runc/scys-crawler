import asyncio
import json

from crawlee import Request
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext

from models.base import db
from models.sc_profile import ensure_sc_profile_schema
from services.profile_storage import save_profile


async def main() -> None:
    db.connect(reuse_if_open=True)
    ensure_sc_profile_schema()

    crawler = HttpCrawler(max_requests_per_crawl=10)

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f"Processing {context.request.url} ...")

        content = await context.http_response.read()
        response_data = json.loads(content.decode("utf-8"))

        if response_data.get("status") == 0 and "data" in response_data:
            data = response_data["data"]
            save_profile(data)
            context.log.info("Profile saved to database")
        else:
            context.log.warning(f"Invalid response data {response_data}")  

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
        "cookie": "_qimei_uuid42=19c08123a1f1006640e0cdcbadc39dc6766717bcf1; _qimei_i_3=47c47583915959d99490ad3053d273e7ffbcf2f6440a50d7b0892f5b219a256e683731973c89e2b88faf; _qimei_h38=1ac9ceb440e0cdcbadc39dc603000003519c08; __user_token.v3=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VyIjpudWxsLCJ1c2VyX2lkIjozNjQyNzksIm5hbWUiOiJhc3RvbmUiLCJ4cV9pZCI6MzQ5MCwibnVtYmVyIjo3MjcsInhxX2dtdF9leHBpcmUiOjE3NjU0NTgwMDAsInhxX2dtdF91cGRhdGUiOjE3NjUxOTE1MzIsInRva2VuX2V4cGlyZSI6MTc2NjQwMTEzMiwiYXZhdGFyIjoiaHR0cHM6Ly9zZWFyY2gwMS5zaGVuZ2NhaXlvdXNodS5jb20vdXBsb2FkL2F2YXRhci9GdHVnVE1HQk83SkNkTE03WVM2QW1KdU93bzA1In0.Dph5r4SSwYSh8MjAYJeuVRkCafjw42u3hMrV06GkGNA; _qimei_fingerprint=705df28ec648d4dcf112a7b07a4d70d9; _qimei_i_1=70c52883c409598894c5fa345bd173b3a3bdf6a04608008be0dc2e582593206c616334903980b1dcde91afc0",
    }

    # Request body with target user ID
    payload = {"targetUserId": 36397}
    
    request = Request.from_url(
        url="https://scys.com/shengcai-web/client/personalCenter/getProfileInfo",
        method="POST",
        headers=headers,
        payload=json.dumps(payload),
    )

    await crawler.run([request])


if __name__ == "__main__":
    asyncio.run(main())
