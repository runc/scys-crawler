import datetime
import json
from typing import Any

from models.sc_profile import SCProfile


def _json_dumps(value: Any) -> str | None:
    if value is None:
        return None

    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def save_profile(data: dict) -> None:
    """Persist a profile payload coming from different endpoints."""

    user_info = data.get("user") or {}
    user_id = data.get("unionUserId") or user_info.get("_id")
    if not user_id:
        return

    introduction_value: Any = data.get("introduction")
    if introduction_value is None:
        introduction_value = data.get("intro")
    if introduction_value is None:
        introduction_value = user_info.get("intro")
    if isinstance(introduction_value, (dict, list)):
        introduction_value = _json_dumps(introduction_value)
    elif introduction_value is not None:
        introduction_value = str(introduction_value)

    follow_status_value = data.get("followStatus")
    if follow_status_value is None:
        follow_status_value = user_info.get("follow_status")

    profile_data = {
        "user_id": user_id,
        "name": data.get("userName") or user_info.get("name"),
        "avatar": data.get("avatar") or user_info.get("avatar"),
        "xq_user_id": data.get("xqUserId") or user_info.get("user_id"),
        "xq_group_number": data.get("xqGroupNumber") or user_info.get("group_number"),
        "introduction": introduction_value,
        "province": data.get("province"),
        "city": data.get("city"),
        "district": data.get("district"),
        "gender": data.get("gender") or user_info.get("gender"),
        "follow_count": _coerce_int(data.get("followCount")),
        "follower_count": _coerce_int(data.get("followerCount")),
        "mutual_follow_count": _coerce_int(data.get("mutualFollowCount")),
        "total_like_and_coin_count": _coerce_int(data.get("totalLikeAndCoinCount")),
        "is_navigator": bool(data.get("isNavigator")),
        "navigator_expire_time": data.get("navigatorExpireTime"),
        "date_expire": data.get("dateExpire") or user_info.get("xq_date_expire"),
        "privacy_settings": _json_dumps(data.get("privacySettings")),
        "follow_status": _coerce_int(follow_status_value),
        "profile_json": json.dumps(data, ensure_ascii=False),
        "updated_at": datetime.datetime.now(),
    }

    SCProfile.insert(profile_data).on_conflict(
        conflict_target=[SCProfile.user_id],
        update=profile_data,
    ).execute()
