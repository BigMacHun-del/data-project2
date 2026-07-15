# --------------
# 작성자 : 김대훈
# 작성목적 : 공공 API(JSON) 비동기 데이터 수집 미니 파이프라인
# 작성일 : 2026년 7월 15일
#
# open-meteo(위경도 기반 현재기온)와 timeapi.io(타임존 기반 현재시간)에서
# asyncio + httpx로 도시별 데이터를 동시에 수집하는 스크립트입니다.
#
# 변경사항 내역
# 0.1 : 2026년 7월 15일 - 최초 작성
#         - CITIES(서울/도쿄/뉴욕/런던) 정의
#         - fetch_weather(), fetch_time() 비동기 호출 함수 작성
#         - collect_all()에서 asyncio.gather()로 도시 x API 요청 동시 실행
#         - merge_by_city()로 도시별 weather/time 결과 병합
#         - 개별 요청 실패 시 예외를 잡아 ok:False로 반환(파이프라인 중단 방지)
#         - main()에서 실행 시간 측정 및 결과 출력
# --------------

"""
1단계: 공공 API(JSON)에서 비동기 데이터 수집

- open-meteo: 위도/경도 기반 현재 기온
- timeapi.io: 타임존 기반 현재 시간

asyncio.gather()로 도시별 API 호출을 동시에 실행한다.
"""

import asyncio
import time
from typing import Any

import httpx

CITIES: list[dict[str, Any]] = [
    {"name": "서울", "lat": 37.5665, "lon": 126.9780, "tz": "Asia/Seoul"},
    {"name": "도쿄", "lat": 35.6762, "lon": 139.6503, "tz": "Asia/Tokyo"},
    {"name": "뉴욕", "lat": 40.7128, "lon": -74.0060, "tz": "America/New_York"},
    {"name": "런던", "lat": 51.5074, "lon": -0.1278, "tz": "Europe/London"},
]

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
TIME_URL = "https://timeapi.io/api/time/current/zone"

REQUEST_TIMEOUT = 10.0


async def fetch_weather(client: httpx.AsyncClient, city: dict[str, Any]) -> dict[str, Any]:
    """open-meteo에서 현재 날씨(기온 등)를 가져온다."""
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "current_weather": "true",
    }
    try:
        resp = await client.get(WEATHER_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return {"city": city["name"], "source": "weather", "ok": True, "data": resp.json()}
    except httpx.HTTPError as e:
        return {"city": city["name"], "source": "weather", "ok": False, "error": str(e)}


async def fetch_time(client: httpx.AsyncClient, city: dict[str, Any]) -> dict[str, Any]:
    """timeapi.io에서 타임존 기준 현재 시간을 가져온다."""
    params = {"timeZone": city["tz"]}
    try:
        resp = await client.get(TIME_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return {"city": city["name"], "source": "time", "ok": True, "data": resp.json()}
    except httpx.HTTPError as e:
        return {"city": city["name"], "source": "time", "ok": False, "error": str(e)}


async def collect_all(cities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """모든 도시의 날씨 + 시간 정보를 동시에(concurrent) 수집한다."""
    async with httpx.AsyncClient() as client:
        tasks = []
        for city in cities:
            tasks.append(fetch_weather(client, city))
            tasks.append(fetch_time(client, city))
        # asyncio.gather()로 도시 x (weather, time) 총 8개 요청을 동시 실행
        results = await asyncio.gather(*tasks)
    return results


def merge_by_city(raw_results: list[dict[str, Any]], cities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """도시별로 weather/time 결과를 하나의 레코드로 병합한다."""
    by_city: dict[str, dict[str, Any]] = {c["name"]: {"city": c["name"], "lat": c["lat"], "lon": c["lon"], "tz": c["tz"]} for c in cities}

    for r in raw_results:
        city_name = r["city"]
        if r["source"] == "weather":
            if r["ok"]:
                cw = r["data"].get("current_weather", {})
                by_city[city_name]["temperature"] = cw.get("temperature")
                by_city[city_name]["weather_time"] = cw.get("time")
                by_city[city_name]["windspeed"] = cw.get("windspeed")
            else:
                by_city[city_name]["weather_error"] = r["error"]
        elif r["source"] == "time":
            if r["ok"]:
                d = r["data"]
                by_city[city_name]["datetime"] = d.get("dateTime")
                by_city[city_name]["day_of_week"] = d.get("dayOfWeek")
            else:
                by_city[city_name]["time_error"] = r["error"]

    return list(by_city.values())


async def main() -> list[dict[str, Any]]:
    start = time.perf_counter()
    raw_results = await collect_all(CITIES)
    elapsed = time.perf_counter() - start

    merged = merge_by_city(raw_results, CITIES)

    print(f"[비동기 수집 완료] 총 {len(raw_results)}개 요청, {elapsed:.2f}초 소요\n")
    for row in merged:
        print(row)

    return merged


if __name__ == "__main__":
    asyncio.run(main())