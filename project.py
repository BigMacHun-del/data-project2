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
# 0.2 : 2026년 7월 15일 - Weather Pydantic 스키마 및 CSV 저장/로딩 추가
# --------------

"""
1단계: 공공 API(JSON)에서 비동기 데이터 수집

- open-meteo: 위도/경도 기반 현재 기온
- timeapi.io: 타임존 기반 현재 시간

asyncio.gather()로 도시별 API 호출을 동시에 실행한다.
"""

import asyncio
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Union

import httpx
import pandas as pd
from pydantic import BaseModel, ValidationError

CITIES: list[dict[str, Any]] = [
    {"name": "서울", "lat": 37.5665, "lon": 126.9780, "tz": "Asia/Seoul"},
    {"name": "도쿄", "lat": 35.6762, "lon": 139.6503, "tz": "Asia/Tokyo"},
    {"name": "뉴욕", "lat": 40.7128, "lon": -74.0060, "tz": "America/New_York"},
    {"name": "런던", "lat": 51.5074, "lon": -0.1278, "tz": "Europe/London"},
]

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
TIME_URL = "https://timeapi.io/api/time/current/zone"

REQUEST_TIMEOUT = 10.0

CSV_PATH = Path("weather.csv")


class Weather(BaseModel):
    """도시별 현재기온·현지시각 요약 스키마."""

    도시: str
    기온: Union[float, str]
    현지시각: str


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


def format_local_time(iso_str: str | None) -> str:
    """timeapi.io의 ISO 형식 dateTime을 'MM/DD/YYYY HH:MM' 형식으로 변환한다."""
    if not iso_str:
        return "정보없음"
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%m/%d/%Y %H:%M")
    except ValueError:
        return iso_str


def build_weather_list(merged: list[dict[str, Any]]) -> list[Weather]:
    """병합된 원시 데이터를 Weather 스키마 객체 리스트로 변환한다."""
    weathers: list[Weather] = []
    for row in merged:
        temperature = row.get("temperature")
        if temperature is None:
            temperature = row.get("weather_error", "정보없음")

        local_time = format_local_time(row.get("datetime"))

        try:
            weathers.append(Weather(도시=row["city"], 기온=temperature, 현지시각=local_time))
        except ValidationError as e:
            print(f"[스키마 검증 실패] {row.get('city')}: {e}")

    return weathers


def save_weather_csv(weathers: list[Weather], path: Path = CSV_PATH) -> None:
    """Weather 객체 리스트를 CSV로 저장한다."""
    df = pd.DataFrame([w.model_dump() for w in weathers])
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[CSV 저장 완료] {path} ({len(df)}건)")


def load_weather_csv(path: Path = CSV_PATH) -> pd.DataFrame:
    """CSV를 읽어온다. 파일이 존재하지 않으면 예외를 발생시킨다."""
    if not path.exists():
        raise FileNotFoundError(f"CSV 파일이 존재하지 않습니다: {path}")
    return pd.read_csv(path)


async def main() -> list[dict[str, Any]]:
    start = time.perf_counter()
    raw_results = await collect_all(CITIES)
    elapsed = time.perf_counter() - start

    merged = merge_by_city(raw_results, CITIES)

    print(f"[비동기 수집 완료] 총 {len(raw_results)}개 요청, {elapsed:.2f}초 소요\n")
    for row in merged:
        print(row)

    weathers = build_weather_list(merged)
    save_weather_csv(weathers)

    try:
        df = load_weather_csv()
        print("\n[CSV 재로딩 결과]")
        print(df)
    except FileNotFoundError as e:
        print(f"[오류] {e}")

    return merged


if __name__ == "__main__":
    asyncio.run(main())