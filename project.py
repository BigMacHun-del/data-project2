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
# 0.3 : 2026년 7월 15일 - Weather Parquet 저장 및 부분 컬럼(도시, 기온) 읽기 추가
# 0.4 : 2026년 7월 15일 - Ruff 적용하여 정리
# 0.5 : 2026년 7월 15일 - CSV/Parquet 저장·읽기 성능 측정 및 비교 추가
# 0.6 : 2026년 7월 15일 - 함수별 설명 주석 보강, 예외 처리 보강
# 0.7 : 2026년 7월 15일 - 불필요한 print() 전부 logging으로 전환
# 0.8 : 2026년 7월 15일 - test_project.py를 이 파일로 병합
# --------------

"""
공공 API(JSON)에서 비동기 데이터 수집

- open-meteo: 위도/경도 기반 현재 기온
- timeapi.io: 타임존 기반 현재 시간

asyncio.gather()로 도시별 API 호출을 동시에 실행한다.
"""

import asyncio
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Union

import httpx
import pandas as pd
from pydantic import BaseModel, ValidationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# 수집 대상 도시 목록 (이름, 위도, 경도, 타임존)
CITIES: list[dict[str, Any]] = [
    {"name": "서울", "lat": 37.5665, "lon": 126.9780, "tz": "Asia/Seoul"},
    {"name": "도쿄", "lat": 35.6762, "lon": 139.6503, "tz": "Asia/Tokyo"},
    {"name": "뉴욕", "lat": 40.7128, "lon": -74.0060, "tz": "America/New_York"},
    {"name": "런던", "lat": 51.5074, "lon": -0.1278, "tz": "Europe/London"},
]

# 도시별로 다른 위경도/타임존을 params로 넘겨야 하므로 베이스 엔드포인트만 사용
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
TIME_URL = "https://timeapi.io/api/time/current/zone"

REQUEST_TIMEOUT = 10.0

CSV_PATH = Path("weather.csv")
PARQUET_PATH = Path("weather.parquet")


class Weather(BaseModel):
    """도시별 현재기온·현지시각 요약 스키마."""

    도시: str
    기온: Union[float, str]
    현지시각: str


async def fetch_weather(
    client: httpx.AsyncClient, city: dict[str, Any]
) -> dict[str, Any]:
    # open-meteo에서 현재 날씨(기온 등)를 가져옴
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "current_weather": "true",
    }
    try:
        resp = await client.get(WEATHER_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return {
            "city": city["name"],
            "source": "weather",
            "ok": True,
            "data": resp.json(),
        }
    except httpx.HTTPError as e:
        # 타임아웃, 연결 실패, 4xx/5xx 응답 등 HTTP 계층 오류
        return {"city": city["name"], "source": "weather", "ok": False, "error": str(e)}
    except ValueError as e:
        # resp.json() 파싱 실패 (응답 바디가 유효한 JSON이 아닌 경우)
        return {
            "city": city["name"],
            "source": "weather",
            "ok": False,
            "error": f"JSON 파싱 실패: {e}",
        }


async def fetch_time(client: httpx.AsyncClient, city: dict[str, Any]) -> dict[str, Any]:
    # timeapi.io에서 타임존 기준 현재 시간을 가져옴
    params = {"timeZone": city["tz"]}
    try:
        resp = await client.get(TIME_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return {"city": city["name"], "source": "time", "ok": True, "data": resp.json()}
    except httpx.HTTPError as e:
        return {"city": city["name"], "source": "time", "ok": False, "error": str(e)}
    except ValueError as e:
        return {
            "city": city["name"],
            "source": "time",
            "ok": False,
            "error": f"JSON 파싱 실패: {e}",
        }


async def collect_all(cities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # 모든 도시의 날씨 + 시간 정보를 동시에(concurrent) 수집
    async with httpx.AsyncClient() as client:
        tasks = []
        for city in cities:
            tasks.append(fetch_weather(client, city))
            tasks.append(fetch_time(client, city))
        # asyncio.gather()로 도시 x (weather, time) 총 8개 요청을 동시 실행
        results = await asyncio.gather(*tasks)
    return results


def merge_by_city(
    raw_results: list[dict[str, Any]], cities: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """도시별로 weather/time 결과를 하나의 레코드로 병합

    raw_results는 API가 돌려준 city 값을 그대로 사용하므로,
    cities에 없는 이름이 섞여 들어오면 KeyError가 날 수 있고
    이를 방지하기 위해 by_city에 없는 도시명은 조용히 건너뜀
    """
    by_city: dict[str, dict[str, Any]] = {
        c["name"]: {"city": c["name"], "lat": c["lat"], "lon": c["lon"], "tz": c["tz"]}
        for c in cities
    }

    for r in raw_results:
        city_name = r["city"]
        if city_name not in by_city:
            # CITIES에 정의되지 않은 도시명이 응답에 섞여 있으면 무시
            logger.warning("알 수 없는 도시명 무시: %s", city_name)
            continue

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
    # timeapi.io의 ISO 형식 dateTime을 'MM/DD/YYYY HH:MM' 형식으로 변환
    if not iso_str:
        return "정보없음"
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%m/%d/%Y %H:%M")
    except ValueError:
        # 예상 밖의 포맷이면 변환을 포기하고 원본 문자열을 그대로 반환
        return iso_str


def build_weather_list(merged: list[dict[str, Any]]) -> list[Weather]:
    # 병합된 원시 데이터를 Weather 스키마 객체 리스트로 변환
    weathers: list[Weather] = []
    for row in merged:
        temperature = row.get("temperature")
        if temperature is None:
            temperature = row.get("weather_error", "정보없음")

        local_time = format_local_time(row.get("datetime"))

        try:
            city_name = row["city"]
        except KeyError:
            logger.warning("city 키가 없는 레코드 건너뜀: %s", row)
            continue

        try:
            weathers.append(
                Weather(도시=city_name, 기온=temperature, 현지시각=local_time)
            )
        except ValidationError as e:
            logger.warning("스키마 검증 실패(%s): %s", city_name, e)

    return weathers


def save_weather_csv(weathers: list[Weather], path: Path = CSV_PATH) -> None:
    # Weather 객체 리스트를 CSV로 저장
    df = pd.DataFrame([w.model_dump() for w in weathers])
    try:
        df.to_csv(path, index=False, encoding="utf-8-sig")
    except OSError as e:
        logger.error("CSV 저장 실패(%s): %s", path, e)
        raise
    logger.info("CSV 저장 완료: %s (%d건)", path, len(df))


def load_weather_csv(path: Path = CSV_PATH) -> pd.DataFrame:
    """CSV를 읽어온다.

    파일이 존재하지 않으면 FileNotFoundError를,
    파일은 있지만 내용이 비어 있거나(EmptyDataError) 형식이
    깨져 있으면(ParserError) 각각 명확한 메시지와 함께 예외를 발생
    """
    if not path.exists():
        raise FileNotFoundError(f"CSV 파일이 존재하지 않습니다: {path}")
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError as e:
        raise ValueError(f"CSV 파일이 비어 있습니다: {path}") from e
    except pd.errors.ParserError as e:
        raise ValueError(f"CSV 파일 형식이 올바르지 않습니다: {path} ({e})") from e


def save_weather_parquet(weathers: list[Weather], path: Path = PARQUET_PATH) -> None:
    # Weather 객체 리스트를 Parquet으로 저장한다.
    df = pd.DataFrame([w.model_dump() for w in weathers])
    try:
        df.to_parquet(path, index=False)
    except OSError as e:
        logger.error("Parquet 저장 실패(%s): %s", path, e)
        raise
    logger.info("Parquet 저장 완료: %s (%d건)", path, len(df))


def load_weather_parquet(
    path: Path = PARQUET_PATH, columns: list[str] | None = None
) -> pd.DataFrame:
    """Parquet을 읽어온다. columns를 지정하면 해당 컬럼만 읽음

    파일이 존재하지 않으면 FileNotFoundError를,
    존재하지만 손상되어 파싱할 수 없으면 ValueError로 감싸서 원인 제공
    """
    if not path.exists():
        raise FileNotFoundError(f"Parquet 파일이 존재하지 않습니다: {path}")
    try:
        return pd.read_parquet(path, columns=columns)
    except Exception as e:  # noqa: BLE001 - 엔진(pyarrow 등)마다 예외 타입이 달라 광범위하게 처리
        raise ValueError(f"Parquet 파일을 읽을 수 없습니다: {path} ({e})") from e


SEOUL = next(city for city in CITIES if city["name"] == "서울")


def test_seoul_temperature_is_25() -> None:
    """서울의 현재 기온이 25도인지 검사한다. 아니면 Fail 메시지를 출력한다."""

    async def _fetch() -> dict[str, Any]:
        async with httpx.AsyncClient() as client:
            return await fetch_weather(client, SEOUL)

    result = asyncio.run(_fetch())

    assert result["ok"], f"[FAIL] 서울 날씨 API 호출 실패: {result.get('error')}"

    temperature = result["data"]["current_weather"]["temperature"]

    assert temperature == 25, (
        f"[FAIL] 서울 현재 기온이 25도가 아닙니다. 실제 값: {temperature}도"
    )


def measure(func: Any, *args: Any, **kwargs: Any) -> tuple[Any, float]:
    # 임의 함수를 실행하고 결과, 소요시간을 반환
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return result, elapsed


def compare_storage_performance(weathers: list[Weather]) -> dict[str, float]:
    # CSV/Parquet 저장(쓰기) 및 재로딩(읽기) 시간을 측정하고 비교
    timings: dict[str, float | None] = {
        "csv_write": None,
        "csv_read": None,
        "parquet_write": None,
        "parquet_read": None,
    }

    try:
        _, timings["csv_write"] = measure(save_weather_csv, weathers)
        _, timings["csv_read"] = measure(load_weather_csv)
    except (OSError, ValueError) as e:
        logger.error("CSV 성능 측정 중 실패: %s", e)

    try:
        _, timings["parquet_write"] = measure(save_weather_parquet, weathers)
        _, timings["parquet_read"] = measure(load_weather_parquet)
    except (OSError, ValueError) as e:
        logger.error("Parquet 성능 측정 중 실패: %s", e)

    def fmt(value: float | None) -> str:
        return f"{value:.6f}" if value is not None else "N/A"

    table = (
        f"{'항목':<10}{'CSV(초)':>8}{'Parquet(초)':>14}\n"
        f"{'쓰기':<10}{fmt(timings['csv_write']):>8}{fmt(timings['parquet_write']):>14}\n"
        f"{'읽기':<10}{fmt(timings['csv_read']):>8}{fmt(timings['parquet_read']):>14}"
    )
    logger.info("CSV vs Parquet 저장/읽기 성능 비교\n%s", table)

    if timings["csv_write"] is not None and timings["parquet_write"] is not None:
        faster_write = (
            "CSV" if timings["csv_write"] < timings["parquet_write"] else "Parquet"
        )
        logger.info("쓰기 더 빠른 포맷 : %s", faster_write)
    else:
        logger.warning("쓰기 비교 불가 (측정 실패)")

    if timings["csv_read"] is not None and timings["parquet_read"] is not None:
        faster_read = (
            "CSV" if timings["csv_read"] < timings["parquet_read"] else "Parquet"
        )
        logger.info("읽기 더 빠른 포맷 : %s", faster_read)
    else:
        logger.warning("읽기 비교 불가 (측정 실패)")

    return timings


async def main() -> list[dict[str, Any]]:
    # 전체 파이프라인 실행: 수집 -> 스키마 검증 -> CSV/Parquet 저장·재로딩 -> 성능 비교.
    start = time.perf_counter()
    raw_results = await collect_all(CITIES)
    elapsed = time.perf_counter() - start

    merged = merge_by_city(raw_results, CITIES)

    logger.info(
        "비동기 수집 완료: 총 %d개 요청, %.2f초 소요", len(raw_results), elapsed
    )
    for row in merged:
        logger.info("%s", row)

    weathers = build_weather_list(merged)

    try:
        save_weather_csv(weathers)
        df = load_weather_csv()
        logger.info("CSV 재로딩 결과\n%s", df)
    except (FileNotFoundError, OSError, ValueError) as e:
        logger.error("%s", e)

    try:
        save_weather_parquet(weathers)
        df_parquet = load_weather_parquet(columns=["도시", "기온"])
        logger.info("Parquet 재로딩 결과 - 도시/기온만\n%s", df_parquet)
    except (FileNotFoundError, OSError, ValueError) as e:
        logger.error("%s", e)

    compare_storage_performance(weathers)

    return merged


if __name__ == "__main__":
    asyncio.run(main())