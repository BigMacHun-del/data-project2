# --------------
# 작성자 : 김대훈
# 작성목적 : project.py 비동기 수집 파이프라인에 대한 pytest 테스트
# 작성일 : 2026년 7월 15일
#
# 변경사항 내역
# 0.1 : 2026년 7월 15일 - 최초 작성
#         - 서울(lat 37.5665, lon 126.9780) 위경도로 open-meteo에서 조회한
#           현재 기온이 25도인지 검사하는 테스트 추가
#         - 25도가 아니면 실제 값을 담은 Fail 메시지 출력
# --------------

"""
project.py의 fetch_weather()를 이용해 서울 현재 기온을 실제로 조회하고
25도인지 검증하는 pytest 테스트.
"""

import asyncio

import httpx

from project import CITIES, fetch_weather

SEOUL = next(city for city in CITIES if city["name"] == "서울")


def test_seoul_temperature_is_25() -> None:
    """서울의 현재 기온이 25도인지 검사한다. 아니면 Fail 메시지를 출력한다."""

    async def _fetch() -> dict:
        async with httpx.AsyncClient() as client:
            return await fetch_weather(client, SEOUL)

    result = asyncio.run(_fetch())

    assert result["ok"], f"[FAIL] 서울 날씨 API 호출 실패: {result.get('error')}"

    temperature = result["data"]["current_weather"]["temperature"]

    assert temperature == 25, (
        f"[FAIL] 서울 현재 기온이 25도가 아닙니다. 실제 값: {temperature}도"
    )
