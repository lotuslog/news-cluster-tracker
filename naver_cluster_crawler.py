"""
네이버 뉴스 클러스터 크롤러 — 이벤트 기반 버전
- 테이블: article_master / cluster_master / cluster_article_events
- 적재 방식: 진입(enter) / 이탈(exit) 이벤트만 append (스냅샷 방식 폐기)
- 중복 방지: 현재 활성 기사 목록을 BQ에서 조회 후 diff
- 로그: stdout (Actions 콘솔에서 바로 확인)
"""

import re
import time
import logging
import sys
import os
import json
from datetime import datetime
from dataclasses import dataclass

from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
SECTIONS = {
    "정치":      100,
    "경제":      101,
    "사회":      102,
    "생활/문화": 103,
    "세계":      104,
    "IT/과학":   105,
}

BASE_URL    = "https://news.naver.com/section/{section_id}"
CLUSTER_URL = "https://news.naver.com/cluster/{cluster_id}/section/{section_id}"

GCP_SA_JSON    = os.environ["GCP_SA_JSON"]
BQ_PROJECT     = os.environ["BQ_PROJECT"]
BQ_DATASET     = os.environ.get("BQ_DATASET", "naver_cluster")
TARGET_SECTION = os.environ.get("TARGET_SECTION")  # 예: "정치,100"

# 테이블명
TBL_ARTICLE = "article_master"
TBL_CLUSTER = "cluster_master"
TBL_EVENTS  = "cluster_article_events"

HEADLESS              = True
PAGE_TIMEOUT          = 20_000
CLICK_DELAY           = 1.0
BETWEEN_CLUSTER_DELAY = 1.2

# ─────────────────────────────────────────────
# [추가] URL 정형화 유틸 
# ─────────────────────────────────────────────
def normalize_url(url: str) -> str:
    """URL에서 쿼리스트링 및 불필요한 스키마를 제거하여 중복 매칭 방지
    - mnews(모바일) 경로를 PC 경로로 통일 (같은 기사가 두 형식으로 잡히는 문제 방지)
    """
    if not url:
        return ""
    if url.startswith("/"):
        url = f"https://news.naver.com{url}"
    # 쿼리스트링(?), 앵커(#) 제거
    url = url.split("?")[0].split("#")[0]
    # mnews 경로 제거 → PC 버전 경로로 통일
    # 예: https://n.news.naver.com/mnews/article/277/0005778853
    #  →  https://n.news.naver.com/article/277/0005778853
    url = url.replace("/mnews/article/", "/article/")
    return url.strip()

# ─────────────────────────────────────────────
# 로깅
# ─────────────────────────────────────────────
def get_logger(name: str = "crawler") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                datefmt="%Y-%m-%d %H:%M:%S")
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger

log = get_logger()


# ─────────────────────────────────────────────
# 데이터 구조
# ─────────────────────────────────────────────
@dataclass
class ArticleMasterRow:
    article_url:   str
    article_title: str
    press:         str
    first_seen_at: str  # TIMESTAMP

@dataclass
class ClusterMasterRow:
    cluster_id:         str
    cluster_title:      str
    section:            str
    cluster_created_at: str
    first_seen_at:      str  # TIMESTAMP

@dataclass
class ClusterArticleEvent:
    cluster_id:   str
    article_url:  str
    event_type:   str        # "enter" | "exit"
    event_at:     str        # TIMESTAMP
    initial_rank: int | None # enter 시에만, exit는 None


# ─────────────────────────────────────────────
# BigQuery 스키마
# ─────────────────────────────────────────────
SCHEMA_ARTICLE_MASTER = [
    bigquery.SchemaField("article_url",   "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("article_title", "STRING"),
    bigquery.SchemaField("press",         "STRING"),
    bigquery.SchemaField("first_seen_at", "TIMESTAMP", mode="REQUIRED"),
]

SCHEMA_CLUSTER_MASTER = [
    bigquery.SchemaField("cluster_id",         "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("cluster_title",      "STRING"),
    bigquery.SchemaField("section",            "STRING"),
    bigquery.SchemaField("cluster_created_at", "STRING"),
    bigquery.SchemaField("first_seen_at",      "TIMESTAMP", mode="REQUIRED"),
]

SCHEMA_CLUSTER_ARTICLE_EVENTS = [
    bigquery.SchemaField("cluster_id",   "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("article_url",  "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("event_type",   "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("event_at",     "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("initial_rank", "INTEGER"),  # NULLABLE
]


# ─────────────────────────────────────────────
# BigQuery 연결 및 초기화
# ─────────────────────────────────────────────
def get_bq_client() -> bigquery.Client:
    sa_info = json.loads(GCP_SA_JSON)
    creds = Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=BQ_PROJECT, credentials=creds)


def ensure_tables(client: bigquery.Client) -> dict[str, str]:
    """
    데이터셋 + 테이블 3개 없으면 자동 생성
    반환: { 테이블명: full_table_id }
    """
    # 데이터셋
    dataset_ref = bigquery.Dataset(f"{BQ_PROJECT}.{BQ_DATASET}")
    dataset_ref.location = "asia-northeast3"
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)
        log.info(f"데이터셋 생성: {BQ_DATASET}")

    table_configs = [
        (TBL_ARTICLE, SCHEMA_ARTICLE_MASTER,           None),
        (TBL_CLUSTER, SCHEMA_CLUSTER_MASTER,           None),
        (TBL_EVENTS,  SCHEMA_CLUSTER_ARTICLE_EVENTS,   "event_at"),  # 파티션
    ]

    table_ids = {}
    for tbl_name, schema, partition_field in table_configs:
        full_id = f"{BQ_PROJECT}.{BQ_DATASET}.{tbl_name}"
        try:
            client.get_table(full_id)
        except Exception:
            tbl = bigquery.Table(full_id, schema=schema)
            if partition_field:
                tbl.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field,
                )
            client.create_table(tbl, exists_ok=True)
            log.info(f"테이블 생성: {full_id}")
        table_ids[tbl_name] = full_id

    return table_ids


# ─────────────────────────────────────────────
# 상태 조회 — 현재 활성 기사 / 등록된 마스터
# ─────────────────────────────────────────────
def fetch_active_articles(client: bigquery.Client,
                          events_id: str,
                          section: str) -> dict[tuple[str, str], dict]:
    """
    현재 활성 상태(enter가 있고 exit가 없는) 기사를 반환
    반환: { (cluster_id, article_url): { initial_rank } }

    [수정] key를 article_url 단독이 아니라 (cluster_id, article_url) 튜플로 변경.
    같은 기사가 여러 클러스터에 동시에 속하는 경우(실제로 빈번함)가 article_url
    단독 key에서는 서로 다른 클러스터의 활성 상태를 한 슬롯에서 덮어써서,
    한 클러스터에서 exit된 게 다른 클러스터의 활성 상태에 가려지거나
    반대로 같은 exit가 반복 기록되는 문제가 있었음.

    섹션 필터링은 cluster_master JOIN으로 처리
    (섹션별 병렬 실행 시 다른 섹션 기사를 건드리지 않기 위해)
    """
    query = f"""
        WITH last_event AS (
            SELECT
                e.cluster_id,
                e.article_url,
                e.event_type,
                e.initial_rank,
                ROW_NUMBER() OVER (
                    PARTITION BY e.cluster_id, e.article_url
                    ORDER BY e.event_at DESC
                ) AS rn
            FROM `{events_id}` e
            JOIN `{BQ_PROJECT}.{BQ_DATASET}.{TBL_CLUSTER}` c
              ON e.cluster_id = c.cluster_id
            WHERE c.section = '{section}'
              AND e.event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        )
        SELECT cluster_id, article_url, initial_rank
        FROM last_event
        WHERE rn = 1 AND event_type = 'enter'
    """
    try:
        rows = client.query(query).result()
        active = {
            (row.cluster_id, row.article_url): {
                "initial_rank": row.initial_rank,
            }
            for row in rows
        }
        log.info(f"현재 활성 기사 {len(active):,}건 로드 (섹션: {section})")
        return active
    except Exception as e:
        log.warning(f"활성 기사 로드 실패: {e}")
        return {}


# ─────────────────────────────────────────────
# [교체] 상태 조회 개선 (비용 최적화 버전)
# ─────────────────────────────────────────────
def chunk_list(lst: list, size: int = 500) -> list:
    """리스트를 size 단위로 분할"""
    return [lst[i:i+size] for i in range(0, len(lst), size)]


def check_existing_masters(client: bigquery.Client, urls: list[str], cluster_ids: list[str]) -> tuple[set[str], set[str]]:
    """
    [비용 최적화] Full Scan을 하지 않고, 이번에 크롤링된 ID들만 타겟팅하여 존재 여부 확인
    URL이 500개 초과 시 청크 단위로 분할 조회
    """
    existing_urls = set()
    existing_clusters = set()

    if urls:
        for chunk in chunk_list(urls, 500):
            url_list_str = ", ".join(f"'{u}'" for u in chunk)
            query_art = f"SELECT article_url FROM `{BQ_PROJECT}.{BQ_DATASET}.{TBL_ARTICLE}` WHERE article_url IN ({url_list_str})"
            try:
                res = client.query(query_art).result()
                existing_urls.update(row.article_url for row in res)
            except Exception as e:
                log.warning(f"article_master 체크 실패: {e}")

    if cluster_ids:
        for chunk in chunk_list(cluster_ids, 500):
            cluster_list_str = ", ".join(f"'{c}'" for c in chunk)
            query_cls = f"SELECT cluster_id FROM `{BQ_PROJECT}.{BQ_DATASET}.{TBL_CLUSTER}` WHERE cluster_id IN ({cluster_list_str})"
            try:
                res = client.query(query_cls).result()
                existing_clusters.update(row.cluster_id for row in res)
            except Exception as e:
                log.warning(f"cluster_master 체크 실패: {e}")

    return existing_urls, existing_clusters

# ─────────────────────────────────────────────
# BigQuery INSERT 헬퍼
# ─────────────────────────────────────────────
def bq_insert(client: bigquery.Client, table_id: str,
              schema: list, rows: list[dict]) -> int:
    if not rows:
        return 0
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    if job.errors:
        log.error(f"BigQuery insert 에러 ({table_id}): {job.errors}")
        return 0
    log.info(f"INSERT {len(rows)}행 → {table_id.split('.')[-1]}")
    return len(rows)


# ─────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────
def parse_cluster_created_at(cluster_id: str) -> str:
    m = re.match(r"c_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})_", cluster_id)
    if m:
        y, mo, d, h, mi = m.groups()
        return f"{y}-{mo}-{d} {h}:{mi}"
    return ""


# ─────────────────────────────────────────────
# 크롤러 핵심 로직 (파싱은 기존과 동일)
# ─────────────────────────────────────────────
def crawl_section(page, section_name: str, section_id: int) -> list[dict]:
    """
    섹션 내 전체 클러스터 크롤링
    반환: [ { cluster_id, cluster_title, article_url, article_title, press, rank }, ... ]
    """
    results = []
    url = BASE_URL.format(section_id=section_id)
    log.info(f"[{section_name}] 섹션 페이지 접속: {url}")

    try:
        page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        time.sleep(1.5)
    except PlaywrightTimeout:
        log.error(f"[{section_name}] 섹션 페이지 타임아웃")
        return results

    soup = BeautifulSoup(page.content(), "html.parser")
    cluster_buttons = soup.select("a.sa_text_cluster")
    if not cluster_buttons:
        cluster_buttons = soup.select("[class*='cluster']")
    log.info(f"[{section_name}] 클러스터 버튼 {len(cluster_buttons)}개 발견")

    seen = set()
    unique_clusters = []
    for btn in cluster_buttons:
        href = btn.get("href", "")
        m = re.search(r"/cluster/(c_\w+)/", href)
        if m:
            cid = m.group(1)
            if cid not in seen:
                seen.add(cid)
                full_url = f"https://news.naver.com{href}" if href.startswith("/") else href
                unique_clusters.append((cid, full_url))

    log.info(f"[{section_name}] 고유 클러스터 {len(unique_clusters)}개")

    for idx, (cluster_id, cluster_url) in enumerate(unique_clusters):
        log.info(f"  [{idx+1}/{len(unique_clusters)}] {cluster_id}")
        rows = crawl_cluster(page, cluster_id, cluster_url, section_name)
        results.extend(rows)
        time.sleep(BETWEEN_CLUSTER_DELAY)

    return results


def crawl_cluster(page, cluster_id: str, cluster_url: str,
                  section_name: str) -> list[dict]:
    rows = []

    try:
        page.goto(cluster_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        time.sleep(CLICK_DELAY)
    except PlaywrightTimeout:
        log.warning(f"  타임아웃: {cluster_id}")
        return rows

    try:
        more_btn = page.locator("a:has-text('기사 더보기'), button:has-text('기사 더보기')")
        if more_btn.count() > 0:
            more_btn.first.click()
            time.sleep(CLICK_DELAY)
    except Exception:
        pass

    soup = BeautifulSoup(page.content(), "html.parser")

    # 클러스터 제목 파싱
    cluster_title = ""
    title_el = soup.select_one("h2.section_cluster_summary_title")
    if title_el:
        spans = title_el.select("span.section_cluster_summary_text")
        parts = [s.get_text(strip=True) for s in spans if s.get_text(strip=True)]
        cluster_title = " · ".join(parts)
    if not cluster_title:
        title_spans = soup.select("h2.section_cluster_topic span.section_cluster_sub_topic")
        if title_spans:
            parts = [s.get_text(strip=True) for s in title_spans if s.get_text(strip=True)]
            cluster_title = " · ".join(parts)
    if not cluster_title:
        h2 = soup.select_one("h2.section_cluster_topic")
        if h2:
            cluster_title = h2.get_text(strip=True)

    cluster_created_at = parse_cluster_created_at(cluster_id)

    # 기사 목록 파싱
    article_items = soup.select("ul.sa_list li.sa_item")
    if not article_items:
        article_items = soup.select("li.sa_item")
    if not article_items:
        article_items = soup.select("li:has(a[href*='n.news.naver.com'])")

    log.info(f"  기사 {len(article_items)}건 ({cluster_title[:20] if cluster_title else cluster_id})")

    parsed_articles = []
    for rank, item in enumerate(article_items, start=1):
        press_el = item.select_one("div.sa_text_press")
        press = press_el.get_text(strip=True) if press_el else ""

        link_el = item.select_one("a.sa_text_title") or \
                  item.select_one("a[href*='n.news.naver.com']")
        if not link_el:
            continue

        raw_url = link_el.get("href", "")
        article_url = normalize_url(raw_url)
        if not article_url:
            continue

        title_el = link_el.select_one("strong.sa_text_strong")
        article_title = title_el.get_text(strip=True) if title_el \
                        else link_el.get_text(strip=True)

        parsed_articles.append({
            "rank":          rank,
            "article_url":   article_url,
            "article_title": article_title,
            "press":         press,
        })

    # [수정] cluster_title이 비어있거나(예: 정치 섹션 일부) "관련 뉴스"처럼 의미 없는
    # 플레이스홀더인 경우, 첫 진입(rank=1) 기사의 article_title로 대체한다.
    PLACEHOLDER_TITLES = {"관련 뉴스", "관련뉴스"}
    if (not cluster_title or cluster_title.strip() in PLACEHOLDER_TITLES) and parsed_articles:
        original_title = cluster_title
        first_article = next((a for a in parsed_articles if a["rank"] == 1), None)
        if first_article:
            cluster_title = first_article["article_title"]
            log.info(
                f"  cluster_title('{original_title}')이 비어있거나 플레이스홀더라 "
                f"rank=1 기사 제목으로 대체: {cluster_title[:30]}"
            )

    for a in parsed_articles:
        rows.append({
            "cluster_id":         cluster_id,
            "cluster_title":      cluster_title,
            "cluster_created_at": cluster_created_at,
            "section":            section_name,
            "article_url":        a["article_url"],
            "article_title":      a["article_title"],
            "press":              a["press"],
            "rank":               a["rank"],
        })

    return rows


# ─────────────────────────────────────────────
# 이벤트 diff 로직
# ─────────────────────────────────────────────
def build_events(
    crawled: list[dict],                    # 이번 수집 결과
    active:  dict[tuple[str, str], dict],   # 현재 활성 기사 { (cluster_id, article_url): {...} }
    known_articles: set[str],               # article_master 등록 여부
    known_clusters: set[str],               # cluster_master 등록 여부
    now: str,                               # TIMESTAMP 문자열
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    반환: (new_articles, new_clusters, events)
    - new_articles : article_master에 INSERT할 행
    - new_clusters : cluster_master에 INSERT할 행
    - events       : cluster_article_events에 INSERT할 행 (enter + exit)

    [수정] 동일 article_url이 여러 cluster_id에 동시에 속하는 경우(실제로 1,052건
    이상 발견되어 흔한 패턴으로 확인됨)를 올바르게 다루기 위해, enter/exit 판단의
    key를 article_url 단독이 아니라 (cluster_id, article_url) 튜플로 변경했다.
    기존에는 article_url만 key로 써서 같은 기사가 클러스터 A에서는 활성, 클러스터
    B에서는 비활성이어도 하나의 슬롯에서 서로 덮어쓰며 충돌했고, 그 결과 같은
    (cluster_id, article_url) 조합에서 exit가 연속으로 반복 기록되는 버그가 있었다.
    """
    # 이번 수집에서 본 (cluster_id, article_url) 조합 — enter/exit 판단용
    # 같은 기사가 여러 클러스터에 동시에 수집되는 경우를 모두 보존한다.
    crawled_map: dict[tuple[str, str], dict] = {}
    for row in crawled:
        url = row["article_url"]
        cid = row["cluster_id"]
        if not url:
            continue
        key = (cid, url)
        if key not in crawled_map:
            crawled_map[key] = row

    # article_master 신규 등록 판단은 article_url 단독 기준 그대로 유지
    # (어느 클러스터에서 봤는지와 무관하게, 기사 자체를 처음 본 시점만 추적)
    crawled_urls_seen: dict[str, dict] = {}
    for row in crawled:
        url = row["article_url"]
        if url and url not in crawled_urls_seen:
            crawled_urls_seen[url] = row

    new_articles: list[dict] = []
    new_clusters: list[dict] = []
    events: list[dict]       = []

    # ── 신규 클러스터 등록
    seen_cluster_ids = {row["cluster_id"] for row in crawled}
    for cid in seen_cluster_ids:
        if cid not in known_clusters:
            # crawled에서 해당 cluster_id의 첫 번째 row로 메타 추출
            meta = next(r for r in crawled if r["cluster_id"] == cid)
            new_clusters.append({
                "cluster_id":         cid,
                "cluster_title":      meta["cluster_title"],
                "section":            meta["section"],
                "cluster_created_at": meta["cluster_created_at"],
                "first_seen_at":      now,
            })
            known_clusters.add(cid)

    # ── 신규 기사 등록 (article_url 단독 기준)
    for url, row in crawled_urls_seen.items():
        if url not in known_articles:
            new_articles.append({
                "article_url":   url,
                "article_title": row["article_title"],
                "press":         row["press"],
                "first_seen_at": now,
            })
            known_articles.add(url)

    # ── enter 이벤트: (cluster_id, article_url) 조합이 이전에 활성이 아니었던 경우만
    for (cid, url), row in crawled_map.items():
        if (cid, url) not in active:
            events.append({
                "cluster_id":   cid,
                "article_url":  url,
                "event_type":   "enter",
                "event_at":     now,
                "initial_rank": row["rank"],
            })

    # ── exit 이벤트: 이전에 활성이었던 (cluster_id, article_url) 조합이
    #    이번 수집에서 같은 클러스터 안에 더 이상 보이지 않는 경우만
    for (cid, url), meta in active.items():
        if (cid, url) not in crawled_map:
            events.append({
                "cluster_id":   cid,
                "article_url":  url,
                "event_type":   "exit",
                "event_at":     now,
                "initial_rank": None,
            })

    enter_cnt = sum(1 for e in events if e["event_type"] == "enter")
    exit_cnt  = sum(1 for e in events if e["event_type"] == "exit")
    log.info(f"이벤트 — enter: {enter_cnt} / exit: {exit_cnt}")
    log.info(f"신규 — article_master: {len(new_articles)} / cluster_master: {len(new_clusters)}")

    return new_articles, new_clusters, events


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def main():
    import pytz
    KST = pytz.timezone("Asia/Seoul")
    start = datetime.now(KST)
    now_str = start.strftime("%Y-%m-%d %H:%M:%S")

    if TARGET_SECTION:
        name, sid = TARGET_SECTION.split(",")
        sections = {name: int(sid)}
    else:
        sections = SECTIONS

    log.info("=" * 60)
    log.info(f"크롤러 시작: {now_str} KST")
    log.info(f"실행 섹션: {list(sections.keys())}")
    log.info("=" * 60)

    # BigQuery 연결 + 테이블 준비
    client    = get_bq_client()
    table_ids = ensure_tables(client)

    article_id = table_ids[TBL_ARTICLE]
    cluster_id = table_ids[TBL_CLUSTER]
    events_id  = table_ids[TBL_EVENTS]

    # [변경] 기존의 테이블 전체 로드(Full Scan) 방식을 폐기하고 섹션별 루프 내에서 처리하도록 변경합니다.
    total_enter = total_exit = 0
    had_error = False  # [추가] 섹션 처리 중 예외가 한 번이라도 발생했는지 추적

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="ko-KR",
        )
        page = context.new_page()
        page.set_default_timeout(PAGE_TIMEOUT)

        for section_name, section_id in sections.items():
            try:
                log.info(f"\n{'─'*40}")
                log.info(f"[{section_name}] 크롤링 시작")

                # 1. 이번 수집
                crawled = crawl_section(page, section_name, section_id)
                log.info(f"[{section_name}] 수집: {len(crawled)}건")

                # [안전장치] 수집된 데이터가 아예 없으면 네트워크 오류 또는 구조 변경일 수 있으므로 
                # 활성 기사가 통째로 exit 처리되는 참사를 막기 위해 이 섹션은 스킵합니다.
                if not crawled:
                    log.warning(f"[{section_name}] 수집된 기사가 없어 데이터 처리를 스킵합니다.")
                    continue

                # 2. [비용 최적화] 이번에 수집된 대상 목록만 쿼리하여 마스터에 존재하는지 체크 (Full Scan 방지)
                crawled_urls = [r["article_url"] for r in crawled if r["article_url"]]
                crawled_clusters = list({r["cluster_id"] for r in crawled})
                
                known_articles, known_clusters = check_existing_masters(
                    client, crawled_urls, crawled_clusters
                )

                # 3. 현재 활성 기사 조회 (섹션 단위)
                active = fetch_active_articles(client, events_id, section_name)

                # 4. diff → 이벤트 생성
                new_articles, new_clusters, events = build_events(
                    crawled, active, known_articles, known_clusters, now_str
                )

                # 5. BigQuery INSERT
                bq_insert(client, cluster_id, SCHEMA_CLUSTER_MASTER, new_clusters)
                bq_insert(client, article_id, SCHEMA_ARTICLE_MASTER, new_articles)
                bq_insert(client, events_id,  SCHEMA_CLUSTER_ARTICLE_EVENTS, events)

                total_enter += sum(1 for e in events if e["event_type"] == "enter")
                total_exit  += sum(1 for e in events if e["event_type"] == "exit")

            except Exception as e:
                log.error(f"[{section_name}] 예외 발생: {e}", exc_info=True)
                had_error = True  # [추가] 실패를 기록 — 워크플로우가 조용히 성공 처리되는 것을 방지
                continue

        browser.close()

    elapsed = (datetime.now(KST) - start).seconds
    log.info("\n" + "=" * 60)
    log.info(f"완료: enter {total_enter}건 / exit {total_exit}건 / 소요 {elapsed}초")
    log.info("=" * 60)

    # [추가] 섹션 처리 중 예외가 있었다면, GitHub Actions가 이 실행을 "실패"로 인식하도록
    # 명시적으로 비정상 종료한다. 이게 없으면 try/except의 continue로 인해
    # main()이 끝까지 정상 실행되어 exit code 0(성공)으로 보고되고,
    # 실패 메일 알림(notify job)이 영원히 트리거되지 않는다.
    if had_error:
        log.error("하나 이상의 섹션에서 예외가 발생하여 비정상 종료(exit 1) 처리합니다.")
        sys.exit(1)


if __name__ == "__main__":
    main()
