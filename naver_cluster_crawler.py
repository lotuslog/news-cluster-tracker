"""
네이버 뉴스 클러스터 크롤러 — 최종 버전 (결제 계정 연결 전제)
- 테이블: article_master / cluster_master / article_cluster_link
- 적재 방식: (cluster_id, article_url) 조합을 MERGE로 UPSERT
  - 신규 조합이면 first_seen_at = last_seen_at = 지금, INSERT
  - 이미 있는 조합이면 last_seen_at만 지금으로 UPDATE
- [설계 배경] 네이버 클러스터 노출이 사용자/세션/새로고침마다 달라질 수 있어,
  "이번에 안 보였다 = 클러스터에서 빠졌다"는 가정(enter/exit 이벤트 모델)이
  실제 도메인과 맞지 않았다. exit 추적을 폐기하고 "마지막으로 본 시점"만
  갱신하는 구조로 전환했다.
- [전제] 이 버전은 결제 계정이 연결된 GCP 프로젝트에서 동작한다. MERGE(DML)를
  직접 사용하므로, 결제 계정이 없는 프로젝트(BigQuery 무료 등급)에서는
  "DML queries are not allowed in the free tier" 에러로 실패한다.
- 시간 컬럼은 DATETIME(KST, 타임존 정보 없는 문자열)으로 저장한다.
- "기사 더보기"는 버튼이 더 이상 보이지 않을 때까지 반복 클릭해, 대형
  클러스터의 전체 기사 목록을 누락 없이 가져온다.
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
TBL_LINK    = "article_cluster_link"

HEADLESS              = True
PAGE_TIMEOUT          = 20_000
CLICK_DELAY           = 1.0
BETWEEN_CLUSTER_DELAY = 1.2
MAX_MORE_CLICKS        = 15     # "기사 더보기" 최대 반복 클릭 횟수 (대형 클러스터 전체 로드 보장용 안전 상한)
MORE_BTN_CLICK_TIMEOUT = 3_000  # 더보기 버튼 클릭 전용 타임아웃(ms) — PAGE_TIMEOUT(20초)보다 훨씬 짧게
                                # 줘서, 버튼이 안 보이는 경우(더 보여줄 기사가 없음) 빠르게 다음으로 넘어간다

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
    first_seen_at: str  # DATETIME (KST 그대로 저장)

@dataclass
class ClusterMasterRow:
    cluster_id:         str
    cluster_title:      str
    section:            str
    cluster_created_at: str
    first_seen_at:      str  # DATETIME (KST 그대로 저장)

@dataclass
class ArticleClusterLink:
    """(cluster_id, article_url)를 MERGE로 UPSERT.
    PK: (cluster_id, article_url)

    [수정] initial_rank 컬럼 제거. 네이버 공식 안내("배열 순서는 개인화를
    반영해 추천, 대표 기사는 구독 언론사 중심으로 제공")와 실측 데이터
    (30회 관측 중 28~29회 대표 기사가 교체, 1위였던 기사가 이후 50~60위로
    추락하는 사례 다수)를 근거로, 클러스터 내 노출 순위는 비로그인
    크롤링으로는 의미 있게 해석할 수 없는 지표로 판단해 수집을 중단했다.
    """
    cluster_id:    str
    article_url:   str
    first_seen_at: str         # DATETIME (KST) — 최초로 본 시점, 불변
    last_seen_at:  str         # DATETIME (KST) — 가장 최근에 본 시점, 매번 갱신


# ─────────────────────────────────────────────
# BigQuery 스키마
# TIMESTAMP 대신 DATETIME 사용. TIMESTAMP는 절대 시점(UTC 기준)을 저장하는
# 타입이라 타임존 없는 문자열을 넣으면 UTC로 오인되는 문제가 있다. DATETIME은
# 타임존 정보가 없는 "벽시계 시각"을 그대로 저장하므로, KST로 계산한 문자열을
# 그대로 넣으면 조회 시에도 변환 없이 KST 그대로 보인다.
# ─────────────────────────────────────────────
SCHEMA_ARTICLE_MASTER = [
    bigquery.SchemaField("article_url",   "STRING",   mode="REQUIRED"),
    bigquery.SchemaField("article_title", "STRING"),
    bigquery.SchemaField("press",         "STRING"),
    bigquery.SchemaField("first_seen_at", "DATETIME", mode="REQUIRED"),
]

SCHEMA_CLUSTER_MASTER = [
    bigquery.SchemaField("cluster_id",         "STRING",   mode="REQUIRED"),
    bigquery.SchemaField("cluster_title",      "STRING"),
    bigquery.SchemaField("section",            "STRING"),
    bigquery.SchemaField("cluster_created_at", "STRING"),
    bigquery.SchemaField("first_seen_at",      "DATETIME", mode="REQUIRED"),
]

SCHEMA_ARTICLE_CLUSTER_LINK = [
    bigquery.SchemaField("cluster_id",    "STRING",   mode="REQUIRED"),
    bigquery.SchemaField("article_url",   "STRING",   mode="REQUIRED"),
    bigquery.SchemaField("first_seen_at", "DATETIME", mode="REQUIRED"),
    bigquery.SchemaField("last_seen_at",  "DATETIME", mode="REQUIRED"),
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
        (TBL_ARTICLE, SCHEMA_ARTICLE_MASTER,       None),
        (TBL_CLUSTER, SCHEMA_CLUSTER_MASTER,       None),
        (TBL_LINK,    SCHEMA_ARTICLE_CLUSTER_LINK, "first_seen_at"),  # 파티션
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
# 상태 조회 — 이번에 수집된 (cluster_id, article_url) 조합 중 기존 link 확인
# [변경] fetch_active_articles() 삭제. enter/exit 모델을 버리면서 "현재 활성
# 상태"라는 개념 자체가 없어졌다 — 네이버 클러스터 노출이 사용자/세션마다
# 달라 "지금 활성인지"를 신뢰할 수 없었기 때문. 대신 이번에 수집된 조합이
# article_cluster_link에 이미 있는지만 확인하면 충분하다 (있으면 신규 INSERT를
# 신규 INSERT 여부 판단용 (있으면 last_seen_at만 UPDATE, 없으면 신규 INSERT)
# ─────────────────────────────────────────────
def fetch_existing_links(client: bigquery.Client,
                         link_id: str,
                         pairs: list[tuple[str, str]]) -> dict[tuple[str, str], dict]:
    """
    이번에 수집된 (cluster_id, article_url) 조합 중, article_cluster_link에
    이미 존재하는 것만 조회한다 (비용 최적화 — check_existing_masters()와
    같은 패턴으로 이번 수집 대상만 타겟팅, full scan 안 함).

    반환: { (cluster_id, article_url): { first_seen_at } }
    """
    existing: dict[tuple[str, str], dict] = {}
    if not pairs:
        return existing

    for chunk in chunk_list(pairs, 250):  # (cluster_id, article_url) 쌍이라 URL보다 절반으로
        pair_conditions = " OR ".join(
            f"(cluster_id = '{cid}' AND article_url = '{url}')" for cid, url in chunk
        )
        query = f"""
            SELECT cluster_id, article_url, first_seen_at
            FROM `{link_id}`
            WHERE {pair_conditions}
        """
        try:
            rows = client.query(query).result()
            for row in rows:
                existing[(row.cluster_id, row.article_url)] = {
                    "first_seen_at": row.first_seen_at,
                }
        except Exception as e:
            log.warning(f"article_cluster_link 기존 조합 체크 실패: {e}")

    return existing


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
def bq_update_last_seen(client: bigquery.Client, link_id: str,
                        updates: list[tuple[str, str, str]]) -> int:
    """
    이미 article_cluster_link에 존재하는 (cluster_id, article_url) 조합의
    last_seen_at만 갱신한다. updates: [(cluster_id, article_url, now_str), ...]

    DML(MERGE)을 사용한다 — 이 버전은 결제 계정이 연결된 프로젝트를 전제로
    하므로 무료 등급 DML 제한에 해당하지 않는다. 조합 수가 많을 때를 대비해
    하나의 MERGE 문으로 일괄 처리한다(쿼리 1회로 N건 갱신, 비용 효율적).
    """
    if not updates:
        return 0

    values_str = ", ".join(
        f"STRUCT('{cid}' AS cluster_id, '{url}' AS article_url, DATETIME('{now}') AS new_last_seen_at)"
        for cid, url, now in updates
    )
    query = f"""
        MERGE `{link_id}` AS target
        USING (
            SELECT * FROM UNNEST([{values_str}])
        ) AS source
        ON target.cluster_id = source.cluster_id
           AND target.article_url = source.article_url
        WHEN MATCHED THEN
            UPDATE SET last_seen_at = source.new_last_seen_at
    """
    try:
        job = client.query(query)
        job.result()
        log.info(f"UPDATE {len(updates)}행 → {link_id.split('.')[-1]} (last_seen_at 갱신)")
        return len(updates)
    except Exception as e:
        log.error(f"last_seen_at 갱신 실패: {e}")
        return 0


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

    # [수정] "기사 더보기"를 1회만 클릭하면, 기사가 많은 대형 클러스터(예: 90건 이상)는
    # 한 번의 클릭으로 전체 목록이 다 로드되지 않는다. 그 결과 수집 사이클마다 로드되는
    # 기사 수가 들쑥날쑥해져서, 실제로는 빠지지 않은 기사들이 한꺼번에 exit 처리되고
    # 다음 사이클에 다시 enter로 잡히는 "깜빡임" 버그가 발생했다 (한 클러스터에서
    # 96~100개 기사가 동시에 enter/exit하는 패턴으로 확인됨).
    # → 버튼이 더 이상 보이지 않을 때까지(최대 MAX_MORE_CLICKS회) 반복 클릭한다.
    #
    # [추가 수정] count()는 DOM에 존재하는지만 보고, 화면에 실제로 보이는지는
    # 보지 않는다. 그래서 기사가 적어 더보기가 더 필요 없는 클러스터에서
    # "버튼은 DOM에 남아있지만 보이지 않는(element is not visible)" 상태가 되면,
    # click()이 기본 타임아웃(PAGE_TIMEOUT=20초)까지 계속 재시도하다 실패해서
    # 클러스터마다 불필요하게 20초씩 허비했다. is_visible()로 먼저 보이는지
    # 확인하고, 클릭 자체의 타임아웃도 짧게(MORE_BTN_CLICK_TIMEOUT) 줘서
    # 안 보이면 즉시 다음으로 넘어가도록 한다.
    more_click_count = 0
    more_click_failed = False
    for _ in range(MAX_MORE_CLICKS):
        try:
            more_btn = page.locator("a:has-text('기사 더보기'), button:has-text('기사 더보기')").first
            if more_btn.count() == 0 or not more_btn.is_visible():
                break
            more_btn.click(timeout=MORE_BTN_CLICK_TIMEOUT)
            more_click_count += 1
            time.sleep(CLICK_DELAY)
        except PlaywrightTimeout:
            # 버튼이 안 보이거나 클릭이 막혀 있는 경우 — 더 이상 보여줄 기사가
            # 없는 정상적인 상황일 가능성이 높으므로 warning 없이 조용히 종료
            break
        except Exception as e:
            log.warning(f"  [{cluster_id}] 더보기 클릭 실패 ({more_click_count}번째 시도): {e}")
            more_click_failed = True
            break
    if more_click_count > 0:
        log.info(f"  [{cluster_id}] 더보기 {more_click_count}회 클릭")

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
            "more_click_failed":  more_click_failed,  # [추가] 이 클러스터의 더보기 클릭이
                                                        # 중간에 실패했는지 — 신뢰도 낮은 부분
                                                        # 수집 결과를 build_events에서 걸러내기 위함
        })

    return rows


# ─────────────────────────────────────────────
# 이벤트 diff 로직
# ─────────────────────────────────────────────
def build_link_updates(
    crawled: list[dict],                              # 이번 수집 결과
    existing_links: dict[tuple[str, str], dict],      # 기존 link { (cid, url): {first_seen_at} }
    known_articles: set[str],                          # article_master 등록 여부
    known_clusters: set[str],                          # cluster_master 등록 여부
    now: str,                                          # DATETIME 문자열 (KST)
) -> tuple[list[dict], list[dict], list[dict], list[tuple[str, str, str]]]:
    """
    enter/exit 이벤트를 만들지 않고, article_cluster_link에 대한
    INSERT(신규 조합) / UPDATE(기존 조합의 last_seen_at 갱신) 대상만 만든다.

    반환: (new_articles, new_clusters, new_links, last_seen_updates)
    - new_articles      : article_master에 INSERT할 행
    - new_clusters      : cluster_master에 INSERT할 행
    - new_links         : article_cluster_link에 INSERT할 신규 조합
    - last_seen_updates : article_cluster_link에서 last_seen_at만 갱신할
                          (cluster_id, article_url, now) 튜플 목록

    "더보기 클릭 실패 시 exit 보류" 로직은 필요 없다 — exit 판단 자체가
    없기 때문이다 (네이버 클러스터 노출이 사용자/세션마다 달라 "안 보였다 =
    사라졌다"를 신뢰할 수 없어, exit 추적을 처음부터 두지 않는다).
    """
    # 이번 수집에서 본 (cluster_id, article_url) 조합 — 같은 기사가 여러
    # 클러스터에 동시에 수집되는 경우를 모두 보존한다.
    crawled_map: dict[tuple[str, str], dict] = {}
    for row in crawled:
        url = row["article_url"]
        cid = row["cluster_id"]
        if not url:
            continue
        key = (cid, url)
        if key not in crawled_map:
            crawled_map[key] = row

    # article_master 신규 등록 판단은 article_url 단독 기준
    # (어느 클러스터에서 봤는지와 무관하게, 기사 자체를 처음 본 시점만 추적)
    crawled_urls_seen: dict[str, dict] = {}
    for row in crawled:
        url = row["article_url"]
        if url and url not in crawled_urls_seen:
            crawled_urls_seen[url] = row

    new_articles: list[dict] = []
    new_clusters: list[dict] = []
    new_links:    list[dict] = []
    last_seen_updates: list[tuple[str, str, str]] = []

    # ── 신규 클러스터 등록
    seen_cluster_ids = {row["cluster_id"] for row in crawled}
    for cid in seen_cluster_ids:
        if cid not in known_clusters:
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

    # ── article_cluster_link: 신규 조합은 INSERT, 기존 조합은 last_seen_at만 UPDATE
    for (cid, url), row in crawled_map.items():
        if (cid, url) in existing_links:
            last_seen_updates.append((cid, url, now))
        else:
            new_links.append({
                "cluster_id":    cid,
                "article_url":   url,
                "first_seen_at": now,
                "last_seen_at":  now,
            })

    log.info(
        f"링크 — 신규: {len(new_links)} / last_seen_at 갱신: {len(last_seen_updates)}"
    )
    log.info(f"신규 — article_master: {len(new_articles)} / cluster_master: {len(new_clusters)}")

    return new_articles, new_clusters, new_links, last_seen_updates


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def main():
    import pytz
    KST = pytz.timezone("Asia/Seoul")
    start = datetime.now(KST)

    # [수정] 컬럼 타입을 TIMESTAMP에서 DATETIME으로 변경함에 따라, 저장 문자열도
    # KST 기준 타임존 정보 없는 문자열로 되돌린다. DATETIME은 TIMESTAMP와 달리
    # "절대 시점"이 아니라 "타임존 없는 시각"을 그대로 저장하는 타입이라, BigQuery가
    # 이 값을 UTC로 재해석하지 않는다. 즉 여기 적힌 숫자 그대로가 KST이고,
    # 조회 시 별도 변환이 필요 없다.
    now_str = start.strftime("%Y-%m-%d %H:%M:%S")

    if TARGET_SECTION:
        name, sid = TARGET_SECTION.split(",")
        sections = {name: int(sid)}
    else:
        sections = SECTIONS

    log.info("=" * 60)
    log.info(f"크롤러 시작: {now_str} KST (DATETIME 컬럼에 그대로 저장)")
    log.info(f"실행 섹션: {list(sections.keys())}")
    log.info("=" * 60)

    # BigQuery 연결 + 테이블 준비
    client    = get_bq_client()
    table_ids = ensure_tables(client)

    article_id = table_ids[TBL_ARTICLE]
    cluster_id = table_ids[TBL_CLUSTER]
    link_id    = table_ids[TBL_LINK]

    # [변경] 기존의 테이블 전체 로드(Full Scan) 방식을 폐기하고 섹션별 루프 내에서 처리하도록 변경합니다.
    total_new_links = total_updated_links = 0
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

                # [안전장치] 수집된 데이터가 아예 없으면 네트워크 오류 또는 구조 변경일
                # 수 있으므로 이 섹션은 스킵합니다.
                if not crawled:
                    log.warning(f"[{section_name}] 수집된 기사가 없어 데이터 처리를 스킵합니다.")
                    continue

                # 2. [비용 최적화] 이번에 수집된 대상 목록만 쿼리하여 마스터에 존재하는지 체크 (Full Scan 방지)
                crawled_urls = [r["article_url"] for r in crawled if r["article_url"]]
                crawled_clusters = list({r["cluster_id"] for r in crawled})

                known_articles, known_clusters = check_existing_masters(
                    client, crawled_urls, crawled_clusters
                )

                # 3. 이번에 수집된 (cluster_id, article_url) 조합 중 이미
                #    article_cluster_link에 있는 것만 조회 (신규 INSERT 여부 판단용)
                crawled_pairs = list({
                    (r["cluster_id"], r["article_url"])
                    for r in crawled if r["article_url"]
                })
                existing_links = fetch_existing_links(client, link_id, crawled_pairs)

                # 4. diff → link INSERT/UPDATE 대상 생성
                new_articles, new_clusters, new_links, last_seen_updates = build_link_updates(
                    crawled, existing_links, known_articles, known_clusters, now_str
                )

                # 5. BigQuery 적재
                bq_insert(client, cluster_id, SCHEMA_CLUSTER_MASTER, new_clusters)
                bq_insert(client, article_id, SCHEMA_ARTICLE_MASTER, new_articles)
                bq_insert(client, link_id,    SCHEMA_ARTICLE_CLUSTER_LINK, new_links)
                bq_update_last_seen(client, link_id, last_seen_updates)

                total_new_links     += len(new_links)
                total_updated_links += len(last_seen_updates)

            except Exception as e:
                log.error(f"[{section_name}] 예외 발생: {e}", exc_info=True)
                had_error = True  # [추가] 실패를 기록 — 워크플로우가 조용히 성공 처리되는 것을 방지
                continue

        browser.close()

    elapsed = (datetime.now(KST) - start).seconds
    log.info("\n" + "=" * 60)
    log.info(f"완료: 신규 링크 {total_new_links}건 / last_seen_at 갱신 {total_updated_links}건 / 소요 {elapsed}초")
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
