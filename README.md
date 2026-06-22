# 네이버 뉴스 클러스터 크롤러

네이버 뉴스 클러스터를 10분마다 자동 수집해서 Google BigQuery에 적재하는 크롤러

**분석 목적:** 언론사의 이슈·속보 대응력 분석
- 동일 이슈 클러스터 내 언론사별 기사 행태 비교
- 미진입 클러스터 주제 파악
- 진입 클러스터의 송고 타이밍·기사 구성 비교

---

## 파일 구조

```
news-cluster-crawler/
├── naver_cluster_crawler_bq.py   ← 크롤러 본체
├── requirements_bq.txt           ← Python 패키지 목록
├── README.md
└── .github/
    └── workflows/
        └── crawl_bq.yml          ← GitHub Actions 자동 실행 설정
```

---

## 전체 아키텍처

```
[cron-job.org] ──10분마다 POST──▶ [GitHub Actions]
                                        │
                              섹션 6개 병렬 job 실행
                              (정치/경제/사회/생활·문화/세계/IT·과학)
                                        │
                              [naver_cluster_crawler_bq.py]
                                        │
                              네이버 뉴스 클러스터 크롤링
                              (Playwright + BeautifulSoup)
                                        │
                                        ▼
                              [Google BigQuery]
                              naver_cluster.article_master
                              naver_cluster.cluster_master
                              naver_cluster.cluster_article_events
```

---

## DB 설계

기존 스냅샷 방식(매 수집마다 전체 행 적재)에서 **이벤트 기반 방식**으로 변경.

> **변경 배경**
> 기존 방식은 동일 기사가 클러스터에 노출되는 동안 10분마다 반복 적재되어
> 11일 운영 시 BigQuery 무료 용량(10GB)을 초과함.
> 또한 랭킹(`article_rank`)이 10분 주기로 빈번하게 변동(167회 수집 중 59회 변동 사례 확인)하여
> 매 스냅샷 랭킹 기록의 분석 가치가 낮다고 판단, 첫 진입 시 순위만 보존하는 방식으로 전환.

---

### 테이블 1. `article_master` — 기사 원장

기사 URL당 **1행**. 기사가 어느 클러스터에서든 처음 등장할 때 INSERT.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `article_url` | STRING | 기사 URL **(PK)** |
| `article_title` | STRING | 기사 제목 |
| `press` | STRING | 언론사명 |
| `first_seen_at` | TIMESTAMP | 최초 수집 시각 (KST) |

---

### 테이블 2. `cluster_master` — 클러스터 원장

클러스터 ID당 **1행**. 클러스터가 처음 감지될 때 INSERT.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `cluster_id` | STRING | 클러스터 고유 ID **(PK)** |
| `cluster_title` | STRING | 클러스터 제목 |
| `section` | STRING | 섹션명 (정치/경제/사회/생활·문화/세계/IT·과학) |
| `cluster_created_at` | STRING | 네이버 표시 클러스터 생성 시각 |
| `first_seen_at` | TIMESTAMP | 크롤러 최초 감지 시각 (KST) |

---

### 테이블 3. `cluster_article_events` — 기사 진입/이탈 이벤트

기사가 클러스터에 **진입하거나 이탈할 때만** append. 핵심 분석 테이블.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `cluster_id` | STRING | 클러스터 고유 ID |
| `article_url` | STRING | 기사 URL |
| `event_type` | STRING | `enter` (진입) / `exit` (이탈) |
| `event_at` | TIMESTAMP | 이벤트 감지 시각 (KST) |
| `initial_rank` | INTEGER | 진입 시 클러스터 내 순위 (`exit`이면 NULL) |

> **`initial_rank` 보존 이유**
> 랭킹 자체는 노이즈가 심해 추적하지 않으나,
> 첫 진입 시 순위는 "대표기사 여부" 판단에 활용 가능하여 enter 이벤트에만 기록.

**파티션:** `event_at` 기준 일별 파티션

---

## 크롤러 동작 방식

```
실행 시작 (섹션 1개 단위)
  │
  ├─ BigQuery 연결 + 테이블 없으면 자동 생성
  ├─ cluster_article_events에서 현재 활성 기사 목록 로드 (is_active 상태 추적용)
  │
  └─ Playwright(Chromium)로 네이버 뉴스 크롤링
       │
       ├─ 섹션 페이지 접속 → 클러스터 목록 파싱
       ├─ 클러스터별 상세 페이지 접속 → 기사 목록 파싱
       │
       ├─ 신규 클러스터 → cluster_master INSERT
       ├─ 신규 기사 → article_master INSERT
       │
       ├─ 이전 수집에 없던 기사 → cluster_article_events에 enter INSERT
       └─ 이번 수집에서 사라진 기사 → cluster_article_events에 exit INSERT
```

---

## 실행 트리거
### cron-job.org (메인 트리거)
GitHub Actions cron 지연 문제를 우회하기 위해 외부에서 정확히 10분마다 `workflow_dispatch` 호출

```
URL:    https://api.github.com/repos/yeon-crypto/news-cluster-crawler/actions/workflows/crawl_bq.yml/dispatches
Method: POST
Header: Authorization: Bearer {GitHub PAT}
Header: Accept: application/vnd.github+json
Header: Content-Type: application/json
Body:   {"ref":"gcp"}
주기:   Every 10 minutes
```

---

## GitHub Secrets 설정

Settings → Secrets and variables → Actions에 아래 등록 필요

| Secret 이름 | 값 |
|---|---|
| `GCP_SA_JSON` | GCP 서비스 계정 키 JSON 전체 |
| `BQ_PROJECT` | GCP 프로젝트 ID |
| `SLACK_WEBHOOK_URL` | Slack Incoming Webhook URL (실패 알림용) |

---

## GCP 서비스 계정 권한

IAM에서 서비스 계정에 아래 역할 2개 필요

| 역할 | 용도 |
|---|---|
| BigQuery 데이터 편집자 | 데이터셋/테이블 생성 + 데이터 적재 |
| BigQuery 사용자 | 쿼리 실행 (활성 기사 목록 조회용) |

---

## 환경변수

| 변수명 | 필수 | 기본값 | 설명 |
|---|---|---|---|
| `GCP_SA_JSON` | ✅ | - | GCP 서비스 계정 JSON |
| `BQ_PROJECT` | ✅ | - | GCP 프로젝트 ID |
| `BQ_DATASET` | ❌ | `naver_cluster` | BigQuery 데이터셋명 |
| `TARGET_SECTION` | ❌ | 전체 | 특정 섹션만 실행 (예: `정치,100`) |
