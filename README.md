# 뉴스 클러스터 트래커 (news-cluster-tracker)

네이버 뉴스 섹션별 클러스터(관련 기사 묶음)를 10분 주기로 수집하여, 기사의 진입(enter)/이탈(exit)
이벤트를 BigQuery에 적재하는 파이프라인. 스냅샷이 아닌 **이벤트 기반 적재**로 누적 데이터량을
최소화하고, 진입률·체류 시간 등의 시계열 분석을 지원한다.

---

## 아키텍처

```
cron-job.org (10분 주기, PAT 인증)
    │  POST workflow_dispatch
    ▼
GitHub Actions ── matrix: 정치 / 경제 / 사회 / 생활문화 / 세계 / IT과학 (6개 섹션 병렬)
    │
    ├─ Playwright(Chromium) + BeautifulSoup
    │     섹션 페이지 → 클러스터 목록 → "더보기" 클릭 → 기사 목록 파싱
    │
    ├─ diff 판단 (Python)
    │     이번 수집 결과 vs BigQuery 내 현재 활성 기사 목록 비교
    │     → 신규 기사 / 신규 클러스터 / enter 이벤트 / exit 이벤트 산출
    │
    ▼
Google BigQuery (asia-northeast3)
    ├─ article_master            기사 원장
    ├─ cluster_master             클러스터 원장
    └─ cluster_article_events     enter/exit 이벤트 (event_at 기준 일별 파티션)
    │
    ▼
6개 섹션 중 1개 이상 실패 시, 전체 종료 후 정확히 1회 ─→ Gmail SMTP 메일 알림
```

---

## 데이터 모델

| 테이블 | PK | 역할 | 적재 방식 |
|---|---|---|---|
| `article_master` | article_url | 기사 원장 | 신규 기사만 append |
| `cluster_master` | cluster_id | 클러스터 원장 | 신규 클러스터만 append |
| `cluster_article_events` | (cluster_id, article_url, event_type, event_at) | enter/exit 이벤트 | append-only |

**클러스터 제목 보정**: 일부 클러스터는 제목이 비어있거나 "관련 뉴스" 같은 의미 없는 텍스트로만
잡히는 경우가 있다. 이 경우 해당 클러스터에 최초 진입(`rank=1`)한 기사의 제목으로 대체해
`cluster_title`을 채운다.

---

## 크롤러 처리 흐름 (섹션별)

1. Playwright로 섹션 페이지 접속 → 클러스터 목록 파싱
2. 클러스터별 상세 페이지에서 "더보기" 클릭 → 기사 목록 파싱
3. 이번 수집 결과만으로 BigQuery 마스터 테이블 존재 여부 조회 (전체 스캔 없이 비용 최적화)
4. 현재 활성 기사 목록(최근 30일, 섹션 단위) 조회 후 diff
5. 신규 기사·클러스터 INSERT, enter/exit 이벤트 INSERT

**안전장치**
- 수집 결과가 0건이면 네트워크 오류·구조 변경으로 간주해 해당 섹션을 스킵한다
  (활성 기사가 전부 exit로 오판되는 것을 방지).
- 어느 섹션에서든 처리 중 예외가 발생하면 다른 섹션은 영향 없이 계속 진행하되, 전체 종료 시점에
  실행 자체는 실패로 종료되어 알림이 트리거된다.

---

## GitHub Actions 워크플로우 (`crawl_cr.yml`)

- `workflow_dispatch` 트리거로 외부 스케줄러(cron-job.org)가 호출
- 6개 섹션을 `matrix`로 병렬 실행 (`fail-fast: false` — 한 섹션 실패가 나머지에 영향 없음)
- `concurrency.cancel-in-progress: true`로 이전 실행이 끝나기 전 중복 트리거 방지
- Playwright 브라우저 바이너리는 캐싱되어 매 실행마다 새로 다운로드하지 않는다
- 6개 섹션이 모두 끝난 뒤, 1개 이상 실패했을 경우에만 별도 `notify` job이 실행되어 이메일 1통 발송

---

## 환경 변수 / GitHub Secrets

| Secret 이름 | 용도 |
|---|---|
| `GCP_SA_JSON` | BigQuery 서비스 계정 키 (JSON 전체) |
| `BQ_PROJECT` | GCP 프로젝트 ID |
| `MAIL_USERNAME` | 발송용 Gmail 주소 |
| `MAIL_PASSWORD` | Gmail 앱 비밀번호 |
| `MAIL_TO` | 알림 수신 이메일 주소 |

서비스 계정에는 다음 IAM 역할이 필요하다.
- `roles/bigquery.dataEditor` — 테이블 생성/읽기/쓰기
- `roles/bigquery.jobUser` — 쿼리·적재 Job 실행

---

## 외부 스케줄러 (cron-job.org)

- GitHub Personal Access Token(Fine-grained, `Actions: Read and write` 권한)으로 인증
- 10분 주기로 다음 엔드포인트를 호출
  ```
  POST https://api.github.com/repos/{계정}/{저장소}/actions/workflows/crawl_cr.yml/dispatches
  Authorization: Bearer {PAT}
  Body: {"ref": "main"}
  ```
