-- ============================================================
-- article_cluster_link.last_seen_at 동기화
-- sighting_log에서 각 (cluster_id, article_url)의 가장 최근 seen_at을 가져와
-- article_cluster_link를 통째로 재작성한다 (DDL, DML 아님 — 결제 계정 불필요)
--
-- 수동 실행: BigQuery 콘솔에서 이 쿼리를 그대로 실행하면 됨
-- 자동 실행: 별도 GitHub Actions 워크플로우가 1일 1회 동일 쿼리를 실행
-- ============================================================

CREATE OR REPLACE TABLE `news-cluster-tracker.naver_cluster.article_cluster_link`
PARTITION BY DATE(first_seen_at) AS
SELECT
  l.cluster_id,
  l.article_url,
  l.initial_rank,
  l.first_seen_at,
  -- sighting_log에 해당 조합의 기록이 있으면 최신 seen_at으로 갱신,
  -- 없으면(예: 1일 만료로 이미 사라진 경우) 기존 last_seen_at을 그대로 유지
  COALESCE(s.max_seen_at, l.last_seen_at) AS last_seen_at
FROM `news-cluster-tracker.naver_cluster.article_cluster_link` l
LEFT JOIN (
  SELECT cluster_id, article_url, MAX(seen_at) AS max_seen_at
  FROM `news-cluster-tracker.naver_cluster.sighting_log`
  GROUP BY cluster_id, article_url
) s
ON l.cluster_id = s.cluster_id AND l.article_url = s.article_url;

-- ============================================================
-- 검증
-- ============================================================

-- 1) last_seen_at이 NULL인 행 수 (도입 첫 회차 이후로는 0이어야 정상)
SELECT COUNT(*) AS null_last_seen_count
FROM `news-cluster-tracker.naver_cluster.article_cluster_link`
WHERE last_seen_at IS NULL;

-- 2) 갱신 결과 샘플
SELECT cluster_id, article_url, initial_rank, first_seen_at, last_seen_at
FROM `news-cluster-tracker.naver_cluster.article_cluster_link`
ORDER BY last_seen_at DESC
LIMIT 20;

-- 3) 전체 행 수 (동기화 전후로 변하면 안 됨 — JOIN이 1:1이어야 함)
SELECT COUNT(*) AS total_rows
FROM `news-cluster-tracker.naver_cluster.article_cluster_link`;
