USE DATABASE RETAIL_DB;
USE SCHEMA ABT_BUY;

CREATE OR REPLACE TABLE match_config AS
SELECT 0.80::FLOAT AS similarity_threshold;

CREATE OR REPLACE VIEW abt_canonical AS
SELECT
    ID AS PRODUCT_ID,
    TRIM(
        LOWER(
            COALESCE(NAME, '') || ' ' || 
            COALESCE(DESCRIPTION, '')
        )
    ) AS CLEAN_TEXT,
    PRICE
FROM ABT;

CREATE OR REPLACE VIEW buy_canonical AS
SELECT
    ID AS PRODUCT_ID,
    TRIM(
        LOWER(
            COALESCE(NAME, '') || ' ' || 
            COALESCE(DESCRIPTION, '') || ' ' || 
            COALESCE(MANUFACTURER, '')
        )
    ) AS CLEAN_TEXT,
    PRICE
FROM BUY;

CREATE OR REPLACE TABLE abt_embeddings AS
SELECT
    PRODUCT_ID,
    CLEAN_TEXT,
    AI_EMBED('snowflake-arctic-embed-m', CLEAN_TEXT) AS EMBEDDING
FROM ABT_CANONICAL;

CREATE OR REPLACE TABLE buy_embeddings AS
SELECT
    PRODUCT_ID,
    CLEAN_TEXT,
    AI_EMBED('snowflake-arctic-embed-m', CLEAN_TEXT) AS EMBEDDING
FROM BUY_CANONICAL;

CREATE OR REPLACE TABLE SIMILARITY_SCORES AS
WITH similarity_scores_cte AS (
    SELECT
        a.PRODUCT_ID AS ABT_ID,
        b.PRODUCT_ID AS BUY_ID,
        VECTOR_COSINE_SIMILARITY(a.EMBEDDING, b.EMBEDDING) AS SIMILARITY
    FROM ABT_EMBEDDINGS a
    CROSS JOIN BUY_EMBEDDINGS b
)
SELECT
    ABT_ID,
    BUY_ID,
    SIMILARITY
FROM similarity_scores_cte
ORDER BY SIMILARITY DESC;

CREATE OR REPLACE TABLE PRODUCT_MATCHES AS
SELECT ABT_ID, BUY_ID, SIMILARITY
FROM (
    SELECT
        ABT_ID,
        BUY_ID,
        SIMILARITY,
        ROW_NUMBER() OVER (PARTITION BY ABT_ID ORDER BY SIMILARITY DESC) AS RN
    FROM SIMILARITY_SCORES
)
WHERE RN = 1 AND SIMILARITY >= (
    SELECT SIMILARITY_THRESHOLD
    FROM MATCH_CONFIG
);

SELECT
    COUNT(*) AS TOTAL_GROUND_TRUTH,
    SUM(CASE WHEN PM.BUY_ID IS NOT NULL THEN 1 ELSE 0 END) AS CORRECT_MATCHES
FROM ABT_BUY_PERFECTMAPPING GT
LEFT JOIN PRODUCT_MATCHES PM
    ON GT.IDABT = PM.ABT_ID
    AND GT.IDBUY = PM.BUY_ID;

CREATE OR REPLACE VIEW final_product_matches AS
SELECT
    p.ABT_ID,
    p.BUY_ID,
    p.SIMILARITY,
    'EMBEDDING_BASED' AS MATCH_STRATEGY,
    CASE
        WHEN p.SIMILARITY >= (cfg.t + 0.6 * (1.0 - cfg.t)) THEN 'HIGH'
        WHEN p.SIMILARITY >= (cfg.t + 0.4 * (1.0 - cfg.t)) THEN 'MEDIUM'
        ELSE 'LOW'
    END AS CONFIDENCE_BUCKET,
    p.SIMILARITY AS FINAL_CONFIDENCE
FROM PRODUCT_MATCHES p,
     (SELECT similarity_threshold AS t FROM match_config) cfg
WHERE p.SIMILARITY >= cfg.t;

CREATE OR REPLACE VIEW price_comparison AS
SELECT
    F.ABT_ID,
    F.BUY_ID,
    A.PRICE AS ABT_PRICE,
    B.PRICE AS BUY_PRICE,
    CASE
        WHEN A.PRICE IS NULL OR B.PRICE IS NULL THEN 'INSUFFICIENT DATA'
        WHEN TRY_TO_DOUBLE(REGEXP_REPLACE(A.PRICE, '[$,]', '')) > TRY_TO_DOUBLE(REGEXP_REPLACE(B.PRICE, '[$,]', '')) THEN 'ABT_OVERPRICED'
        WHEN TRY_TO_DOUBLE(REGEXP_REPLACE(A.PRICE, '[$,]', '')) < TRY_TO_DOUBLE(REGEXP_REPLACE(B.PRICE, '[$,]', '')) THEN 'BUY_OVERPRICED'
    END AS PRICING_STATUS
FROM FINAL_PRODUCT_MATCHES AS F
LEFT JOIN ABT A ON F.ABT_ID = A.ID
LEFT JOIN BUY B ON F.BUY_ID = B.ID;

-- CREATE OR REPLACE VIEW matching_metrics AS
-- SELECT
--     COUNT(*) AS TOTAL_GROUND_TRUTH_PAIRS,
--     COUNT(F.ABT_ID) AS CORRECTLY_MATCHED_PAIRS,
--     ROUND(
--         COUNT(F.ABT_ID) / COUNT(*) :: FLOAT,
--         4
--     ) AS PRECISION
-- FROM ABT_BUY_PERFECTMAPPING GT
-- LEFT JOIN FINAL_PRODUCT_MATCHES F
-- ON GT.IDABT = F.ABT_ID
-- AND GT.IDBUY = F.BUY_ID;

CREATE OR REPLACE VIEW matching_metrics AS

WITH cfg AS (
    SELECT similarity_threshold AS t
    FROM match_config
), gt AS (
    SELECT DISTINCT IDABT, IDBUY
    FROM ABT_BUY_PERFECTMAPPING
),
pred AS (
    SELECT DISTINCT ABT_ID, BUY_ID, MATCH_STRATEGY, FINAL_CONFIDENCE
    FROM FINAL_PRODUCT_MATCHES
),

-- True Positives: predicted pairs that exist in ground truth
tp AS (
    SELECT p.ABT_ID, p.BUY_ID
    FROM pred p
    INNER JOIN gt g
        ON p.ABT_ID = g.IDABT
       AND p.BUY_ID = g.IDBUY
),

-- False Positives: predicted pairs that are NOT in ground truth
fp AS (
    SELECT p.ABT_ID, p.BUY_ID
    FROM pred p
    LEFT JOIN gt g
        ON p.ABT_ID = g.IDABT
       AND p.BUY_ID = g.IDBUY
    WHERE g.IDABT IS NULL
),

-- False Negatives: ground truth pairs that were NOT predicted
fn AS (
    SELECT g.IDABT, g.IDBUY
    FROM gt g
    LEFT JOIN pred p
        ON p.ABT_ID = g.IDABT
       AND p.BUY_ID = g.IDBUY
    WHERE p.ABT_ID IS NULL
),

-- Aggregate counts
counts AS (
    SELECT
        (SELECT COUNT(*) FROM gt)      AS total_ground_truth_pairs,
        (SELECT COUNT(*) FROM pred)    AS total_predicted_pairs,
        (SELECT COUNT(*) FROM tp)      AS true_positives,
        (SELECT COUNT(*) FROM fp)      AS false_positives,
        (SELECT COUNT(*) FROM fn)      AS false_negatives
)
SELECT
    cfg.t AS similarity_threshold,
    total_ground_truth_pairs,
    total_predicted_pairs,
    true_positives,
    false_positives,
    false_negatives,

    -- Precision = TP / (TP + FP)
    ROUND(
        true_positives / NULLIF((true_positives + false_positives), 0)::FLOAT,
        4
    ) AS precision,

    -- Recall = TP / (TP + FN)
    ROUND(
        true_positives / NULLIF((true_positives + false_negatives), 0)::FLOAT,
        4
    ) AS recall,

    -- F1 = 2 * P * R / (P + R)
    ROUND(
        CASE
            WHEN (NULLIF((true_positives + false_positives), 0) IS NULL)
              OR (NULLIF((true_positives + false_negatives), 0) IS NULL) THEN NULL
            ELSE
                /* compute using counts to avoid rounding twice */
                2 * (true_positives::FLOAT) /
                NULLIF(
                    ((true_positives + false_positives)::FLOAT) +
                    ((true_positives + false_negatives)::FLOAT),
                    0
                )
        END,
        4
    ) AS f1,

    -- Coverage: fraction of ABT_IDs in GT that received at least one prediction
    ROUND((
        SELECT COUNT(DISTINCT g.IDABT)
        FROM gt g
        JOIN pred p ON p.ABT_ID = g.IDABT
    ) / NULLIF((
        SELECT COUNT(DISTINCT g2.IDABT)
        FROM gt g2
    ), 0)::FLOAT, 4) AS abt_coverage
FROM counts
CROSS JOIN cfg;