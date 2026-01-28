import trino
import pandas as pd
import json
import sys
from datetime import datetime, timedelta
import warnings
from pathlib import Path
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# ==============================================================================
# 접속 정보 설정
# ==============================================================================
HOST = 'aidp-trino-analysis.sksiltron.co.kr'
PORT = 31085
USER = '253699'
PASSWORD = '$iltron3501'

# ==============================================================================
# Trino 연결 생성 함수
# ==============================================================================
def create_trino_connection():
    """Trino DB에 안전하게 연결"""
    return trino.dbapi.connect(
        host=HOST,
        port=PORT,
        user=USER,
        http_scheme='https',
        auth=trino.auth.BasicAuthentication(USER, PASSWORD),
        verify=False
    )

# ==============================================================================
# 안전한 float 변환
# ==============================================================================
def safe_float(val, default=0.0):
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        if val.strip().lower() == "nan":
            return float('nan')
        try:
            return float(val)
        except ValueError:
            return default
    return default

# ==============================================================================
# EXPLAIN으로 IO 통계 확인
# ==============================================================================
def check_data_size_before_query(conn, query):
    explain_query = f"EXPLAIN (TYPE IO, FORMAT JSON) {query}"
    cur = conn.cursor()

    try:
        print("EXPLAIN 쿼리 실행 중... (예상 데이터 스캔 및 전송 정보 확인)")
        cur.execute(explain_query)
        explain_result = cur.fetchall()

        json_str = explain_result[0][0]
        io_stats = json.loads(json_str)

        input_tables = io_stats.get("inputTableColumnInfos", [])
        total_input_gb = 0.0
        print("\n쿼리 예상 스캔 정보 (입력 기준):")
        for table_info in input_tables:
            table_name = table_info["table"]["schemaTable"]["table"]
            estimate = table_info.get("estimate", {})
            size_bytes = safe_float(estimate.get("outputSizeInBytes"), 0)
            size_gb = size_bytes / (1024 ** 3)
            total_input_gb += size_gb
            print(f"  - 테이블: {table_name}")
            print(f"    예상 스캔 크기: {size_bytes / (1024**2):.2f} MB ({size_gb:.3f} GB)")

        global_estimate = io_stats.get("estimate", {})
        output_size_bytes = safe_float(global_estimate.get("outputSizeInBytes"), float('nan'))
        output_size_gb = output_size_bytes / (1024 ** 3) if not pd.isna(output_size_bytes) else float('nan')

        print(f"\n 총 예상 출력 데이터 크기: "
              f"{output_size_bytes / (1024**2):.2f} MB ({output_size_gb:.3f} GB)" 
              if not pd.isna(output_size_gb) else "⚠️ 출력 크기 추정 불가 (outputSizeInBytes = NaN)")

        if pd.isna(output_size_gb) or output_size_gb == float('inf'):
            print(f"출력 추정 실패 → 입력 기준 예측 사용: {total_input_gb:.3f} GB")
            if total_input_gb > 1.0:
                confirm = input("계속 진행하시겠습니까? (y/N): ").strip().lower()
                if confirm not in ['y', 'yes']:
                    print("사용자에 의해 쿼리 취소됨.")
                    sys.exit(0)
        else:
            if output_size_gb > 1.0:
                confirm = input("계속 진행하시겠습니까? 매우 큰 데이터일 수 있습니다. (y/N): ").strip().lower()
                if confirm not in ['y', 'yes']:
                    print("사용자에 의해 쿼리 취소됨.")
                    sys.exit(0)

        print("용량 확인 완료. 실제 쿼리 실행을 시작합니다.")

    except Exception as e:
        print(f"EXPLAIN 분석 중 오류 발생: {e}")
        confirm = input("EXPLAIN 실패. 그래도 쿼리 실행하시겠습니까? (y/N): ").strip().lower()
        if confirm not in ['y', 'yes']:
            print("사용자에 의해 쿼리 취소됨.")
            sys.exit(0)
    finally:
        cur.close()

# ==============================================================================
# 메인 실행 함수
# ==============================================================================
def main():
    # 오늘 날짜 기준 어제 날짜 생성
    YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    YESTERDAY_NM = (datetime.now() - timedelta(days=1)).strftime('%y-%m-%d')  # '26-01-25'
    YESTERDAY_YEAR_MONTH = YESTERDAY[:6]  # '202601'

    print(f"일자: 어제 ({YESTERDAY})")

    QUERY = f"""
 WITH
-- (1) Z: 원본 + 보정 데이터 통합
Z AS (
-- (1-1) 원본 데이터
SELECT
A.WAF_SIZE,
A.BASE_DT,
A.DIV_CD,
A.REJ_DIV_CD,
A.FAC_ID,
A.OPER_ID,
A.OWNR_CD,
A.CRET_CD,
A.PROD_ID,
A.IGOT_ID,
A.BLK_ID,
A.SUBLOT_ID,
A.USER_LOT_ID,
A.EQP_ID,
COALESCE(TRIM(BEF_ALIAS.ALIAS_RSN_CD), A.BEF_BAD_RSN_CD) AS BEF_BAD_RSN_CD,
COALESCE(TRIM(AFT_ALIAS.ALIAS_RSN_CD), A.AFT_BAD_RSN_CD) AS AFT_BAD_RSN_CD,
A.REJ_GROUP,
A.OPER1_GROUP,
A.OPER2_GROUP,
A.RESPON,
A.ALLO_GROUP,
A.RESPON_RATIO,
A.IN_QTY,
A.OUT_QTY,
A.LOSS_QTY,
A.REAL_DPT_GROUP,
D.CD_NM AS GRD_CD_NM_CS,
E.CD_NM AS GRD_CD_NM_PS,
C.CUST_SITE_NM,
SUBSTR(B.BESOF_BASE_YW_NM, 3) AS WEEK_DAY_NM,
A.DATA_CHG_DTTM
FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLSTD_S A

-- BEF 매핑
LEFT JOIN (
SELECT
XX.REJ_RSN_GRP,
XX.REJ_RSN_CD,
XX.ALIAS_RSN_CD,
ROW_NUMBER() OVER (PARTITION BY XX.REJ_RSN_GRP, XX.REJ_RSN_CD ORDER BY XX.REJ_RSN_GRP) AS RN
FROM oracle.PMDW_MGR.DW_BA_CM_REJRSNINFO_M XX
WHERE XX.WAF_SIZE = '300'
AND XX.PROD_DIV_CD = CASE WHEN 'WF' = 'WF' THEN 'PW' ELSE 'EPI' END
) AS BEF_ALIAS
ON BEF_ALIAS.REJ_RSN_GRP = A.REJ_GROUP
AND BEF_ALIAS.REJ_RSN_CD = A.BEF_BAD_RSN_CD
AND BEF_ALIAS.RN = 1

-- AFT 매핑
LEFT JOIN (
SELECT
XX.REJ_RSN_GRP,
XX.REJ_RSN_CD,
XX.ALIAS_RSN_CD,
ROW_NUMBER() OVER (PARTITION BY XX.REJ_RSN_GRP, XX.REJ_RSN_CD ORDER BY XX.REJ_RSN_GRP) AS RN
FROM oracle.PMDW_MGR.DW_BA_CM_REJRSNINFO_M XX
WHERE XX.WAF_SIZE = '300'
AND XX.PROD_DIV_CD = CASE WHEN 'WF' = 'WF' THEN 'PW' ELSE 'EPI' END
) AS AFT_ALIAS
ON AFT_ALIAS.REJ_RSN_GRP = A.REJ_GROUP
AND AFT_ALIAS.REJ_RSN_CD = A.AFT_BAD_RSN_CD
AND AFT_ALIAS.RN = 1

JOIN oracle.PMDW_MGR.DW_BA_CM_BASEDATE_M B
ON B.BASE_DT = A.BASE_DT

LEFT JOIN oracle.PMDW_MGR.DW_BA_MS_PROD_M C
ON C.PROD_ID = A.PROD_ID
AND C.SPEC_DIV_CD = 'PS'

LEFT JOIN oracle.DMS_MGR.TB_FX_CODES D
ON D.UP_CD = 'DMS010'
AND D.SYS_CD = 'DMS'
AND D.CD_VAL = C.GRD_CD_NM

LEFT JOIN oracle.DMS_MGR.TB_FX_CODES E
ON E.UP_CD = 'DMS010'
AND E.SYS_CD = 'DMS'
AND E.CD_VAL = C.GRD_CD_NM_PS

JOIN oracle.PMDW_MGR.DW_BA_CM_STDPOPER_M Z1
ON Z1.FAC_ID = A.FAC_ID
AND Z1.OPER_ID = A.OPER_ID

WHERE
1 = 1
AND Z1.OPER_DIV_L = 'WF'
AND (CASE WHEN 'PN' = 'PN' THEN TRUE ELSE C.GRD_CD_NM = 'PN' END)
AND (CASE WHEN 'PN' = 'PN' THEN TRUE ELSE C.GRD_CD_NM_PS = 'PN' END)
AND Z1.FAC_ID IN ('WF7', 'WF8', 'WFA', 'FPC7', 'FPC8')
AND 'N' = 'N'
AND A.WAF_SIZE = '300'
AND A.BASE_DT = '{YESTERDAY}'  --  어제 하루만

UNION ALL

-- (1-2) 보정 데이터
SELECT
A.WAF_SIZE,
A.BASE_DT,
A.DIV_CD,
A.REJ_DIV_CD,
A.FAC_ID,
A.OPER_ID,
A.OWNR_CD,
A.CRET_CD,
A.PROD_ID,
A.IGOT_ID,
A.BLK_ID,
A.SUBLOT_ID,
A.USER_LOT_ID,
A.EQP_ID,
COALESCE(TRIM(BEF_ALIAS.ALIAS_RSN_CD), A.BEF_BAD_RSN_CD) AS BEF_BAD_RSN_CD,
COALESCE(TRIM(AFT_ALIAS.ALIAS_RSN_CD), A.AFT_BAD_RSN_CD) AS AFT_BAD_RSN_CD,
A.REJ_GROUP,
A.OPER1_GROUP,
A.OPER2_GROUP,
A.RESPON,
A.ALLO_GROUP,
A.RESPON_RATIO,
SUM(A.IN_QTY) AS IN_QTY,
SUM(A.OUT_QTY) AS OUT_QTY,
SUM(A.LOSS_QTY) AS LOSS_QTY,
A.REAL_DPT_GROUP,
D.CD_NM AS GRD_CD_NM_CS,
E.CD_NM AS GRD_CD_NM_PS,
C.CUST_SITE_NM,
SUBSTR(B.BESOF_BASE_YW_NM, 3) AS WEEK_DAY_NM,
MAX(A.DATA_CHG_DTTM) AS DATA_CHG_DTTM
FROM (
SELECT *
FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S
) A

-- BEF 매핑
LEFT JOIN (
SELECT
XX.REJ_RSN_GRP,
XX.REJ_RSN_CD,
XX.ALIAS_RSN_CD,
ROW_NUMBER() OVER (PARTITION BY XX.REJ_RSN_GRP, XX.REJ_RSN_CD ORDER BY XX.REJ_RSN_GRP) AS RN
FROM oracle.PMDW_MGR.DW_BA_CM_REJRSNINFO_M XX
WHERE XX.WAF_SIZE = '300'
AND XX.PROD_DIV_CD = CASE WHEN 'WF' = 'WF' THEN 'PW' ELSE 'EPI' END
) AS BEF_ALIAS
ON BEF_ALIAS.REJ_RSN_GRP = A.REJ_GROUP
AND BEF_ALIAS.REJ_RSN_CD = A.BEF_BAD_RSN_CD
AND BEF_ALIAS.RN = 1

-- AFT 매핑
LEFT JOIN (
SELECT
XX.REJ_RSN_GRP,
XX.REJ_RSN_CD,
XX.ALIAS_RSN_CD,
ROW_NUMBER() OVER (PARTITION BY XX.REJ_RSN_GRP, XX.REJ_RSN_CD ORDER BY XX.REJ_RSN_GRP) AS RN
FROM oracle.PMDW_MGR.DW_BA_CM_REJRSNINFO_M XX
WHERE XX.WAF_SIZE = '300'
AND XX.PROD_DIV_CD = CASE WHEN 'WF' = 'WF' THEN 'PW' ELSE 'EPI' END
) AS AFT_ALIAS
ON AFT_ALIAS.REJ_RSN_GRP = A.REJ_GROUP
AND AFT_ALIAS.REJ_RSN_CD = A.AFT_BAD_RSN_CD
AND AFT_ALIAS.RN = 1

JOIN oracle.PMDW_MGR.DW_BA_CM_BASEDATE_M B
ON B.BASE_DT = A.BASE_DT

LEFT JOIN oracle.PMDW_MGR.DW_BA_MS_PROD_M C
ON C.PROD_ID = A.PROD_ID
AND C.SPEC_DIV_CD = 'PS'

LEFT JOIN oracle.DMS_MGR.TB_FX_CODES D
ON D.UP_CD = 'DMS010'
AND D.SYS_CD = 'DMS'
AND D.CD_VAL = C.GRD_CD_NM

LEFT JOIN oracle.DMS_MGR.TB_FX_CODES E
ON E.UP_CD = 'DMS010'
AND E.SYS_CD = 'DMS'
AND E.CD_VAL = C.GRD_CD_NM_PS

JOIN oracle.PMDW_MGR.DW_BA_CM_STDPOPER_M Z1
ON Z1.FAC_ID = A.FAC_ID
AND Z1.OPER_ID = A.OPER_ID

WHERE
1 = 1
AND Z1.OPER_DIV_L = 'WF'
AND (CASE WHEN 'PN' = 'PN' THEN TRUE ELSE C.GRD_CD_NM = 'PN' END)
AND (CASE WHEN 'PN' = 'PN' THEN TRUE ELSE C.GRD_CD_NM_PS = 'PN' END)
AND Z1.FAC_ID IN ('WF7', 'WF8', 'WFA', 'FPC7', 'FPC8')
AND 'N' = 'N'
AND A.WAF_SIZE = '300'
AND A.BASE_DT = '{YESTERDAY}'  --  어제 하루만

GROUP BY
A.WAF_SIZE, A.BASE_DT, A.DIV_CD, A.REJ_DIV_CD, A.FAC_ID, A.OPER_ID,
A.OWNR_CD, A.CRET_CD, A.PROD_ID, A.IGOT_ID, A.BLK_ID, A.SUBLOT_ID,
A.USER_LOT_ID, A.EQP_ID, A.REJ_GROUP, A.OPER1_GROUP, A.OPER2_GROUP,
A.RESPON, A.ALLO_GROUP, A.RESPON_RATIO, A.REAL_DPT_GROUP,
D.CD_NM, E.CD_NM, C.CUST_SITE_NM, SUBSTR(B.BESOF_BASE_YW_NM, 3),
BEF_ALIAS.ALIAS_RSN_CD, AFT_ALIAS.ALIAS_RSN_CD,
A.BEF_BAD_RSN_CD, A.AFT_BAD_RSN_CD
),
-- (2) Z_WITH_PIMS: Z + PIMS_PROD 조인
Z_WITH_PIMS AS (
SELECT
Z.*,
P.CREQ_T1, P.CREQ_T2, P.CREQ_T3,
P.CREQ_V1, P.CREQ_V2, P.CREQ_V3
FROM Z
LEFT JOIN iceberg.ibg_lake.PIMS_PROD P
ON P.MS_CODE = Z.PROD_ID
AND P.SPEC_TYPE = 'CS'
)
--  최종 SELECT
SELECT
Z.*,
X1.EQP_NM,
X.TEAMGRP_NM,
X.SORT_CD,
COALESCE(X.TEAMGRP_NM, Z.REAL_DPT_GROUP) AS N_DPT_GROUP,
--  PART_NO: 일반 CASE 문 (상관 없음)
CASE
WHEN STRPOS(UPPER(Z.CREQ_T1), 'PART') > 0 THEN TRIM(SUBSTR(Z.CREQ_V1, STRPOS(Z.CREQ_V1, ':') + 1, 100))
WHEN STRPOS(UPPER(Z.CREQ_T2), 'PART') > 0 THEN TRIM(SUBSTR(Z.CREQ_V2, STRPOS(Z.CREQ_V2, ':') + 1, 100))
WHEN STRPOS(UPPER(Z.CREQ_T3), 'PART') > 0 THEN TRIM(SUBSTR(Z.CREQ_V3, STRPOS(Z.CREQ_V3, ':') + 1, 100))
ELSE ' '
END AS PART_NO
FROM Z_WITH_PIMS Z
--  Step 5: 팀부서그룹 매핑 (LATERAL)
LEFT JOIN LATERAL (
SELECT
S2.TEAMGRP_NM,
S2.SORT_SEQ AS SORT_CD
FROM oracle.PMDW_MGR.DW_BA_CM_LOSSREJGRPDTL_M S1
INNER JOIN oracle.PMDW_MGR.DW_BA_CM_LOSSREJGRP_M S2
ON S1.TEAMGRP_CD = S2.TEAMGRP_CD
AND S1.WAF_SIZE = S2.WAF_SIZE
AND S1.OPER_DIV_L = S2.OPER_DIV_L
WHERE
S1.TARGET_DIV_CD IN ('A', 'L')
AND S1.WAF_SIZE = '300'
AND S1.OPER_DIV_L = 'WF'
AND S1.ED_DT >= '{YESTERDAY}'  --  어제 포함 범위
AND S1.ST_DT <= '{YESTERDAY}'  --  어제 포함 범위
AND S1.DPT_CD = Z.REAL_DPT_GROUP
ORDER BY S1.ST_DT DESC
LIMIT 1
) X ON TRUE
--  Step 4: EQP_NM 매핑
LEFT JOIN oracle.PMDW_MGR.DW_BA_CM_STDPEQP_H X1
ON X1.FAC_ID = Z.FAC_ID
AND X1.EQP_ID = Z.EQP_ID
AND X1.ST_DT <= Z.BASE_DT
AND (X1.ED_DT >= Z.BASE_DT OR X1.ED_DT IS NULL OR X1.ED_DT = '99991231')
LEFT JOIN oracle.PMDW_MGR.DW_BA_CM_STDPEQP_M X2
ON X2.FAC_ID = X1.FAC_ID
AND X2.EQP_ID = X1.EQP_ID
AND X2.APPLY_YN = 'Y'
    """

    conn = None
    cur = None
    try:
        # 1. 연결 생성
        conn = create_trino_connection()
        print("🔗 Trino에 연결되었습니다.")

        # 2. 용량 사전 점검
        check_data_size_before_query(conn, QUERY)

        # 3. 실제 쿼리 실행
        cur = conn.cursor()
        print("\n✅ 실제 쿼리 실행 중...")
        cur.execute(QUERY)

        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=columns)

        print(f"✅ 데이터 로드 완료 | 행 수: {len(df)}, 열 수: {len(df.columns)}")
        print(df.head())

    except Exception as e:
        print(f"❌ 쿼리 실행 중 오류 발생: {e}")
        sys.exit(1)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("🔗 데이터베이스 연결이 종료되었습니다.")

# ✅ 실행
if __name__ == "__main__":
    main()
