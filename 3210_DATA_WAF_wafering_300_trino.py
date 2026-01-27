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
        print(f" EXPLAIN 분석 중 오류 발생: {e}")
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

    print(f"일자: 어제 ({YESTERDAY})")

    
    # 쿼리 동적 생성 (f-string 사용)
    QUERY = f"""
WITH step1_base AS (
    SELECT 
        A.WAF_ID, A.WAF_SEQ, A.WAF_SIZE, A.BASE_DT, A.DIV_CD, A.REJ_DIV_CD,
        A.FAC_ID, A.OPER_ID, A.OWNR_CD, A.CRET_CD, A.PROD_ID, A.IGOT_ID,
        A.BLK_ID, A.SUBLOT_ID, A.USER_LOT_ID, A.EQP_ID, A.BEF_BAD_RSN_CD,
        A.AFT_BAD_RSN_CD, A.REJ_GROUP, A.OPER1_GROUP, A.OPER2_GROUP,
        A.RESPON, A.ALLO_GROUP, A.RESPON_RATIO, A.IN_QTY, A.OUT_QTY,
        A.LOSS_QTY, A.REAL_DPT_GROUP, A.HST_REG_DTTM, 'ORI' AS DATA_TYPE, A.DATA_CHG_DTTM
    FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLWAFSTD_S A
    WHERE A.WAF_SIZE = '300'
      AND A.BASE_DT = '{YESTERDAY}'        -- ✅ 어제 날짜 자동 삽입
      AND A.FAC_ID IN ('WF7','WF8','WFA','FPC7','FPC8')

    UNION ALL

    SELECT 
        A.WAF_ID, A.WAF_SEQ, A.WAF_SIZE, A.BASE_DT, A.DIV_CD, A.REJ_DIV_CD,
        A.FAC_ID, A.OPER_ID, A.OWNR_CD, A.CRET_CD, A.PROD_ID, A.IGOT_ID,
        A.BLK_ID, A.SUBLOT_ID, A.USER_LOT_ID, A.EQP_ID, A.BEF_BAD_RSN_CD,
        A.AFT_BAD_RSN_CD, A.REJ_GROUP, A.OPER1_GROUP, A.OPER2_GROUP,
        A.RESPON, A.ALLO_GROUP, A.RESPON_RATIO, A.IN_QTY, A.OUT_QTY,
        A.LOSS_QTY, A.REAL_DPT_GROUP, NULL AS HST_REG_DTTM, 'MNL' AS DATA_TYPE, A.DATA_CHG_DTTM
    FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S A
    WHERE A.WAF_SIZE = '300'
      AND A.BASE_DT = '{YESTERDAY}'        -- ✅ 어제 날짜 자동 삽입
      AND A.FAC_ID IN ('WF7','WF8','WFA','FPC7','FPC8')
),
step2_joined AS (
    SELECT 
        b.*,
        bd.BASE_DT AS BASE_DT_NAME,
        mp.CUST_SITE_NM,
        mp.GRD_CD_NM AS GRD_CD_NM_CS,
        mp.GRD_CD_NM_PS,
        so.OPER_DIV_L,
        SUBSTR(bd.BESOF_BASE_YW_NM, 3) AS WEEK_DAY_NM  -- 예: '26-04'
    FROM step1_base b
    JOIN oracle.PMDW_MGR.DW_BA_CM_BASEDATE_M bd ON bd.BASE_DT = b.BASE_DT
    LEFT JOIN oracle.PMDW_MGR.DW_BA_MS_PROD_M mp ON mp.PROD_ID = b.PROD_ID AND mp.SPEC_DIV_CD = 'PS'
    JOIN oracle.PMDW_MGR.DW_BA_CM_STDPOPER_M so ON so.FAC_ID = b.FAC_ID AND so.OPER_ID = b.OPER_ID
    WHERE 
        so.OPER_DIV_L = 'WF'
        AND b.FAC_ID IN ('WF7','WF8','WFA','FPC7','FPC8')
        AND (CASE WHEN 'PN' = 'PN' THEN TRUE ELSE mp.GRD_CD_NM = 'PN' END)
        AND (CASE WHEN 'PN' = 'PN' THEN TRUE ELSE mp.GRD_CD_NM_PS = 'PN' END)
),
step3_rej_alias AS (
    WITH rej_alias_map AS (
        SELECT 
            REJ_RSN_GRP,
            REJ_RSN_CD,
            ALIAS_RSN_CD
        FROM oracle.PMDW_MGR.DW_BA_CM_REJRSNINFO_M
        WHERE WAF_SIZE = '300'
          AND PROD_DIV_CD = CASE WHEN 'WF' = 'WF' THEN 'PW' ELSE 'EPI' END
    )
    SELECT 
        j.WAF_ID,
        j.WAF_SEQ,
        j.WAF_SIZE,
        j.BASE_DT,
        j.DIV_CD,
        j.REJ_DIV_CD,
        j.FAC_ID,
        j.OPER_ID,
        j.OWNR_CD,
        j.CRET_CD,
        j.PROD_ID,
        j.IGOT_ID,
        j.BLK_ID,
        j.SUBLOT_ID,
        j.USER_LOT_ID,
        j.EQP_ID,
        j.CUST_SITE_NM,
        j.REJ_GROUP,
        j.OPER1_GROUP,
        j.OPER2_GROUP,
        j.RESPON,
        j.ALLO_GROUP,
        j.RESPON_RATIO,
        j.IN_QTY,
        j.OUT_QTY,
        j.LOSS_QTY,
        j.REAL_DPT_GROUP,
        j.HST_REG_DTTM,
        j.DATA_TYPE,
        j.DATA_CHG_DTTM,
        j.BASE_DT_NAME,
        j.GRD_CD_NM_CS,
        j.GRD_CD_NM_PS,
        j.OPER_DIV_L,
        j.WEEK_DAY_NM,
        COALESCE(ram1.ALIAS_RSN_CD, j.BEF_BAD_RSN_CD) AS BEF_BAD_RSN_CD,
        COALESCE(ram2.ALIAS_RSN_CD, j.AFT_BAD_RSN_CD) AS AFT_BAD_RSN_CD
    FROM step2_joined j
    LEFT JOIN rej_alias_map ram1 
        ON ram1.REJ_RSN_GRP = j.REJ_GROUP 
       AND ram1.REJ_RSN_CD = j.BEF_BAD_RSN_CD
    LEFT JOIN rej_alias_map ram2 
        ON ram2.REJ_RSN_GRP = j.REJ_GROUP 
       AND ram2.REJ_RSN_CD = j.AFT_BAD_RSN_CD
),
-- ✅ STEP 5: F_GET_PART_NO 정확 재현 (이미지 데이터 기반)
step5_part_no AS (
    WITH part_no_source AS (
        SELECT 
            a.ms_code AS PROD_ID,
            CASE 
                WHEN STRPOS(UPPER(a.creq_t1), 'PART') > 0 THEN TRIM(a.creq_v1)
                WHEN STRPOS(UPPER(a.creq_t2), 'PART') > 0 THEN TRIM(a.creq_v2)
                WHEN STRPOS(UPPER(a.creq_t3), 'PART') > 0 THEN TRIM(a.creq_v3)
                ELSE ' '
            END AS PART_NO
        FROM iceberg.ibg_lake.pims_prod a
        WHERE a.spec_type = 'CS'
    )
    SELECT 
        j.*,
        COALESCE(pns.PART_NO, ' ') AS PART_NO
    FROM step3_rej_alias j
    LEFT JOIN part_no_source pns 
        ON pns.PROD_ID = j.PROD_ID
)

SELECT *
FROM step5_part_no
    """

    conn = None
    cur = None
    try:
        # 1. 연결 생성
        conn = create_trino_connection()
        print("Trino에 연결되었습니다.")

        # 2. 용량 사전 점검
        check_data_size_before_query(conn, QUERY)

        # 3. 실제 쿼리 실행
        cur = conn.cursor()
        print("\n실제 쿼리 실행 중...")
        cur.execute(QUERY)

        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=columns)

        print(f"데이터 로드 완료 | 행 수: {len(df)}, 열 수: {len(df.columns)}")
        print(df.head())

    except Exception as e:
        print(f"쿼리 실행 중 오류 발생: {e}")
        sys.exit(1)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("데이터베이스 연결이 종료되었습니다.")

# 실행
if __name__ == "__main__":
    main()
    
