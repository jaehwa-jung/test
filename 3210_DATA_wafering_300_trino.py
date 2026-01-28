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

    
    QUERY = f"""
    -- =============================================
    -- [Trino] LossYieldService.SELECT_TEAM_LOSS_RATE (어제 자동 입력)
    -- =============================================
    WITH TBL_DTP_GRP AS (
        SELECT *
        FROM (
            SELECT 
                S2.TEAMGRP_NM, 
                S2.SORT_SEQ, 
                S1.DPT_CD, 
                ROW_NUMBER() OVER (PARTITION BY S1.DPT_CD ORDER BY ST_DT DESC) AS M
            FROM oracle.PMDW_MGR.DW_BA_CM_LOSSREJGRPDTL_M S1
            JOIN oracle.PMDW_MGR.DW_BA_CM_LOSSREJGRP_M S2
                ON S1.TEAMGRP_CD = S2.TEAMGRP_CD
                AND S1.WAF_SIZE = S2.WAF_SIZE
                AND S1.OPER_DIV_L = S2.OPER_DIV_L
                AND S1.TARGET_DIV_CD IN ('A','L')
                AND S1.WAF_SIZE = '300'
                AND S1.OPER_DIV_L = 'WF'
                AND S1.ED_DT >= '{YESTERDAY}'
                AND S1.ST_DT <= '{YESTERDAY}'
        ) A
        WHERE M = 1
    ),
    -- 일자 목록 생성 (어제 하루)
    DATE_LIST AS (
        SELECT 
            '{YESTERDAY}' AS BASE_DT,
            '{YESTERDAY_NM}' AS BASE_DT_NM
    ),
    -- 일별 목표(GOAL) 조회 + 중복 제거
    DAILY_GOAL AS (
        SELECT 
            Z.BASE_DT_NM,
            A.YLD_DIV3_CD AS REJ_GROUP,
            'D' AS CATEGORY,
            SUM(A.GOAL_VAL) AS GOAL_RATIO
        FROM DATE_LIST Z
        INNER JOIN (
            SELECT DISTINCT
                BASE_YM,
                WAF_SIZE,
                YLD_DIV1_CD,
                YLD_DIV3_CD,
                GOAL_DIV_CD,
                YLD_PLAN_TYPE,
                REF_DIV2,
                GOAL_VAL
            FROM oracle.PMDW_MGR.DW_BA_CM_YLDPLAN_M
            WHERE 
                WAF_SIZE = '300'
                AND YLD_DIV1_CD = 'WF'
                AND GOAL_DIV_CD = 'BAD-RATE'
                AND YLD_PLAN_TYPE = 'BP'
                AND REF_DIV2 = 'PN'
                AND BASE_YM = SUBSTR('{YESTERDAY}', 1, 6)  -- '202601'
        ) A
            ON A.BASE_YM = SUBSTR(Z.BASE_DT, 1, 6)
        GROUP BY 
            Z.BASE_DT_NM,
            A.YLD_DIV3_CD
    ),
    -- REJ_GROUP 목록 추출
    REJ_GROUP_LIST AS (
        SELECT DISTINCT REJ_GROUP
        FROM (
            SELECT REJ_GROUP FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLSTD_S
            WHERE WAF_SIZE = '300' AND BASE_DT = '{YESTERDAY}'
              AND DIV_CD <> 'COM_QTY'
            UNION
            SELECT REJ_GROUP FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S
            WHERE WAF_SIZE = '300' AND BASE_DT = '{YESTERDAY}'
              AND DIV_CD <> 'COM_QTY'
        ) A
    ),
    -- Loss 및 ComQty 통합
    LOSS_INFO AS (
        -- ---------------------------------------------------------- 통합 분자 (불량량)
        SELECT
            A.WAF_SIZE,
            B.OPER_DIV_L,
            A.BASE_DT,
            '' AS DIV_CD,
            A.REJ_GROUP,
            COALESCE(CASE WHEN A.DIV_CD = 'RESC_HG_QTY' THEN A.BEF_BAD_RSN_CD ELSE A.AFT_BAD_RSN_CD END, 'N/A') AS AFT_BAD_RSN_CD,
            COALESCE(E.TEAMGRP_NM, A.REAL_DPT_GROUP) AS REAL_DPT_GROUP,
            A.BEF_BAD_RSN_CD,
            SUM(A.LOSS_QTY) AS LOSS_QTY,
            0 AS LOSS_QTY_TOT,
            0 AS MGR_QTY,
            NULL AS MS_ID,
            NULL AS EQP_ID
        FROM (
            SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
            FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLSTD_S
            WHERE WAF_SIZE = '300' AND BASE_DT = '{YESTERDAY}'

            UNION ALL

            SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
            FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S
            WHERE WAF_SIZE = '300' AND BASE_DT = '{YESTERDAY}'
        ) A
        INNER JOIN oracle.PMDW_MGR.DW_BA_CM_STDPOPER_M B
            ON B.FAC_ID = A.FAC_ID AND B.OPER_ID = A.OPER_ID
           AND B.OPER_DIV_L = 'WF' AND B.FAC_ID IN ('WF7','WF8','WFA','FPC7','FPC8')
        LEFT JOIN oracle.PMDW_MGR.DW_BA_MS_PROD_M D
            ON D.PROD_ID = A.PROD_ID AND D.SPEC_DIV_CD = 'PS'
           AND (D.GRD_CD_NM = 'PN' OR D.GRD_CD_NM_PS = 'PN')
        LEFT JOIN (
            SELECT DPT_CD, TEAMGRP_NM, ROW_NUMBER() OVER (PARTITION BY DPT_CD ORDER BY SORT_SEQ) AS RN
            FROM TBL_DTP_GRP
        ) E ON E.DPT_CD = A.REAL_DPT_GROUP AND E.RN = 1
        WHERE
            A.DIV_CD <> 'COM_QTY'
            AND A.WAF_SIZE = '300'
            AND A.BASE_DT = '{YESTERDAY}'
            AND CONCAT(A.WAF_SIZE, B.OPER_DIV_L) NOT IN ('200WF', '300EPI')
        GROUP BY 
            A.WAF_SIZE, B.OPER_DIV_L, A.BASE_DT, A.REJ_GROUP,
            COALESCE(CASE WHEN A.DIV_CD = 'RESC_HG_QTY' THEN A.BEF_BAD_RSN_CD ELSE A.AFT_BAD_RSN_CD END, 'N/A'),
            COALESCE(E.TEAMGRP_NM, A.REAL_DPT_GROUP), A.BEF_BAD_RSN_CD

        UNION ALL

        -- ---------------------------------------------------------- 통합 분모 (합계량)
        SELECT
            A.WAF_SIZE,
            B.OPER_DIV_L,
            A.BASE_DT,
            'COM_QTY' AS DIV_CD,
            'TOTAL' AS REJ_GROUP,
            COALESCE(A.AFT_BAD_RSN_CD, 'N/A') AS AFT_BAD_RSN_CD,
            A.REAL_DPT_GROUP,
            NULL AS BEF_BAD_RSN_CD,
            0 AS LOSS_QTY,
            0 AS LOSS_QTY_TOT,
            SUM(A.IN_QTY) AS MGR_QTY,
            NULL AS MS_ID,
            NULL AS EQP_ID
        FROM (
            SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
            FROM oracle.PMDW_MGR.DM_PP_AC_TOTALFAULTDTLSTD_S
            WHERE WAF_SIZE = '300' AND BASE_DT = '{YESTERDAY}'

            UNION ALL

            SELECT WAF_SIZE, FAC_ID, BASE_DT, OPER_ID, REJ_GROUP, DIV_CD, BEF_BAD_RSN_CD, AFT_BAD_RSN_CD, REAL_DPT_GROUP, LOSS_QTY, IN_QTY, PROD_ID, EQP_ID
            FROM oracle.PMDW_MGR.DW_BA_CM_TOTALFAULTMANUAL_S
            WHERE WAF_SIZE = '300' AND BASE_DT = '{YESTERDAY}'
        ) A
        INNER JOIN oracle.PMDW_MGR.DW_BA_CM_STDPOPER_M B
            ON B.FAC_ID = A.FAC_ID AND B.OPER_ID = A.OPER_ID
           AND B.OPER_DIV_L = 'WF' AND B.FAC_ID IN ('WF7','WF8','WFA','FPC7','FPC8')
        WHERE
            A.DIV_CD = 'COM_QTY'
            AND A.WAF_SIZE = '300'
            AND A.BASE_DT = '{YESTERDAY}'
            AND CONCAT(A.WAF_SIZE, B.OPER_DIV_L) NOT IN ('200WF', '300EPI')
        GROUP BY 
            A.WAF_SIZE, B.OPER_DIV_L, A.BASE_DT, 
            COALESCE(A.AFT_BAD_RSN_CD, 'N/A'), A.REAL_DPT_GROUP
    ),
    -- 일별 Loss 정보
    MGR_LOSS_INFO AS (
        SELECT 
            Z.WAF_SIZE, 
            Z.OPER_DIV_L,
            date_format(date_parse(Z.BASE_DT, '%Y%m%d'), '%y-%m-%d') AS BASE_DT_NM,
            Z.REJ_GROUP, 
            Z.AFT_BAD_RSN_CD, 
            Z.BEF_BAD_RSN_CD,
            SUM(Z.LOSS_QTY) AS LOSS_QTY, 
            SUM(Z.LOSS_QTY_TOT) AS LOSS_QTY_TOT,
            'D' AS CATEGORY
        FROM LOSS_INFO Z
        WHERE Z.DIV_CD = ''
        GROUP BY 
            Z.WAF_SIZE, Z.OPER_DIV_L, Z.BASE_DT, Z.REJ_GROUP, Z.AFT_BAD_RSN_CD, Z.BEF_BAD_RSN_CD
    ),
    -- MGR_COMQTY_INFO: REJ_GROUP_LIST 기반 MGR_QTY 복제
    MGR_COMQTY_INFO AS (
        SELECT 
            C.WAF_SIZE, 
            C.OPER_DIV_L,
            C.BASE_DT_NM,
            R.REJ_GROUP,
            C.COM_QTY,
            C.MGR_QTY,
            C.CATEGORY
        FROM (
            SELECT 
                Z.WAF_SIZE, 
                Z.OPER_DIV_L,
                date_format(date_parse(Z.BASE_DT, '%Y%m%d'), '%y-%m-%d') AS BASE_DT_NM,
                SUM(Z.LOSS_QTY) AS COM_QTY,
                SUM(Z.MGR_QTY) AS MGR_QTY,
                'D' AS CATEGORY
            FROM LOSS_INFO Z
            WHERE Z.DIV_CD = 'COM_QTY'
            GROUP BY 
                Z.WAF_SIZE, Z.OPER_DIV_L, Z.BASE_DT
        ) C
        CROSS JOIN REJ_GROUP_LIST R
    ),
    -- 최종 데이터 조합
    FINAL_DATA AS (
        SELECT 
            L.CATEGORY,
            L.BASE_DT_NM,
            L.REJ_GROUP,
            L.AFT_BAD_RSN_CD,
            CAST(CASE WHEN C.MGR_QTY > 0 THEN CAST(L.LOSS_QTY AS DOUBLE) / NULLIF(C.MGR_QTY, 0) ELSE 0.0 END AS DECIMAL(24,16)) AS LOSS_RATIO,
            CAST(COALESCE(G.GOAL_RATIO, 0.0) AS DECIMAL(24,16)) AS GOAL_RATIO,
            CAST(COALESCE(G.GOAL_RATIO, 0.0) AS DECIMAL(24,16)) AS GOAL_RATIO_SUM,
            CAST(CASE WHEN C.MGR_QTY > 0 THEN (CAST(L.LOSS_QTY AS DOUBLE) / NULLIF(C.MGR_QTY, 0)) - COALESCE(G.GOAL_RATIO, 0.0) ELSE -COALESCE(G.GOAL_RATIO, 0.0) END AS DECIMAL(24,16)) AS GAP_RATIO,
            L.LOSS_QTY,
            C.MGR_QTY,
            CAST(NULL AS DECIMAL(24,16)) AS COM_QTY,
            99999 AS SORT_CD,
            'N/A' AS PROD_GRP,
            'N/A' AS EQP_NM,
            'N/A' AS EQP_MODEL_NM,
            '일' AS CATEGORY_NAME
        FROM MGR_LOSS_INFO L
        LEFT JOIN MGR_COMQTY_INFO C
            ON C.BASE_DT_NM = L.BASE_DT_NM
           AND C.REJ_GROUP = L.REJ_GROUP
        LEFT JOIN DAILY_GOAL G
            ON G.BASE_DT_NM = L.BASE_DT_NM
           AND G.REJ_GROUP = L.REJ_GROUP
    )
    -- 최종 출력
    SELECT * FROM FINAL_DATA
    ORDER BY BASE_DT_NM, REJ_GROUP, LOSS_QTY DESC
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
    
