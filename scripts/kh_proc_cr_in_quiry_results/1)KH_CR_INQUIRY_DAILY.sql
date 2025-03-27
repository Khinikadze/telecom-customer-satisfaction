DECLARE
  -- Период, за который формируется выборка
  start_dt DATE; -- начало периода 
  end_dt DATE;   -- конец периода 

BEGIN
  -- Установка диапазона дат
  start_dt := to_date('01.01.2025','dd.mm.yyyy'); -- начальная дата
  end_dt   := to_date('01.03.2025','dd.mm.yyyy'); -- конечная дата

  ----------------------------------------------------------------------
  -- ШАГ 0. Очистка таблицы результатов, чтобы записать новые данные --
  ----------------------------------------------------------------------

  -- Если необходимо пересоздать таблицу 
  /*
  DROP TABLE fin_ba.KH_CR_INQUIRY_DAILY;
  CREATE TABLE fin_ba.KH_CR_INQUIRY_DAILY
  (
         COMP_OS_ID NUMERIC ,
         BILLING_FILIAL_ID NUMERIC, 
         ITPC_ITPC_ID NUMERIC, 
         SK_SUBS_ID NUMBER(18),
         snap_date DATE,
         snap_week DATE,
         snap_month DATE,
         snap_quarter DATE,
         SK_BMT_ID NUMBER(10),
         SUBS_OS_OS_ID NUMBER(4),
         CR_OS_OS_ID NUMBER(4),
         inqr_id  number(18) ,
         LAT NUMBER(13,10),    
         LON NUMBER(13,10),
         DESCRIPTION VARCHAR2(4000),
         CRCAT_CRCAT_ID NUMBER(2),
         CR_WEIGHT NUMBER(4,2),
         CODE_LEVEL_1 VARCHAR2(128),
         CODE_LEVEL_2 VARCHAR2(128),
         CODE_LEVEL_3 VARCHAR2(128),
         CODE_LEVEL_4 VARCHAR2(128),
         FULL_NAME VARCHAR2(4000),
         FLAG_LOCAL_GENERAL NUMERIC,
         filial_id number ,
         subcat_pyramid_name	varchar2(50),
         cat_pyramid_name	varchar2(50),
         SUBS_SUBS_ID number
  );
  COMMIT;
  */

  -- Удаление старых записей за указанный период
  DELETE FROM fin_ba.KH_CR_INQUIRY_DAILY 
  WHERE snap_date >= start_dt 
    AND snap_date < end_dt;

  ----------------------------------------------------------------------
  -- ШАГ 1. Вставка свежих данных в результирующую таблицу ----------
  ----------------------------------------------------------------------

  INSERT INTO fin_ba.KH_CR_INQUIRY_DAILY (
    COMP_OS_ID, BILLING_FILIAL_ID, ITPC_ITPC_ID, SK_SUBS_ID,
    snap_date, snap_week, snap_month, snap_quarter,
    SK_BMT_ID, SUBS_OS_OS_ID, CR_OS_OS_ID, inqr_id, LAT,
    LON, DESCRIPTION, CRCAT_CRCAT_ID, CR_WEIGHT,
    CODE_LEVEL_1, CODE_LEVEL_2, CODE_LEVEL_3, CODE_LEVEL_4,
    FULL_NAME, FLAG_LOCAL_GENERAL, filial_id,
    subcat_pyramid_name, cat_pyramid_name, SUBS_SUBS_ID
  )
  SELECT
    cr.COMP_OS_ID,
    cr.BILLING_FILIAL_ID,
    cr.ITPC_ITPC_ID,
    cr.SK_SUBS_ID,
    trunc(cr.CREATE_DATE, 'dd') AS snap_date,
    trunc(cr.CREATE_DATE, 'iw') AS snap_week,
    trunc(cr.CREATE_DATE, 'mm') AS snap_month,
    trunc(cr.CREATE_DATE, 'q') AS snap_quarter,
    cr.SK_BMT_ID,
    cr.SUBS_OS_OS_ID,
    cr.CR_OS_OS_ID,
    inqr_id,
    cr.LAT,
    cr.LON,
    cr.DESCRIPTION,
    cr.CRCAT_CRCAT_ID,
    cr.CR_WEIGHT,
    t.CODE_LEVEL_1,
    t.CODE_LEVEL_2,
    t.CODE_LEVEL_3,
    t.CODE_LEVEL_4,
    t.FULL_NAME,
    a.flag_indicator,
    sp.filial_id,
    pc.subcat_pyramid_name,
    pc.cat_pyramid_name,
    sp.SUBS_SUBS_ID
  FROM PUB_DS.F_CR_INQUIRY_DAILY cr

  -- Присоединение справочника тем обращений
  LEFT JOIN pub_ds.H_TOPIC t
    ON t.ITPC_ID = cr.ITPC_ITPC_ID
   AND t.BILLING_FILIAL_ID = cr.BILLING_FILIAL_ID
   AND t.START_DATE <= start_dt 
   AND t.END_DATE > end_dt

  -- Присоединение пирамиды абонента на конкретную дату
  LEFT JOIN pub_ds.s_subs_pyramid_daily sp
    ON sp.SK_SUBS_ID = cr.SK_SUBS_ID
   AND sp.snap_date = trunc(cr.CREATE_DATE, 'dd')
   AND sp.snap_date >= start_dt 
   AND sp.snap_date < end_dt

  -- Присоединение категорий пирамиды аббонента
  LEFT JOIN PUB_DS.D_PYRAMID_CATEGORY pc 
    ON pc.SUBCAT_PYRAMID_ID = sp.SUBCAT_PYRAMID_ID_3M

  -- Вычисление отклонений и флагов аномалий
  LEFT JOIN (
    SELECT
      a.os_os_id,
      a.sk_bmt_id,
      a.year,
      a.month,
      a.week,
      a.day,
      SUM(flag_indicator) AS flag_indicator
    FROM (
      SELECT
        *,
        CASE WHEN flag_indicator = 1 THEN c - c_minimum ELSE 0 END AS kol_poim_otkl_1,
        CASE WHEN flag_indicator = 1 THEN c - m ELSE 0 END AS kol_poim_otkl_2
      FROM (
        SELECT 
          *,
          ABS((0.6745 * (c - m)) / NULLIF(mad, 0)) AS Z_modif,
          c_maximum - c_minimum AS maxmin,
          CASE
            WHEN c_maximum - c_minimum < 3 THEN 0
            WHEN c - c_minimum >= 10 THEN 1
            WHEN ABS((0.6745 * (c - m)) / NULLIF(mad, 0)) > 3 THEN 1
            ELSE 0
          END AS flag_indicator
        FROM (
          SELECT 
            *,
            MEDIAN(ABS(c - sr)) OVER (PARTITION BY os_os_id, sk_bmt_id, year, month, week) AS mad
          FROM (
            SELECT 
              *,
              MEDIAN(c) OVER (PARTITION BY os_os_id, sk_bmt_id, year, month, week) AS m,
              AVG(c) OVER (PARTITION BY os_os_id, sk_bmt_id, year, month, week) AS sr,
              MAX(ABS(c)) OVER (PARTITION BY os_os_id, sk_bmt_id, year, month, week) AS c_maximum,
              MIN(ABS(c)) OVER (PARTITION BY os_os_id, sk_bmt_id, year, month, week) AS c_minimum
            FROM (
              SELECT 
                b.os_os_id,
                a.sk_bmt_id,
                EXTRACT(YEAR FROM create_date) AS year,
                EXTRACT(MONTH FROM create_date) AS month,
                CEIL(EXTRACT(DAY FROM create_date) / 7) AS week,
                EXTRACT(DAY FROM create_date) AS day,
                COUNT(*) AS c
              FROM PUB_DS.F_CR_INQUIRY_DAILY a
              LEFT JOIN PUB_DS.D_BMT b ON a.sk_bmt_id = b.sk_bmt_id AND b.end_date > SYSDATE
              LEFT JOIN (
                SELECT DISTINCT OS_OS_ID, RO 
                FROM map_ds.m_org_struct_bill_v 
                WHERE end_date > SYSDATE
              ) c ON c.os_os_id = b.os_os_id
              WHERE crcat_crcat_id IN (1,3,4,6,7,8,9,10)
                AND create_date >= start_dt 
                AND create_date < end_dt
                AND crcat_crcat_id IS NOT NULL
                AND COMP_OS_ID = 150
              GROUP BY b.os_os_id, a.sk_bmt_id, EXTRACT(YEAR FROM create_date), 
                       EXTRACT(MONTH FROM create_date), 
                       CEIL(EXTRACT(DAY FROM create_date) / 7),  
                       EXTRACT(DAY FROM create_date)
            ) a
          ) a
        ) a
      ) a
    ) a
    GROUP BY a.os_os_id, a.sk_bmt_id, a.year, a.month, a.week, a.day
  ) a 
    ON a.os_os_id = cr.cr_os_os_id 
   AND a.sk_bmt_id = cr.sk_bmt_id 
   AND a.os_os_id IS NOT NULL
   AND EXTRACT(YEAR FROM cr.CREATE_DATE) = a.YEAR
   AND EXTRACT(MONTH FROM cr.CREATE_DATE) = a.MONTH
   AND CEIL(EXTRACT(DAY FROM cr.CREATE_DATE) / 7) = a.WEEK
   AND EXTRACT(DAY FROM cr.CREATE_DATE) = a.day

  -- Ограничение по дате
  WHERE cr.CREATE_DATE >= start_dt 
    AND cr.CREATE_DATE < end_dt;

  COMMIT;

END;