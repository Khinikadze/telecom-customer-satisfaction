----------------------------------------------------------------------
-- ШАГ 1. ПОДГОТОВКА CSI-РЕЗУЛЬТАТОВ ПО ОПРОСАМ -----------------------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ: Данный скрипт собирает CSI-результаты из опросов клиентов,
-- обрабатывая данные из источника F_NPS_SURVEY_RESULTS и рассчитывая значения
-- CSI (Customer Satisfaction Index) по ключевым метрикам. Результаты записываются
-- в таблицу fin_ba.mr_nps_segments_new_csi.

-- Просмотр текущих агрегированных данных по CSI
-- SELECT measure_id, snap_date, SUM(value_2) FROM fin_ba.mr_nps_segments_new_csi GROUP BY measure_id, snap_date;

-- Получение последней даты среза
-- SELECT MAX(snap_date) FROM fin_ba.mr_nps_segments_new_csi;

-- Пример запроса агрегации
SELECT 
  measure_id, 
  snap_month, 
  SUM(value_1),
  SUM(value_2),
  SUM(value_1) / SUM(value_2)
FROM fin_ba.mr_nps_segments_new_csi 
WHERE measure_id = -378
GROUP BY measure_id, snap_month;

----------------------------------------------------------------------

-- ОБЪЯВЛЕНИЕ ПЕРЕМЕННЫХ ДЛЯ УПРАВЛЕНИЯ ПЕРИОДОМ ----------------------

DECLARE 
  start_dt DATE;      -- Начало периода обработки
  end_dt   DATE;      -- Конец периода обработки
  delta    NUMERIC;   
BEGIN
  start_dt := TO_DATE('21.02.2025', 'DD.MM.RRRR');
  end_dt   := TO_DATE('23.02.2025', 'DD.MM.RRRR');

  ----------------------------------------------------------------------
  -- ШАГ 2. ОЧИСТКА СТАРЫХ ДАННЫХ ЗА УКАЗАННЫЙ ПЕРИОД ------------------
  ----------------------------------------------------------------------

  DELETE FROM fin_ba.mr_nps_segments_new_csi   
  WHERE snap_date >= start_dt AND snap_date <= end_dt;

  ----------------------------------------------------------------------
  -- ШАГ 3. ЗАПИСЬ ОБНОВЛЕННЫХ ДАННЫХ ПО CSI ---------------------------
  ----------------------------------------------------------------------

  INSERT INTO fin_ba.mr_nps_segments_new_csi 
  SELECT
    measure_id,
    a.sk_subs_id,
    ans_date AS snap_date,
    TRUNC(ans_date, 'IW') AS snap_week,
    TRUNC(ans_date, 'MM') AS snap_month,
    TRUNC(ans_date, 'Q')  AS snap_quarter,
    1 AS mnco_group_id,
    os_os_id,
    CAST(score AS NUMERIC) AS nps,
    value_1,
    value_2,
    value_1 AS value_3,
    value_2 AS value_4,
    NULL AS survey_arpu_group,
    NULL AS survey_city_size,
    b.gender AS survey_gender,
    CASE 
      WHEN age >= 14 AND age <= 20 THEN '14-20'
      WHEN age <= 30 THEN '21-30'
      WHEN age <= 40 THEN '31-40'
      WHEN age <= 50 THEN '41-50'
      WHEN age <= 60 THEN '51-60'
      WHEN age <= 65 THEN '61-65'
      ELSE 'Прочее'
    END AS survey_age,
    CASE 
      WHEN EXTRACT(HOUR FROM CAST(ans_date AS TIMESTAMP)) < 6 THEN '1. 0-6 часов'
      WHEN EXTRACT(HOUR FROM CAST(ans_date AS TIMESTAMP)) < 12 THEN '2. 6-12 часов'
      WHEN EXTRACT(HOUR FROM CAST(ans_date AS TIMESTAMP)) < 18 THEN '3. 12-18 часов'
      ELSE '4. 18-24 часов'
    END AS survey_time_group,
    EXTRACT(DAY FROM ans_date) AS survey_day,
    CASE TO_CHAR(ans_date, 'D')
      WHEN '1' THEN '1. Понедельник'
      WHEN '2' THEN '2. Вторник'
      WHEN '3' THEN '3. Среда'
      WHEN '4' THEN '4. Четверг'
      WHEN '5' THEN '5. Пятница'
      WHEN '6' THEN '6. Суббота'
      WHEN '7' THEN '7. Воскресенье'
    END AS survey_weekday,
    CASE 
      WHEN TO_CHAR(ans_date, 'D') IN ('6', '7') THEN 'Выходные'
      ELSE 'Будни'
    END AS survey_weekday_type,
    NULL AS survey_operator_id
  FROM (
    -- ШАГ 3.1: Получение агрегированных результатов по CSI из сырых анкет
    WITH 
    TABLE_RAW AS (
      SELECT /*+ PARALLEL(8)*/
             rwt.survey_program_medalia, 
             rwt.survey_id,
             rwt.creation_date,
             TO_CHAR(rwt.creation_date, 'YYYY.MM') AS creation_month,
             rwt.msisdn,
             rwt.sk_subs_id,
             rwt.list_id,
             rwt.creation_date AS event_dttm,
             MIN(CASE WHEN rwt.key_name = 'ANS1_T' THEN rwt.key_value END) AS score,
             MIN(CASE WHEN rwt.key_name = 'SURVEY_ID' THEN rwt.key_value END) AS surv_id,
             MIN(CASE WHEN rwt.key_name = 'marker_old_logic' THEN 
                      CASE WHEN rwt.key_value = '1' OR rwt.key_value IS NULL THEN 1 ELSE 0 END
                 END) AS current_logic,
             MIN(CASE WHEN rwt.key_name = 'SUBS_ACTIVATION_DATE' THEN rwt.key_value END) AS subs_activation_date,
             MIN(CASE WHEN rwt.key_name = 'OS' THEN rwt.key_value END) AS os
      FROM pub_ds.f_nps_survey_results rwt 
      WHERE TRUNC(rwt.creation_date, 'DD') BETWEEN start_dt AND end_dt
        AND rwt.key_name IN ('ANS1TIME', 'ANS1_T', 'marker_old_logic', 'SUBS_ACTIVATION_DATE', 'OS', 'SURVEY_ID')
        AND rwt.survey_program_medalia IS NOT NULL
        AND rwt.survey_id IS NOT NULL
        AND rwt.msisdn IS NOT NULL
      GROUP BY rwt.creation_date, rwt.survey_program_medalia, rwt.survey_id, rwt.msisdn, rwt.sk_subs_id, rwt.list_id
    ),
    
    TABLE_NORMALIZED AS (
      SELECT /*+ PARALLEL(8)*/
             rw.survey_program_medalia AS survey_program,
             rw.survey_id,
             rw.msisdn,
             rw.sk_subs_id,
             rw.creation_date AS ans_date,
             TO_DATE(rw.event_dttm) AS snap_date,
             rw.event_dttm,
             rw.score,
             rw.surv_id,
             rw.list_id,
             rw.subs_activation_date,
             rw.os,
             ROW_NUMBER() OVER (PARTITION BY rw.creation_month, rw.survey_program_medalia, rw.survey_id, rw.msisdn ORDER BY rw.creation_date) AS row_num
      FROM TABLE_RAW rw
      WHERE rw.event_dttm IS NOT NULL
        AND rw.score IN ('0','1','2','3','4','5','6','7','8','9','10')
        AND (rw.current_logic = 1 OR rw.current_logic IS NULL)
    ),

    TABLE_TOTAL AS (
      SELECT /*+ PARALLEL(8)*/
             TRUNC(r.snap_date, 'MM') AS snap_month,
             r.snap_date,
             r.ans_date,
             r.sk_subs_id,
             r.survey_program,
             r.surv_id,
             r.list_id,
             r.score,
             CASE 
               WHEN r.survey_program = 10102 AND r.list_id = 1851 THEN -379  -- B2C CSI Тарифа
               WHEN r.survey_program = 50110 AND r.list_id = 561  THEN -378  -- B2C CSI Качество голоса
               WHEN r.survey_program = 40110 AND r.list_id = 502  THEN -377  -- B2C CSI МИ
               ELSE 0 
             END AS measure_id,
             SUM(r.val1) AS value_1,
             SUM(r.val2) AS value_2
      FROM (
        SELECT /*+ PARALLEL(8)*/
               tw2.snap_date,
               tw2.sk_subs_id,
               tw2.score,
               TRUNC(tw2.ans_date, 'DD') AS ans_date,
               tw2.survey_program,
               tw2.surv_id,
               tw2.list_id,
               MIN(CASE WHEN tw2.score IN ('9','10') THEN 1 ELSE 0 END) AS val1,
               NULLIF(COUNT(CASE WHEN tw2.score != '0' THEN 1 END), 0) AS val2
        FROM TABLE_NORMALIZED tw2
        WHERE tw2.row_num = 1
        GROUP BY tw2.snap_date, tw2.ans_date, tw2.score, tw2.survey_program, tw2.sk_subs_id, tw2.surv_id, tw2.list_id
      ) r
      GROUP BY r.snap_date, r.ans_date, r.sk_subs_id, r.survey_program, r.surv_id, r.list_id, r.score
      ORDER BY r.snap_date
    )

    SELECT * FROM TABLE_TOTAL a
    LEFT OUTER JOIN pub_ds.s_subs_pyramid_daily p
      ON a.sk_subs_id = p.sk_subs_id AND a.ans_date = p.snap_date
    LEFT OUTER JOIN pub_ds.s_subs_clnt_info b
      ON p.billing_filial_id = b.billing_filial_id AND
         a.sk_subs_id = b.sk_subs_id AND
         TRUNC(a.ans_date, 'MM') = b.snap_date
    WHERE measure_id != 0
  );

  -- Завершение транзакции
  COMMIT;
END;
