------------------------------------------------------------------------------------------
-- ШАГ 1. ЗАГРУЗКА СЫРОГО ТРАФИКА АБОНЕНТОВ ЗА 7 ДНЕЙ ДО ОПРОСА --------------------------
------------------------------------------------------------------------------------------

-- КОММЕНТАРИЙ: Данный блок собирает данные о трафике (в МБ и минутах) за 7 дней до даты опроса.
-- Данные собираются из таблицы звонков, с классификацией трафика по крупным городам (Digital) и регионам (Oblast).
-- Далее результаты записываются в промежуточную таблицу `mr_nps_segments_season_traff`.

DECLARE 
  start_dt DATE;  -- Начало недели (понедельник)
  end_dt   DATE;  -- Конец недели (в данном случае — следующий понедельник)
BEGIN
  start_dt := TO_DATE('06.01.2025', 'dd.mm.yyyy');
  end_dt   := TO_DATE('13.01.2025', 'dd.mm.yyyy');

  WHILE start_dt <= end_dt LOOP

    -- Очистка данных по текущей неделе из целевой таблицы
    DELETE FROM fin_ba.mr_nps_segments_season_traff
     WHERE snap_week >= start_dt AND snap_week <= end_dt;

    --------------------------------------------------------------------------
    -- ШАГ 1.1. ВСТАВКА СЫРЫХ ДАННЫХ О ТРАФИКЕ В TEMP-ТАБЛИЦУ ----------------
    --------------------------------------------------------------------------

    -- КОММЕНТАРИЙ: Собираются объемы трафика по абонентам, разбивая на:
    -- 1. Digital City (предопределенный список крупных городов)
    -- 2. Oblast (все остальные города)
    -- Учитывается трафик в МБ и минутах отдельно.
    INSERT /*+ enable_parallel_dml parallel(6)*/ INTO fin_ba.mr_nps_segments_season_traff
    (
      snap_week, sk_subs_id,
      digital_city_mb, oblast_mb,
      digital_city_minutes, oblast_minutes
    )
    WITH rawdata AS (
      SELECT /*+ parallel(8)*/ DISTINCT sk_subs_id, snap_week, snap_date
      FROM fin_ba.mr_nps_survey_results_input 
      WHERE snap_week = start_dt
    ),
    substraf AS (
      SELECT /*+ parallel(8)*/
             snap_week, a.sk_subs_id,
             
             -- Сумма мегабайт по цифровым городам
             SUM(CASE WHEN r.settlement IN (<список_цифровых_городов>) THEN a.duration_mb END) AS digital_city_mb,

             -- Сумма мегабайт по регионам (все, что не цифровые города)
             SUM(CASE WHEN r.settlement NOT IN (<список_цифровых_городов>) THEN a.duration_mb END) AS oblast_mb,

             -- Сумма минут по цифровым городам
             SUM(CASE WHEN r.settlement IN (<список_цифровых_городов>) THEN a.duration_minutes END) AS digital_city_minutes,

             -- Сумма минут по регионам
             SUM(CASE WHEN r.settlement NOT IN (<список_цифровых_городов>) THEN a.duration_minutes END) AS oblast_minutes

      FROM PUB_DS.F_SUBS_CALLS_DIALED_DAILY a
      JOIN rawdata b ON a.sk_subs_id = b.sk_subs_id
      LEFT JOIN mf_mcaas_qr.d_gdc_fedcell r ON a.a_area = r.lac AND a.a_cell = r.cellid
      WHERE a.call_date BETWEEN (b.snap_date - 7) AND b.snap_date
      GROUP BY snap_week, a.sk_subs_id
    )
    SELECT * FROM substraf;

    -- Смещение периода на следующую неделю
    start_dt := start_dt + 7;
  END LOOP;
  COMMIT;

------------------------------------------------------------------------------------------
-- ШАГ 2. СЕГМЕНТИРОВАНИЕ АБОНЕНТОВ ПО ОСНОВНОМУ ТИПУ ТРАФИКА ---------------------------
------------------------------------------------------------------------------------------

-- КОММЕНТАРИЙ: На основе собранного трафика определяем преобладающий тип:
-- 1. Digital vs Oblast — по МБ и по минутам отдельно
-- 2. Далее объединяем результаты и определяем доминирующий канал: трафик или голос

-- Очистка целевой таблицы
DELETE FROM fin_ba.mr_nps_segments_season_traf_seg;
COMMIT;

-- Вставка сегментов
INSERT INTO fin_ba.mr_nps_segments_season_traf_seg
(
  snap_week, sk_subs_id, segment
)
SELECT 
  a.snap_week,
  a.sk_subs_id,
  CASE 
    WHEN prev_traf = 'mb' THEN segm_mb_65_35
    WHEN prev_traf = 'min' THEN segm_min_65_35
  END AS segm_by_sh
FROM (
  SELECT 
    seas_seg.*,

    -- Определение преобладающего трафика: mb или min
    CASE 
      WHEN min_sh > mb_sh THEN 'min'
      WHEN mb_sh > min_sh THEN 'mb'
      ELSE '-'
    END AS prev_traf
  FROM (
    SELECT 
      traf.sk_subs_id, traf.snap_week,

      -- Доли минут и мегабайт по неделе
      (NVL(traf.digital_city_minutes,0)+NVL(traf.oblast_minutes,0)) /
      NULLIF(SUM(NVL(traf.digital_city_minutes,0)+NVL(traf.oblast_minutes,0)) 
             OVER (PARTITION BY traf.snap_week),0) AS min_sh,

      (NVL(traf.digital_city_mb,0)+NVL(traf.oblast_mb,0)) /
      NULLIF(SUM(NVL(traf.digital_city_mb,0)+NVL(traf.oblast_mb,0)) 
             OVER (PARTITION BY traf.snap_week),0) AS mb_sh,

      -- Сегментация по мегабайтам: Digital / Oblast / Equals / Null
      CASE 
        WHEN NVL(traf.digital_city_mb,0)+NVL(traf.oblast_mb,0) IS NULL OR
             NVL(traf.digital_city_mb,0)+NVL(traf.oblast_mb,0) = 0 THEN 'Null'
        WHEN NVL(traf.digital_city_mb,0) > 0 AND NVL(traf.oblast_mb,0) = 0 THEN 'Digital'
        WHEN NVL(traf.digital_city_mb,0) = 0 AND NVL(traf.oblast_mb,0) > 0 THEN 'Oblast'
        WHEN NVL(traf.digital_city_mb,0) / (NVL(traf.digital_city_mb,0)+NVL(traf.oblast_mb,0)) >= 0.7 THEN 'Digital'
        WHEN NVL(traf.oblast_mb,0) / (NVL(traf.digital_city_mb,0)+NVL(traf.oblast_mb,0)) >= 0.7 THEN 'Oblast'
        ELSE 'Equals'
      END AS segm_mb_65_35,

      -- Сегментация по минутам: аналогично
      CASE 
        WHEN NVL(traf.digital_city_minutes,0)+NVL(traf.oblast_minutes,0) IS NULL OR
             NVL(traf.digital_city_minutes,0)+NVL(traf.oblast_minutes,0) = 0 THEN 'Null'
        WHEN NVL(traf.digital_city_minutes,0) > 0 AND NVL(traf.oblast_minutes,0) = 0 THEN 'Digital'
        WHEN NVL(traf.digital_city_minutes,0) = 0 AND NVL(traf.oblast_minutes,0) > 0 THEN 'Oblast'
        WHEN NVL(traf.digital_city_minutes,0) / (NVL(traf.digital_city_minutes,0)+NVL(traf.oblast_minutes,0)) >= 0.7 THEN 'Digital'
        WHEN NVL(traf.oblast_minutes,0) / (NVL(traf.digital_city_minutes,0)+NVL(traf.oblast_minutes,0)) >= 0.7 THEN 'Oblast'
        ELSE 'Equals'
      END AS segm_min_65_35,

      traf.digital_city_mb, traf.oblast_mb,
      traf.digital_city_minutes, traf.oblast_minutes

    FROM fin_ba.mr_nps_segments_season_traff traf
  ) seas_seg
) a;
COMMIT;
END;
