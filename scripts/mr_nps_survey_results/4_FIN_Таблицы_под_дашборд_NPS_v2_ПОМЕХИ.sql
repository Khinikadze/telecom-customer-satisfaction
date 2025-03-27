----------------------------------------------------------------------
-- ШАГ 1. АНАЛИЗ ПЛОЩАДОК С ПОМЕХАМИ НА СОТОВЫХ СЕТЯХ ----------------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ:
-- Скрипт формирует витрину `fin_ba.mr_nps_survey_radioblock`,
-- где фиксируется, был ли абонент в месяце на сотах с проблемами
-- (в целом и в топ-5 местах). Проблемные соты определяются на основе
-- таблицы `mf_mcaas_qr.cells_interfer23G_m`, а информация о трафике — 
-- из `A_SUBS_BS_MONTHLY` и `a_SUBS_TOP5_PLACE_MONTHLY`.

DECLARE 
  start_dt DATE;     -- Начальная дата анализа
  end_dt   DATE;     -- Конечная дата анализа
  delta    NUMERIC;  -- Смещение по месяцам для источников (-1 = предыдущий месяц)
BEGIN
  start_dt := TO_DATE('01.02.2022', 'DD.MM.YYYY');
  end_dt   := TO_DATE('01.03.2023', 'DD.MM.YYYY');
  delta    := -1;

  ----------------------------------------------------------------------
  -- ШАГ 2. УДАЛЕНИЕ СТАРЫХ ДАННЫХ ЗА ПЕРИОД ---------------------------
  ----------------------------------------------------------------------

  DELETE FROM fin_ba.mr_nps_survey_radioblock
  WHERE start_date >= start_dt AND start_date < end_dt;

  ----------------------------------------------------------------------
  -- ШАГ 3. ВСТАВКА ДАННЫХ О ПРОБЛЕМНЫХ СОТАХ --------------------------
  ----------------------------------------------------------------------

  INSERT INTO fin_ba.mr_nps_survey_radioblock (
    start_date,
    sk_subs_id,
    has_traf_in_problemcell,
    has_traf_in_problemcell_top5
  )
  SELECT 
    snap_month AS start_date,
    sk_subs_id,
    SUM(problemcell)             AS has_traf_in_problemcell,
    SUM(problemcell_top5)       AS has_traf_in_problemcell_top5
  FROM (
    ------------------------------------------------------------------
    -- Подзапрос 1: Абонент пользовался трафиком на проблемных сотах
    ------------------------------------------------------------------

    SELECT 
      nps.snap_month,
      nps.sk_subs_id,
      SUM(problemcell)         AS problemcell,
      0                        AS problemcell_top5
    FROM (
      SELECT DISTINCT snap_month, sk_subs_id
      FROM fin_ba.mr_nps_survey_results_input
      WHERE snap_month >= start_dt AND snap_month < end_dt
    ) nps
    LEFT JOIN (
      SELECT /*+ parallel(8) */
             place_code, lac, cellid, sk_subs_id, start_date
      FROM pub_ds.A_SUBS_BS_MONTHLY
      WHERE start_date >= ADD_MONTHS(start_dt, delta)
        AND start_date <  ADD_MONTHS(end_dt, delta)
        AND (NVL(DURATION_MINUTES, 0) > 0 OR NVL(DURATION_MB, 0) > 0)
    ) bs
      ON nps.sk_subs_id = bs.sk_subs_id
     AND ADD_MONTHS(nps.snap_month, delta) = bs.start_date
    LEFT JOIN (
      SELECT DISTINCT /*+ parallel(8) */
             place_name, 1 AS problemcell
      FROM PUB_DS.H_NRI_BASE_STATION_ADV
      WHERE nms_cell_name IN (
        SELECT DISTINCT cellname
        FROM mf_mcaas_qr.cells_interfer23G_m
        WHERE start_date >= TO_DATE('01.01.2024', 'DD.MM.YYYY')
          AND start_date <= TO_DATE('01.04.2024', 'DD.MM.YYYY')
        GROUP BY cellname, branch, filial_id
        HAVING SUM(cnt_ovr) > 1000
      )
    ) radio_block
      ON bs.place_code = radio_block.place_name
    GROUP BY nps.snap_month, nps.sk_subs_id

    UNION ALL

    ------------------------------------------------------------------
    -- Подзапрос 2: Абонент был в топ-5 местах с проблемами
    ------------------------------------------------------------------

    SELECT 
      nps.snap_month,
      nps.sk_subs_id,
      0                        AS problemcell,
      SUM(problemcell)        AS problemcell_top5
    FROM (
      SELECT DISTINCT snap_month, sk_subs_id
      FROM fin_ba.mr_nps_survey_results_input
      WHERE snap_month >= start_dt AND snap_month < end_dt
    ) nps
    LEFT JOIN (
      SELECT /*+ parallel(8) */
             place_nri_id, sk_subs_id, start_date
      FROM PUB_DS.a_SUBS_TOP5_PLACE_MONTHLY
      WHERE start_date >= ADD_MONTHS(start_dt, delta)
        AND start_date <  ADD_MONTHS(end_dt, delta)
    ) bs
      ON nps.sk_subs_id = bs.sk_subs_id
     AND ADD_MONTHS(nps.snap_month, delta) = bs.start_date
    LEFT JOIN (
      SELECT DISTINCT /*+ parallel(8) */
             place_name, 1 AS problemcell
      FROM PUB_DS.H_NRI_BASE_STATION_ADV
      WHERE nms_cell_name IN (
        SELECT DISTINCT cellname
        FROM mf_mcaas_qr.cells_interfer23G_m
        WHERE start_date >= TO_DATE('01.01.2024', 'DD.MM.YYYY')
          AND start_date <  TO_DATE('01.04.2024', 'DD.MM.YYYY')
        GROUP BY cellname, branch, filial_id
        HAVING SUM(cnt_ovr) > 1000
      )
    ) radio_block
      ON bs.place_nri_id = radio_block.place_name
    GROUP BY nps.snap_month, nps.sk_subs_id
  )
  GROUP BY snap_month, sk_subs_id;

  ----------------------------------------------------------------------
  -- ШАГ 4. ЗАВЕРШЕНИЕ ТРАНЗАКЦИИ --------------------------------------
  ----------------------------------------------------------------------

  COMMIT;

END;