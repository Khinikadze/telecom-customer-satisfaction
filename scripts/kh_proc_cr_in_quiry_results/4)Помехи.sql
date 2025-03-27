----------------------------------------------------------------------
--ШАГ 1. АНАЛИЗ ПЛОЩАДОК С ПОМЕХАМИ-----------------------------------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ:
-- Скрипт формирует таблицу, в которой фиксируется информация:
-- - находился ли абонент в соте с помехами;
-- - находился ли он в ТОП-5 местах с помехами.
-- Источники: CR_INQUIRY_DAILY, A_SUBS_BS_MONTHLY, A_SUBS_TOP5_PLACE_MONTHLY,
--            H_NRI_BASE_STATION_ADV, CELLS_INTERFER23G_M

DECLARE 
  start_dt DATE;    -- Начальная дата периода анализа
  end_dt DATE;      -- Конечная дата периода анализа
  delta   NUMERIC;  -- Смещение периода анализа (-1 = прошлый месяц)
BEGIN
  -- Установка дат
  start_dt := TO_DATE('01.01.2025', 'DD.MM.YYYY');
  end_dt   := TO_DATE('01.03.2025', 'DD.MM.YYYY');
  delta    := -1;

  ----------------------------------------------------------------------
  --ШАГ 0. ФОРМИРОВАНИЕ ЦЕЛЕВОЙ ТАБЛИЦЫ --------------------------------
  ----------------------------------------------------------------------

  /*
  -- Технический блок создания таблицы
  DROP TABLE fin_ba.kh_proc_cr_inquiry_radioblock;
  CREATE TABLE fin_ba.kh_proc_cr_inquiry_radioblock
  (
    start_date                    DATE,      -- Месяц оценки
    sk_subs_id                    NUMBER,    -- ID абонента
    measure_id                    INTEGER,   -- ID метрики (не используется пока)
    has_traf_in_problemcell       NUMBER,   -- Был ли трафик на сотах с помехами
    has_traf_in_problemcell_top5  NUMBER    -- Был ли трафик в ТОП-5 местах с помехами
  );
  COMMIT;
  */

  -- Очистка данных за указанный период
  DELETE FROM fin_ba.kh_proc_cr_inquiry_radioblock
  WHERE start_date >= start_dt AND start_date < end_dt;

  ----------------------------------------------------------------------
  --ШАГ 1. ФОРМИРОВАНИЕ ДАННЫХ ПО ПРОБЛЕМНЫМ ПЛОЩАДКАМ ----------------
  ----------------------------------------------------------------------

  -- Вставка агрегированных значений в итоговую таблицу
  INSERT INTO fin_ba.kh_proc_cr_inquiry_radioblock (
    start_date, sk_subs_id,
    has_traf_in_problemcell,
    has_traf_in_problemcell_top5
  )
  SELECT snap_month AS start_date,
         sk_subs_id,
         SUM(problemcell) AS has_traf_in_problemcell,
         SUM(problemcell_top5) AS has_traf_in_problemcell_top5
  FROM (
  
    -- БЛОК 1: Абоненты с трафиком на сотах с помехами
    SELECT cr.snap_month, cr.sk_subs_id,
           SUM(problemcell) AS problemcell,
           0 AS problemcell_top5
    FROM (
      SELECT DISTINCT snap_month, sk_subs_id
      FROM fin_ba.KH_CR_INQUIRY_DAILY
      WHERE snap_month >= start_dt AND snap_month < end_dt
    ) cr
    LEFT OUTER JOIN (
      SELECT /*+ parallel (8)*/
             place_code, lac, cellid, sk_subs_id, start_date,
             NVL(duration_minutes, 0) AS dur_min,
             NVL(duration_mb, 0) AS dur_mb
      FROM pub_ds.A_SUBS_BS_MONTHLY
      WHERE start_date >= ADD_MONTHS(start_dt, delta)
        AND start_date <  ADD_MONTHS(end_dt, delta)
        AND (NVL(duration_minutes,0) > 0 OR NVL(duration_mb,0) > 0)
    ) bs
      ON cr.sk_subs_id = bs.sk_subs_id
     AND ADD_MONTHS(cr.snap_month, delta) = bs.start_date
    LEFT OUTER JOIN (
      SELECT DISTINCT /*+ parallel (8)*/ place_name, 1 AS problemcell
      FROM PUB_ds.H_NRI_BASE_STATION_ADV
      WHERE nms_cell_name IN (
        SELECT cellname
        FROM mf_mcaas_qr.cells_interfer23G_m
        WHERE start_date >= TO_DATE('01.01.2024', 'DD.MM.YYYY')
          AND start_date <= TO_DATE('01.04.2024', 'DD.MM.YYYY')
        GROUP BY cellname, branch, filial_id
        HAVING SUM(cnt_ovr) > 1000
      )
    ) radio_block
      ON bs.place_code = radio_block.place_name
    GROUP BY cr.snap_month, cr.sk_subs_id

    UNION ALL

    -- БЛОК 2: Абоненты в ТОП-5 локациях с помехами
    SELECT cr.snap_month, cr.sk_subs_id,
           0 AS problemcell,
           SUM(problemcell) AS problemcell_top5
    FROM (
      SELECT DISTINCT snap_month, sk_subs_id
      FROM fin_ba.KH_CR_INQUIRY_DAILY
      WHERE snap_month >= start_dt AND snap_month < end_dt
    ) cr
    LEFT OUTER JOIN (
      SELECT /*+ parallel (8)*/ 
             place_nri_id, sk_subs_id, start_date
      FROM PUB_DS.a_SUBS_TOP5_PLACE_MONTHLY
      WHERE start_date >= ADD_MONTHS(start_dt, delta)
        AND start_date <  ADD_MONTHS(end_dt, delta)
    ) bs
      ON cr.sk_subs_id = bs.sk_subs_id
     AND ADD_MONTHS(cr.snap_month, delta) = bs.start_date
    LEFT OUTER JOIN (
      SELECT DISTINCT /*+ parallel (8)*/ place_name, 1 AS problemcell
      FROM PUB_ds.H_NRI_BASE_STATION_ADV
      WHERE nms_cell_name IN (
        SELECT cellname
        FROM mf_mcaas_qr.cells_interfer23G_m
        WHERE start_date >= TO_DATE('01.01.2024', 'DD.MM.YYYY')
          AND start_date <  TO_DATE('01.04.2024', 'DD.MM.YYYY')
        GROUP BY cellname, branch, filial_id
        HAVING SUM(cnt_ovr) > 1000
      )
    ) radio_block
      ON bs.place_nri_id = radio_block.place_name
    GROUP BY cr.snap_month, cr.sk_subs_id

  ) t
  GROUP BY snap_month, sk_subs_id;

  -- Финализация транзакции
  COMMIT;

END;
