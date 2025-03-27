----------------------------------------------------------------------
-- ШАГ 1. ФОРМИРОВАНИЕ ВИТРИНЫ ПО ПЕРЕПЛАТАМ И VAS-УСЛУГАМ ----------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ: Скрипт собирает данные о переплатах за услуги и удержаниях
-- на основе данных из агрегированной таблицы `F_SUBS_AGGR_MONTHLY`, 
-- с привязкой к дате опроса из `mr_nps_survey_results_input`. 
-- Результаты вставляются в таблицу `fin_ba.mr_nps_survey_results_vas`.

-- ПАРАМЕТРЫ ЗАПУСКА
DECLARE 
  start_dt DATE;     -- Начало периода анализа
  end_dt   DATE;     -- Конец периода анализа
  delta    NUMERIC;  -- Смещение по месяцу (-1 — предыдущий месяц)
BEGIN
  start_dt := TO_DATE('01.01.2025', 'DD.MM.YYYY');
  end_dt   := TO_DATE('01.02.2025', 'DD.MM.YYYY');
  delta    := -1;

  ----------------------------------------------------------------------
  -- ШАГ 2. УДАЛЕНИЕ СТАРЫХ ДАННЫХ ЗА ПЕРИОД ---------------------------
  ----------------------------------------------------------------------

  DELETE FROM fin_ba.mr_nps_survey_results_vas
  WHERE snap_date >= start_dt AND snap_date < end_dt;

  ----------------------------------------------------------------------
  -- ШАГ 3. ВСТАВКА ДАННЫХ ПО ПЕРЕПЛАТАМ И VAS -------------------------
  ----------------------------------------------------------------------

  INSERT INTO fin_ba.mr_nps_survey_results_vas
  SELECT /*+ parallel(8)*/ 
         nps.sk_subs_id,
         nps.snap_date,
         nps.snap_month,

         -- Переплаты по направлениям
         SUM(CASE WHEN CH.DETG_DETG_ID IN (30, 3379, 22)
                   AND CH.LCAL_LCAL_ID IN (16,17,18,19,20,21,94)
                  THEN CH.AMOUNT ELSE 0 END) AS product_perepl_mn,   -- МН

         SUM(CASE WHEN CH.DETG_DETG_ID IN (21,30,3383)
                   AND CH.LCAL_LCAL_ID IN (7,14,15,227,228)
                  THEN CH.AMOUNT ELSE 0 END) AS product_perepl_gor,  -- Городские

         SUM(CASE WHEN CH.DETG_DETG_ID IN (21,30,3383,3380,3381)
                   AND CH.LCAL_LCAL_ID IN (
                       6,13,225,468,469,470,471,472,473,474,475,
                       476,477,478,479,480,481,482,483,484,485,486,
                       505,506,507,508,509,510)
                  THEN CH.AMOUNT ELSE 0 END) AS product_perepl_oper, -- Операторы

         SUM(CASE WHEN CH.DETG_DETG_ID IN (25,32,42)
                   AND CH.LCAL_LCAL_ID IN (
                       23,24,26,27,28,29,30,31,33,34,36,
                       552,553,554,555,556,487,488,489,490,491,492,
                       493,494,495,496,497,498,499,500,501,502,503,504)
                  THEN CH.AMOUNT ELSE 0 END) AS product_perepl_sms,  -- SMS

         -- VAS-услуги
         SUM(CASE WHEN CH.VAS_VAS_ID = 10600078 THEN CH.REVENUE ELSE 0 END) AS product_uderzh, -- Удержание
         SUM(CASE WHEN CH.VAS_VAS_ID = 10600050 THEN CH.REVENUE ELSE 0 END) AS product_kz,     -- Контроль звонка
         SUM(CASE WHEN CH.VAS_VAS_ID = 10600045 THEN CH.REVENUE ELSE 0 END) AS product_gpya,   -- Гудок
         SUM(CASE WHEN CH.VAS_VAS_ID = 10600095 THEN CH.REVENUE ELSE 0 END) AS product_zkz     -- Звонки за казнь
  FROM (
    SELECT DISTINCT sk_subs_id, snap_date, snap_month
    FROM mr_nps_survey_results_input
    WHERE snap_date >= start_dt AND snap_date < end_dt
  ) nps

  INNER JOIN pub_ds.F_SUBS_AGGR_MONTHLY CH
    ON CH.sk_subs_id = nps.sk_subs_id
   AND CH.start_date = ADD_MONTHS(nps.snap_month, delta)

  WHERE CH.billing_filial_id = 10
    AND CH.start_date >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
    AND CH.start_date < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
    AND (
      CH.VAS_VAS_ID IN (10600078, 10600050, 10600045, 10600095)
      OR CH.DETG_DETG_ID IN (
        30, 3379, 22, 
        21, 30, 3383,
        3380, 3381,
        25, 32, 42
      )
    )

  GROUP BY nps.sk_subs_id, nps.snap_date, nps.snap_month;

  ----------------------------------------------------------------------
  -- ШАГ 4. ЗАВЕРШЕНИЕ ТРАНЗАКЦИИ --------------------------------------
  ----------------------------------------------------------------------

  COMMIT;

END;
