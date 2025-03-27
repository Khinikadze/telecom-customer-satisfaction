----------------------------------------------------------------------
--ШАГ 1. АНАЛИЗ ПОЛЬЗОВАНИЯ VAS-ПРОДУКТАМИ----------------------------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ:
-- Скрипт собирает информацию по использованию дополнительных платных VAS-услуг 
-- (переадресации, удержание, контент и пр.) на основе агрегированной ежемесячной 
-- информации из F_SUBS_AGGR_MONTHLY. Используется в анализе клиентского опыта.

-- ИНИЦИАЛИЗАЦИЯ ПАРАМЕТРОВ
DECLARE 
  start_dt DATE;    -- Начальная дата периода анализа
  end_dt DATE;      -- Конечная дата периода анализа
  delta   NUMERIC;  -- Смещение по месяцу (-1 = предыдущий месяц)
BEGIN
  start_dt := TO_DATE('01.01.2025', 'DD.MM.YYYY');
  end_dt   := TO_DATE('01.03.2025', 'DD.MM.YYYY');
  delta    := -1;

  ----------------------------------------------------------------------
  --ШАГ 0. ПОДГОТОВКА ЦЕЛЕВОЙ ТАБЛИЦЫ ----------------------------------
  ----------------------------------------------------------------------

  /*
  DROP TABLE fin_ba.kh_proc_cr_inquiry_results_vas;
  CREATE TABLE fin_ba.kh_proc_cr_inquiry_results_vas
  (
    sk_subs_id             INTEGER,     -- Уникальный ID абонента
    snap_date              DATE,        -- Дата среза
    snap_month             DATE,        -- Месяц анализа
    product_perepl_mn      NUMBER,      -- Переадресация на мобильный
    product_perepl_gor     NUMBER,      -- Переадресация на городской
    product_perepl_oper    NUMBER,      -- Переадресация на других операторов
    product_perepl_sms     NUMBER,      -- Переадресация SMS
    product_uderzh         NUMBER,      -- Удержание 
    product_kz             NUMBER,      -- Контент 
    product_gpya           NUMBER,      -- Голос. помощник 
    product_zkz            NUMBER       -- Знания  
  );
  COMMIT;
  */

  -- Удаление предыдущих данных за период
  DELETE FROM fin_ba.kh_proc_cr_inquiry_results_vas
  WHERE snap_date >= start_dt AND snap_date < end_dt;

  ----------------------------------------------------------------------
  --ШАГ 1. ЗАПОЛНЕНИЕ ДАННЫХ ПО VAS-УСЛУГАМ-----------------------------
  ----------------------------------------------------------------------

  -- Вставка агрегированных значений по категориям VAS-услуг
  INSERT INTO fin_ba.kh_proc_cr_inquiry_results_vas
  SELECT /*+ parallel(8)*/
         cr.sk_subs_id,
         cr.snap_date,
         cr.snap_month,

         -- Переадресация на мобильные направления
         SUM(CASE 
             WHEN CH.DETG_DETG_ID IN (30,3379,22) 
              AND CH.LCAL_LCAL_ID IN (16,17,18,19,20,21,94)
             THEN CH.AMOUNT ELSE 0 END) AS product_perepl_mn,

         -- Переадресация на городские номера
         SUM(CASE 
             WHEN CH.DETG_DETG_ID IN (21,30,3383) 
              AND CH.LCAL_LCAL_ID IN (7,14,15,227,228)
             THEN CH.AMOUNT ELSE 0 END) AS product_perepl_gor,

         -- Переадресация на других операторов
         SUM(CASE 
             WHEN CH.DETG_DETG_ID IN (21,30,3383,3380,3381) 
              AND CH.LCAL_LCAL_ID IN (
                  6,13,225,468,469,470,471,472,473,474,475,476,477,478,479,
                  480,481,482,483,484,485,486,505,506,507,508,509,510)
             THEN CH.AMOUNT ELSE 0 END) AS product_perepl_oper,

         -- Переадресация SMS
         SUM(CASE 
             WHEN CH.DETG_DETG_ID IN (25,32,42) 
              AND CH.LCAL_LCAL_ID IN (
                  23,24,26,27,28,29,30,31,33,34,36,
                  552,553,554,555,556,
                  487,488,489,490,491,492,493,494,495,496,
                  497,498,499,500,501,502,503,504)
             THEN CH.AMOUNT ELSE 0 END) AS product_perepl_sms,

         -- Удержание вызова
         SUM(CASE 
             WHEN CH.VAS_VAS_ID = 10600078 
             THEN CH.REVENUE ELSE 0 END) AS product_uderzh,

         -- Контент 
         SUM(CASE 
             WHEN CH.VAS_VAS_ID = 10600050 
             THEN CH.REVENUE ELSE 0 END) AS product_kz,

         -- Голосовой помощник 
         SUM(CASE 
             WHEN CH.VAS_VAS_ID = 10600045 
             THEN CH.REVENUE ELSE 0 END) AS product_gpya,

         -- Знания 
         SUM(CASE 
             WHEN CH.VAS_VAS_ID = 10600095 
             THEN CH.REVENUE ELSE 0 END) AS product_zkz

  FROM (
    -- Абоненты, заданный период
    SELECT DISTINCT sk_subs_id, snap_date, snap_month
    FROM fin_ba.KH_CR_INQUIRY_DAILY
    WHERE snap_date >= start_dt AND snap_date < end_dt
  ) cr

  -- Источник фактических начислений по услугам
  INNER JOIN pub_ds.F_SUBS_AGGR_MONTHLY ch 
    ON ch.sk_subs_id = cr.sk_subs_id
   AND ch.start_date = ADD_MONTHS(cr.snap_month, delta)

  -- Фильтрация нужных периодов и типов услуг
  WHERE ch.billing_filial_id = 10
    AND ch.start_date >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
    AND ch.start_date <  ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
    AND (
      ch.VAS_VAS_ID IN (10600078, 10600050, 10600045, 10600095)
      OR ch.DETG_DETG_ID IN (
        30,3379,22,21,3383,3380,3381,25,32,42
      )
    )
  GROUP BY cr.sk_subs_id, cr.snap_date, cr.snap_month;

  -- Завершение транзакции
  COMMIT;

END;
