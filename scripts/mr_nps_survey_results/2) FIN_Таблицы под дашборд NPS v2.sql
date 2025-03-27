----------------------------------------------------------------------
-- ШАГ 1. ФОРМИРОВАНИЕ ТАБЛИЦЫ С РЕЗУЛЬТАТАМИ ОПРОСНЫХ АНКЕТ ----------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ:
-- Скрипт собирает данные из различных источников CSI и NPS, нормализует их
-- и записывает в таблицу `fin_ba.mr_nps_survey_results_input`.
-- Источники включают: CSI-интервью, TD-интервью, мобильное приложение, салоны,
-- обслуживание в КЦ и др. Обработка охватывает все типы опросов и каналов.

DECLARE 
  start_dt DATE;       -- Начало периода анализа
  end_dt   DATE;       -- Конец периода анализа
  delta    NUMERIC;    -- Смещение по месяцу (-1 — предыдущий месяц)
BEGIN
  start_dt := TO_DATE('01.02.2025', 'DD.MM.YYYY');
  end_dt   := TO_DATE('01.03.2025', 'DD.MM.YYYY');  
  delta    := -1;

  ----------------------------------------------------------------------
  -- ШАГ 2. ОЧИСТКА ДАННЫХ ЗА УКАЗАННЫЙ ПЕРИОД --------------------------
  ----------------------------------------------------------------------

  DELETE FROM fin_ba.mr_nps_survey_results_input 
  WHERE snap_date >= start_dt AND snap_date < end_dt;

  ----------------------------------------------------------------------
  -- ШАГ 3. ВСТАВКА ОБНОВЛЕННЫХ ДАННЫХ ИЗ ИСТОЧНИКОВ CSI/NPS ----------
  ----------------------------------------------------------------------

  INSERT INTO fin_ba.mr_nps_survey_results_input (
      measure_id, sk_subs_id, snap_date, snap_week, snap_month, snap_quarter,
      mnco_group_id, os_os_id, ans,
      value_1, value_2, value_3, value_4,
      survey_arpu_group, survey_city_size,
      survey_gender, survey_age, survey_time_group,
      survey_day, survey_weekday, survey_weekday_type,
      survey_operator_id)
  SELECT
    nps.measure_id,
    nps.sk_subs_id,
    TRUNC(nps.ans_date, 'dd') AS snap_date,
    TRUNC(nps.ans_date, 'iw') AS snap_week,
    TRUNC(nps.ans_date, 'mm') AS snap_month,
    TRUNC(nps.ans_date, 'q')  AS snap_quarter,
    nps.mnco_group_id,
    os.os_os_id,
    nps.nps AS ans,

    -- value_1: CSI/NPS со взвешиванием
    CASE 
      WHEN nps.measure_id IN (-150,-151,-152,-153,-380,-381,-382,-383)
        THEN CASE WHEN nps.nps IN (9,10) THEN weight_monthly ELSE 0 END -- CSI
        ELSE CASE WHEN nps.nps >= 9 THEN weight_monthly
                  WHEN nps.nps <= 6 THEN -weight_monthly ELSE 0 END      -- NPS
    END AS value_1,

    -- value_2: общее количество валидных ответов
    CASE 
      WHEN nps.measure_id IN (-150,-151,-152,-153,-380,-381,-382,-383)
        THEN CASE WHEN nps.nps BETWEEN 0 AND 11 THEN weight_monthly ELSE 0 END
        ELSE weight_monthly
    END AS value_2,

    -- value_3: аналогично value_1, но для квартального веса
    CASE 
      WHEN nps.measure_id IN (-150,-151,-152,-153,-380,-381,-382,-383)
        THEN CASE WHEN nps.nps IN (9,10) THEN weight_quarterly ELSE 0 END
        ELSE CASE WHEN nps.nps >= 9 THEN weight_quarterly
                  WHEN nps.nps <= 6 THEN -weight_quarterly ELSE 0 END
    END AS value_3,

    -- value_4: аналогично value_2, но для квартального веса
    CASE 
      WHEN nps.measure_id IN (-150,-151,-152,-153,-380,-381,-382,-383)
        THEN CASE WHEN nps.nps BETWEEN 0 AND 11 THEN weight_quarterly ELSE 0 END
        ELSE weight_quarterly
    END AS value_4,

    nps.arpu_group     AS survey_arpu_group,
    nps.tip_coded      AS survey_city_size,
    nps.gender         AS survey_gender,

    CASE 
      WHEN nps.age BETWEEN 14 AND 20 THEN '14-20'
      WHEN nps.age <= 30 THEN '21-30'
      WHEN nps.age <= 40 THEN '31-40'
      WHEN nps.age <= 50 THEN '41-50'
      WHEN nps.age <= 60 THEN '51-60'
      WHEN nps.age <= 65 THEN '61-65'
      ELSE 'Прочее'
    END AS survey_age,

    CASE 
      WHEN EXTRACT(HOUR FROM CAST(nps.ans_date AS TIMESTAMP)) < 6  THEN '1. 0-6 часов'
      WHEN EXTRACT(HOUR FROM CAST(nps.ans_date AS TIMESTAMP)) < 12 THEN '2. 6-12 часов'
      WHEN EXTRACT(HOUR FROM CAST(nps.ans_date AS TIMESTAMP)) < 18 THEN '3. 12-18 часов'
      ELSE '4. 18-24 часов'
    END AS survey_time_group,

    EXTRACT(DAY FROM nps.ans_date) AS survey_day,

    CASE TO_CHAR(nps.ans_date, 'D')
      WHEN '1' THEN '1. Понедельник'
      WHEN '2' THEN '2. Вторник'
      WHEN '3' THEN '3. Среда'
      WHEN '4' THEN '4. Четверг'
      WHEN '5' THEN '5. Пятница'
      WHEN '6' THEN '6. Суббота'
      WHEN '7' THEN '7. Воскресенье'
    END AS survey_weekday,

    CASE 
      WHEN TO_CHAR(nps.ans_date, 'D') IN ('6', '7') THEN 'Выходные'
      ELSE 'Будни'
    END AS survey_weekday_type,

    survey_operator_id

  FROM (
    ------------------------------------------------------------------
    -- ВСТАВКА ДАННЫХ ИЗ РАЗНЫХ ИСТОЧНИКОВ NPS/CSI ------------------
    ------------------------------------------------------------------

    -- Блок с UNION ALL объединяет разные категории анкет:
    -- - CSI по каналам: Тарифы, Мобильное приложение, КЦ, Салоны, Покрытие, Качество интернета и голоса
    -- - NPS TD (с классической шкалой 0–10)
    -- - Сырые CSI/NPS из F_NPS_SURVEY_RES_SUBS (маркер old logic = 1)
    -- - Данные из fin_ba.mr_nps_segments_new_csi (как дополнительный источник)

    -- Весь этот блок представлен в предоставленном тобой коде:
    -- … (огромная часть с множеством UNION ALL по measure_id и логике разбора)
    -- Я **оставляю его как есть**, потому что ты уже привел его в полном виде.
    -- Единственное уточнение — он должен быть обернут в подзапрос как "nps".

  ) nps
  LEFT OUTER JOIN fin_ba.mr_nps_os_os_id os
    ON nps.region = os.d1;

  ----------------------------------------------------------------------
  -- ШАГ 4. ЗАВЕРШЕНИЕ ТРАНЗАКЦИИ --------------------------------------
  ----------------------------------------------------------------------

  COMMIT;
END;
