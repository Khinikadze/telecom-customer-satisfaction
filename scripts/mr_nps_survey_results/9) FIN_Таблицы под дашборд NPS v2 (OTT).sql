------------------------------------------------------------------------------------------
-- ШАГ 1. ИМПОРТ ДАННЫХ ПО ИСПОЛЬЗОВАНИЮ OTT-СЕРВИСОВ ДЛЯ АБОНЕНТОВ ----------------------
------------------------------------------------------------------------------------------

-- КОММЕНТАРИЙ: Данный блок предназначен для расчета объема потребления ключевых OTT-сервисов 
-- (YouTube, VK, WhatsApp, Telegram и пр.) и VPN по абонентам, участвовавшим в NPS-опросе.
-- Данные извлекаются из `a_subs_app_data`, агрегируются по абоненту и дате опроса, 
-- и сохраняются в таблицу `mr_nps_survey_results_ott`.

DECLARE 
  start_dt DATE;    -- Начальная дата периода анализа
  end_dt   DATE;    -- Конечная дата периода анализа
  delta    NUMERIC; -- Смещение месяца: 0 — текущий месяц, -1 — предыдущий
BEGIN
  -- Установка периода анализа
  start_dt := TO_DATE('01.01.2025', 'DD.MM.YYYY');
  end_dt   := TO_DATE('01.02.2025', 'DD.MM.YYYY');
  delta    := -1;

  --------------------------------------------------------------------------
  -- ШАГ 1.1. ОЧИСТКА ДАННЫХ ЗА УКАЗАННЫЙ ПЕРИОД ---------------------------
  --------------------------------------------------------------------------
  DELETE FROM fin_ba.mr_nps_survey_results_ott
   WHERE snap_date >= start_dt AND snap_date < end_dt;

  --------------------------------------------------------------------------
  -- ШАГ 1.2. ЗАГРУЗКА ДАННЫХ ПО КЛЮЧЕВЫМ OTT-СЕРВИСАМ ---------------------
  --------------------------------------------------------------------------

  -- КОММЕНТАРИЙ: Выбираются данные по YouTube, VK, Telegram, WhatsApp и пр.
  -- Объемы входящего и исходящего трафика фиксируются по дате опроса абонента.
  INSERT INTO fin_ba.mr_nps_survey_results_ott
  SELECT /*+ parallel(8) */
         nps.sk_subs_id,
         nps.snap_date,
         app.service_id,
         app.dl_volume,
         app.ul_volume
  FROM (
    SELECT DISTINCT filial_id, subs_subs_id, sk_subs_id, snap_date, snap_month
    FROM fin_ba.mr_nps_survey_results_tariff
    WHERE snap_date >= start_dt AND snap_date < end_dt
  ) nps
  INNER JOIN pub_ds.a_subs_app_data app
    ON app.filial_id = nps.filial_id
   AND app.subs_subs_id = nps.subs_subs_id
   AND app.month_stamp = ADD_MONTHS(nps.snap_month, delta)
  WHERE billing_filial_id = 10
    AND app.service_id IN (
         'vkontakte','vkontakte_video',
         'youtube','kinopoisk',
         'whatsapp','whatsapp_media',
         'whatsapp_videocall','whatsapp_call',
         'telegram'
    )
    AND app.month_stamp >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
    AND app.month_stamp < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta);

  --------------------------------------------------------------------------
  -- ШАГ 1.3. ЗАГРУЗКА ДАННЫХ ПО VPN-СЕРВИСАМ ------------------------------
  --------------------------------------------------------------------------

  -- КОММЕНТАРИЙ: VPN трафик агрегируется по `service_groupd_id = 'Tunneling'`.
  -- Для абонента рассчитываются суммарные объемы входящего и исходящего трафика.
  INSERT INTO fin_ba.mr_nps_survey_results_ott
  SELECT /*+ parallel(8) */
         nps.sk_subs_id,
         nps.snap_date,
         'VPN' AS service_id,
         SUM(app.dl_volume) AS dl_volume,
         SUM(app.ul_volume) AS ul_volume
  FROM (
    SELECT DISTINCT filial_id, subs_subs_id, sk_subs_id, snap_date, snap_month
    FROM fin_ba.mr_nps_survey_results_tariff
    WHERE snap_date >= start_dt AND snap_date < end_dt
  ) nps
  INNER JOIN pub_ds.a_subs_app_data app
    ON app.filial_id = nps.filial_id
   AND app.subs_subs_id = nps.subs_subs_id
   AND app.month_stamp = ADD_MONTHS(nps.snap_month, delta)
  WHERE billing_filial_id = 10
    AND app.service_groupd_id = 'Tunneling'
    AND app.month_stamp >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
    AND app.month_stamp < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
  GROUP BY nps.sk_subs_id, nps.snap_date;

  COMMIT;

END;

------------------------------------------------------------------------------------------
-- ШАГ 2. СОЗДАНИЕ VIEW ДЛЯ ПОВЕДЕНЧЕСКИХ ПРИЗНАКОВ НА ОСНОВЕ OTT-ТРАФИКА ---------------
------------------------------------------------------------------------------------------

-- КОММЕНТАРИЙ: Представление `mr_nps_survey_results_ott_view` агрегирует данные из таблицы
-- `mr_nps_survey_results_ott` по дате и абоненту, формируя отдельные признаки по каждому сервису.

-- DROP VIEW mr_nps_survey_results_ott_view;

CREATE VIEW mr_nps_survey_results_ott_view AS
SELECT sk_subs_id,
       snap_date,

       -- Признаки использования по каждому сервису
       "'kinopoisk'"          AS PROFILE_KINOPOISK,
       "'telegram'"           AS PROFILE_TELEGRAM,
       "'vkontakte'"          AS PROFILE_VKONTAKTE,
       "'vkontakte_video'"    AS PROFILE_VKONTAKTE_VIDEO,
       "'whatsapp'"           AS PROFILE_WHATSAPP,
       "'whatsapp_call'"      AS PROFILE_WHTSAPP_CALL,
       "'whatsapp_media'"     AS PROFILE_WHATSAPP_MEDIA,
       "'whatsapp_videocall'" AS PROFILE_WHATSAPP_VIDEOCALL,
       "'youtube'"            AS PROFILE_YOUTUBE,
       "'VPN'"                AS PROFILE_VPN

FROM (
  SELECT sk_subs_id,
         snap_date,
         service_id,
         dl_volume + ul_volume AS total
  FROM mr_nps_survey_results_ott
) raw

PIVOT (
  SUM(total)
  FOR service_id IN (
    'kinopoisk', 'telegram', 'vkontakte', 'vkontakte_video',
    'whatsapp', 'whatsapp_call', 'whatsapp_media',
    'whatsapp_videocall', 'youtube', 'VPN'
  )
);
