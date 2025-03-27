----------------------------------------------------------------------
--ШАГ 1. ПОЛЬЗОВАНИЕ OTT-СЕРВИСАМИ ПО АБОНЕНТАМ-----------------------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ: Данный скрипт формирует таблицу с объемами использования
-- ключевых OTT-сервисов (YouTube, VK, WhatsApp, Telegram и др.) по абонентам,
-- на основе данных из таблицы kh_proc_cr_inquiry_results_tariff и app-data.

-- ОБЪЯВЛЕНИЕ БЛОКА
DECLARE 
  start_dt DATE;    -- Начальная дата периода анализа
  end_dt DATE;      -- Конечная дата периода анализа
  delta   NUMERIC;  -- Смещение: 0 — тот же месяц, -1 — предыдущий
BEGIN
  -- Установка значений параметров
  start_dt := TO_DATE('01.01.2025', 'DD.MM.YYYY');
  end_dt   := TO_DATE('01.03.2025', 'DD.MM.YYYY');
  delta    := -1;

  ----------------------------------------------------------------------
  --ШАГ 0. ПОДГОТОВКА ЦЕЛЕВОЙ ТАБЛИЦЫ ----------------------------------
  ----------------------------------------------------------------------

  /*
  -- DDL создания таблицы, если она еще не создана
  DROP TABLE fin_ba.kh_proc_cr_inquiry_results_ott;
  CREATE TABLE fin_ba.kh_proc_cr_inquiry_results_ott
  (
    sk_subs_id  INTEGER,             -- Уникальный ID абонента
    snap_date   DATE,                -- Дата среза
    service_id  VARCHAR2(100),       -- Название сервиса (YouTube, VK и т.д.)
    dl_volume   NUMBER,              -- Объем входящего трафика
    ul_volume   NUMBER               -- Объем исходящего трафика
  );
  COMMIT;
  */

  -- Очистка данных за указанный период
  DELETE FROM fin_ba.kh_proc_cr_inquiry_results_ott
  WHERE snap_date >= start_dt AND snap_date < end_dt;

  ----------------------------------------------------------------------
  --ШАГ 1. ВСТАВКА ДАННЫХ ПО КЛЮЧЕВЫМ СЕРВИСАМ (YouTube, VK и др.)
  ----------------------------------------------------------------------

  -- КОММЕНТАРИЙ: Получаем данные по сервисам OTT из app-data, 
  -- соединяя их с основной таблицей по абонентам и дате.
  INSERT INTO fin_ba.kh_proc_cr_inquiry_results_ott
  SELECT /*+ parallel(8)*/
         cr.sk_subs_id,
         cr.snap_date,
         app.service_id,
         app.dl_volume,
         app.ul_volume
  FROM (
    SELECT DISTINCT filial_id, subs_subs_id, sk_subs_id, snap_date, snap_month
    FROM fin_ba.kh_proc_cr_inquiry_results_tariff
    WHERE snap_date >= start_dt AND snap_date < end_dt
  ) cr
  INNER JOIN pub_ds.a_subs_app_data app
    ON app.filial_id = cr.filial_id
   AND app.subs_subs_id = cr.subs_subs_id
   AND app.month_stamp = ADD_MONTHS(cr.snap_month, delta)
  WHERE billing_filial_id = 10
    AND service_id IN (
         'vkontakte','vkontakte_video',
         'youtube','kinopoisk',
         'whatsapp','whatsapp_media',
         'whatsapp_videocall','whatsapp_call',
         'telegram'
    )
    AND app.month_stamp >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
    AND app.month_stamp < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta);

  ----------------------------------------------------------------------
  --ШАГ 2. ВСТАВКА ДАННЫХ ПО VPN-СЕРВИСАМ (Tunneling)
  ----------------------------------------------------------------------

  -- КОММЕНТАРИЙ: Группируем VPN-сервисы по ID-группе "Tunneling"
  -- и суммируем объемы по каждому абоненту и дате
  INSERT INTO fin_ba.kh_proc_cr_inquiry_results_ott
  SELECT /*+ parallel(8)*/ 
         cr.sk_subs_id,
         cr.snap_date,
         'VPN' AS service_id,
         SUM(app.dl_volume) AS dl_volume,
         SUM(app.ul_volume) AS ul_volume
  FROM (
    SELECT DISTINCT filial_id, subs_subs_id, sk_subs_id, snap_date, snap_month
    FROM fin_ba.kh_proc_cr_inquiry_results_tariff
    WHERE snap_date >= start_dt AND snap_date < end_dt
  ) cr
  INNER JOIN pub_ds.a_subs_app_data app
    ON app.filial_id = cr.filial_id
   AND app.subs_subs_id = cr.subs_subs_id
   AND app.month_stamp = ADD_MONTHS(cr.snap_month, delta)
  WHERE billing_filial_id = 10
    AND app.service_groupd_id = 'Tunneling'
    AND app.month_stamp >= ADD_MONTHS(TRUNC(start_dt, 'MM'), delta)
    AND app.month_stamp < ADD_MONTHS(TRUNC(end_dt, 'MM'), delta)
  GROUP BY cr.sk_subs_id, cr.snap_date;

  -- Завершаем транзакцию
  COMMIT;

END;
