----------------------------------------------------------------------
-- ШАГ 1. ПОЛУЧЕНИЕ ИНФОРМАЦИИ ОБ SMS-РАССЫЛКАХ ДЛЯ B2X --------------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ: Данный скрипт рассчитывает количество уникальных SMS-уведомлений,
-- отправленных от "megafon" за последние 30 дней до даты опроса по абонентам B2X,
-- участвующим в NPS-опросах с определёнными идентификаторами метрик (measure_id).
-- Результаты сохраняются в таблицу fin_ba.mr_nps_survey_results_b2x_sms.

-- ПРИМЕЧАНИЕ: Таблица создаётся один раз, если ещё не существует
/*
CREATE TABLE fin_ba.mr_nps_survey_results_b2x_sms
(
  sk_subs_id        NUMBER,   -- Уникальный идентификатор абонента
  snap_date         DATE,     -- Дата среза (дата опроса)
  all_mega_sms_cnt  NUMBER    -- Количество уникальных SMS от "megafon"
);
*/

-- Очистка таблицы перед вставкой новых данных
DELETE FROM fin_ba.mr_nps_survey_results_b2x_sms;

----------------------------------------------------------------------
-- ШАГ 2. ВСТАВКА ДАННЫХ ПО SMS-УВЕДОМЛЕНИЯМ -------------------------
----------------------------------------------------------------------

-- КОММЕНТАРИЙ: Подсчитываются уникальные SMS-уведомления (notification_id),
-- которые были отправлены от megafon по MSISDN абонента в течение 30 дней
-- до даты проведения опроса (snap_date). Используются только фактические отправки (SMGS_ID 2, 3, 8).

INSERT INTO fin_ba.mr_nps_survey_results_b2x_sms
SELECT /*+ parallel(12) */
       nps.sk_subs_id,
       nps.snap_date,
       COUNT(DISTINCT sms.notification_id) AS all_mega_sms_cnt
  FROM fin_ba.mr_nps_survey_results_input nps

  LEFT JOIN pub_ds.a_subs_kpis_monthly s
    ON nps.sk_subs_id = s.sk_subs_id
   AND nps.snap_month = s.start_date

  LEFT JOIN pub_ds.h_cnc_notifications sms
    ON s.msisdn = sms.address_to
   AND sms.order_send_date BETWEEN nps.snap_date - 30 AND nps.snap_date

 WHERE s.billing_filial_id = 10                        -- Только филиал 10
   AND s.filial_id BETWEEN 1 AND 8                     -- Диапазон филиалов
   AND LOWER(sms.address_from) = 'megafon'             -- Отправитель megafon
   AND sms.chnl_code = 'sms'                           -- Только канал sms
   AND sms.smgs_smgs_id IN (2, 3, 8)                   -- Только фактические отправки
   AND nps.measure_id IN (-643, -644)                  -- Целевые метрики NPS

GROUP BY nps.snap_date, nps.sk_subs_id;

-- Завершение транзакции
COMMIT;
