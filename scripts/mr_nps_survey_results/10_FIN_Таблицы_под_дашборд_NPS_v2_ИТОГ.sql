-----------------------------------------------------------------------------------
-- ШАГ 1. СОЗДАНИЕ ВРЕМЕННОЙ ТАБЛИЦЫ ---------------------------------------------
-----------------------------------------------------------------------------------

-- КОММЕНТАРИЙ:
-- Создается временная таблица `mr_nps_survey_results_temp` на основе основной таблицы `mr_nps_survey_results`
-- без данных. Таблица используется как staging-область для последующей очистки, обогащения и категоризации данных
-- по результатам NPS-опросов. Позволяет безопасно тестировать логику обработки перед записью в основную таблицу.

CREATE TABLE fin_ba.mr_nps_survey_results_temp AS 
SELECT * FROM fin_ba.mr_nps_survey_results
WHERE 1=2;

-----------------------------------------------------------------------------------
-- ШАГ 2. ЗАПОЛНЕНИЕ ВРЕМЕННОЙ ТАБЛИЦЫ ОБОГАЩЕННЫМИ ДАННЫМИ -----------------------
-----------------------------------------------------------------------------------

-- КОММЕНТАРИЙ:
-- Происходит массовая вставка данных по абонентам, участвовавшим в NPS-опросе.
-- Основная таблица `mr_nps_survey_results_tariff` объединяется с различными источниками:
--  • OTT и VAS-платформы
--  • региональные признаки (ABC, улучшения, конкуренция)
--  • сведения о тарифе на момент опроса
--  • признаки сегментов и кампаний (репрайсинг, оптауты, реактивации)
--  • использование услуг (YouTube, Telegram, WhatsApp и др.)
--  • характеристики SIM (eSIM, мультисим, тип SIM), история MNP
--  • push-коммуникации, SQI, категории нагрузки и покрытия
-- 
-- Вычисления включают масштабное применение CASE для категоризации непрерывных признаков
-- (доход, трафик, lifetime, SQI, использование OTT и др.) в интервал/уровень.

INSERT INTO fin_ba.mr_nps_survey_results_temp (
  MEASURE_ID, FILIAL_ID, SK_SUBS_ID, 
  SNAP_DATE, SNAP_WEEK, SNAP_MONTH, SNAP_QUARTER, 
  MNCO_GROUP_ID, OS_OS_ID,  
  ANS, VALUE_1, VALUE_2, VALUE_3, VALUE_4,  
  SURVEY_ARPU_GROUP, SURVEY_CITY_SIZE, SURVEY_GENDER,
  SURVEY_AGE, SURVEY_TIME_GROUP, SURVEY_DAY, 
  SURVEY_WEEKDAY, SURVEY_WEEKDAY_TYPE,  
  PRODUCT_FINBLOCK, PRODUCT_MEGA_SMS, PRODUCT_MLK, PRODUCT_TARIFF_CHANGE,
  PROFILE_MOU, PROFILE_DOU, PRODUCT_REVENUE_WO_IITC_VAS_ROAM, PROFILE_MEGA_CIRCLE_PRC,
  PROFILE_DEVICE_PRICE, PROFILE_DEVICE_TYPE, PROFILE_LIFETIME, SEGMENT_NAME,
  PRODUCT_TP_FEE, PROFILE_UTIL_VOICE, PROFILE_UTIL_DATA, PRODUCT_NAME_LINE_TP,
  PRODUCT_GROUP_TARIFF, NETWORK_4G_SHARE, NETWORK_PROSTOI, NETWORK_VOLTE,
  NETWORK_VOWIFI, PRODUCT_CD_REPRICE_2022, PRODUCT_CD_REP1_2023, PRODUCT_CD_REPOP_2023,
  PRODUCT_CD_REPSIMP_2023, NETWORK_OVERLOAD, PROFILE_DEVICE_OS, NETWORK_SQI_VOICE,
  NETWORK_SPEED_LTE, PRODUCT_BNS, NETWORK_DURATION_2G, NETWORK_DURATION_4G,
  NETWORK_SPD_LOWER_THAN_2000, PRODUCT_PERENOS, PRODUCT_EVA, PRODUCT_FAMILY,
  PRODUCT_PRE5G, PRODUCT_MEGASIL_BESPL, PRODUCT_ULUCH, PRODUCT_CD_PERENOS,
  PRODUCT_CD_EVA, PRODUCT_CD_FAMILY, PRODUCT_CD_PRE5G, PRODUCT_CD_MEGASIL_BESPL,
  PRODUCT_CD_ULUCH, PRODUCT_PEREPLATY, PROFILE_KINOPOISK, PROFILE_TELEGRAM,
  PROFILE_VKONTAKTE, PROFILE_VKONTAKTE_VIDEO, PROFILE_WHATSAPP, PROFILE_WHTSAPP_CALL,
  PROFILE_WHATSAPP_VIDEOCALL, PROFILE_YOUTUBE, PROFILE_VPN, REGION_ABC_TOTAL,
  REGION_SQI_TREND, REGION_COMPETITOR, REGION_ABC_COVERAGE, PRODUCT_CD_LINE_TP,
  PRODUCT_CD_GROUP_TARIFF, PRODUCT_PERS, REGION_REFARMING, REGION_IMPROVEMENT,
  REGION_SVO, REGION_FILIAL, NETWORK_SERV_MODEL, NETWORK_CD_SERV_MODEL,
  PRODUCT_CD_DEBITOR, PRODUCT_CD_REACTIV, PRODUCT_CD_RISK_BASE, PRODUCT_DEBITOR,
  NETWORK_SQI_DATA, NETWORK_RADIOBLOCK, PROFILE_COMPLEX_FACTOR_PRODUCT,
  PROFILE_COMPLEX_FACTOR_NETWORK, SURVEY_OPERATOR_GENDER, SURVEY_OPERATOR_AGE,
  PRODUCT_CD_REP1_2024, PROFILE_CD_MULTI_SIM, PRODUCT_CD_REP6_2024,
  NETWORK_SEASON_TRAF, PRODUCT_CD_EVA_TYPE, PRODUCT_EVA_TYPE, PRODUCT_CD_REP9_2024,
  REGION_RO, PRODUCT_CD_REP01_2025, PRODUCT_SIM_TYPE, PROFILE_CD_YOUTUBE,
  PROFILE_CD_TELEGRAM, PRODUCT_MNP_IN
)

SELECT DISTINCT
  -- Абонент, дата, опрос
  tariff.MEASURE_ID, tariff.FILIAL_ID, tariff.SK_SUBS_ID,
  tariff.SNAP_DATE, tariff.SNAP_WEEK, tariff.SNAP_MONTH, tariff.SNAP_QUARTER,
  tariff.MNCO_GROUP_ID, tariff.OS_OS_ID, 
  tariff.ANS, tariff.VALUE_1, tariff.VALUE_2, tariff.VALUE_3, tariff.VALUE_4, 
  tariff.SURVEY_ARPU_GROUP, tariff.SURVEY_CITY_SIZE, tariff.SURVEY_GENDER,
  tariff.SURVEY_AGE, tariff.SURVEY_TIME_GROUP, tariff.SURVEY_DAY,
  tariff.SURVEY_WEEKDAY, tariff.SURVEY_WEEKDAY_TYPE,

  -- Признак использования ФинБлоков (кол-во дней в финблоке за период)
  CASE 
    WHEN NVL(DAYS_IN_FB_S12,0) = 0 THEN '1. Нет ФБ'
    WHEN NVL(DAYS_IN_FB_S12,0) < 1 THEN '2. Менее 1 дня'
    WHEN NVL(DAYS_IN_FB_S12,0) < 5 THEN '3. 1-5 дней'  
    WHEN NVL(DAYS_IN_FB_S12,0) < 30 THEN '4. 5-30 дней'    
    ELSE '5. Более 30 дней' 
  END AS PRODUCT_FINBLOCK,

  -- Объем исходящих SMS (по флагу all_mega_sms_cnt)
  CASE 
    WHEN NVL(sms.all_mega_sms_cnt,0) = 0 THEN '1. Нет SMS'
    WHEN NVL(sms.all_mega_sms_cnt,0) < 5 THEN '2. Менее 5 SMS'
    WHEN NVL(sms.all_mega_sms_cnt,0) < 10 THEN '3. 5-10 SMS'  
    WHEN NVL(sms.all_mega_sms_cnt,0) < 30 THEN '4. 11-20 SMS'    
    ELSE '5. Более 21 SMS' 
  END AS PRODUCT_MEGA_SMS,

  -- Посещения Личного кабинета (МЛК)
  CASE 
    WHEN NVL(CNT_LK,0) = 0 THEN '1. Нет заходов'
    WHEN NVL(CNT_LK,0) <= 1 THEN '2. 1 заход' 
    WHEN NVL(CNT_LK,0) <= 3 THEN '3. 1-3 захода'  
    ELSE '4. Более 3 заходов' 
  END AS PRODUCT_MLK,

  -- Сколько дней назад было изменение тарифа
  CASE 
    WHEN DAYS_SINCE_CHNG IS NULL THEN '6. Прочее'
    WHEN NVL(DAYS_SINCE_CHNG,0) < 30 THEN '1. До 30 дней'
    WHEN NVL(DAYS_SINCE_CHNG,0) < 90 THEN '2. 31-90 дней'
    WHEN NVL(DAYS_SINCE_CHNG,0) < 180 THEN '3. 91-180 дней'  
    WHEN NVL(DAYS_SINCE_CHNG,0) < 366 THEN '4. 181-366 дней'  
    ELSE '5. Более 366 дней' 
  END AS PRODUCT_TARIFF_CHANGE,

  -- Минуты разговора (MOU)
  CASE 
    WHEN NVL(MOU_M1,0) = 0 THEN '1. Нет'
    WHEN NVL(MOU_M1,0) < 5 THEN '2. До 5'
    WHEN NVL(MOU_M1,0) < 100 THEN '3. 5-100'
    WHEN NVL(MOU_M1,0) < 500 THEN '4. 100-500'    
    ELSE '5. Более 500' 
  END AS PROFILE_MOU,

  -- Объем трафика (DOU)
  CASE 
    WHEN NVL(DOU_M1,0) = 0 THEN '1. Нет' 
    WHEN NVL(DOU_M1,0) < 100 THEN '2. До 100 МБ'
    WHEN NVL(DOU_M1,0) < 1000 THEN '3. 0.1 - 1 ГБ' 
    WHEN NVL(DOU_M1,0) < 10000 THEN '4. 1 - 10 ГБ' 
    ELSE '5. Более 10 ГБ' 
  END AS PROFILE_DOU,

  -- Доход за 3 мес без ИТК/ВАС/роуминга
  CASE 
    WHEN NVL(REVENUE_WO_ITC_VAS_ROAM_MA3,0) < 100 THEN '1. До 100 руб'
    WHEN NVL(REVENUE_WO_ITC_VAS_ROAM_MA3,0) < 500 THEN '2. 101-500 руб'
    ELSE '3. Более 500 руб' 
  END AS PRODUCT_REVENUE_WO_IITC_VAS_ROAM,

  -- Доля дней в «круге» (признак стабильности клиента)
  CASE 
    WHEN MEGA_CIRCLE_PRC IS NULL THEN '4. Прочее'
    WHEN MEGA_CIRCLE_PRC > 0.85 THEN '1. Более 85%'
    WHEN MEGA_CIRCLE_PRC > 0.15 THEN '2. 15...85%'             
    ELSE '3. Менее 15%' 
  END AS PROFILE_MEGA_CIRCLE_PRC,

  -- Цена устройства
  CASE 
    WHEN NVL(DEVICE_PRICE,0) IS NULL OR DEVICE_PRICE < 500 THEN '5. Прочее'
    WHEN NVL(DEVICE_PRICE,0) >= 80000 THEN '4. >80 тыс. руб.'
    WHEN NVL(DEVICE_PRICE,0) >= 50000 THEN '3. 50-80 тыс. руб.'
    WHEN NVL(DEVICE_PRICE,0) >= 20000 THEN '2. 20-50 тыс. руб.'
    ELSE '1. <20 тыс. руб.' 
  END AS PROFILE_DEVICE_PRICE,

  -- Тип устройства (смартфон, планшет и др.)
  DEVICE_TYPE_END AS PROFILE_DEVICE_TYPE,

  -- Lifetime — сколько дней с момента подключения
  CASE 
    WHEN NVL(LIFETIME,0) < 30 THEN '1. Менее 30 дней'
    WHEN NVL(LIFETIME,0) < 183 THEN '2. До 6 мес.'
    WHEN NVL(LIFETIME,0) < 366 THEN '3. От 6 мес. до года'
    ELSE '4. Более 1 года' 
  END AS PROFILE_LIFETIME,

  -- Название клиентского сегмента
  SEGMENT_NAME,

  -- Абонентская плата за тариф
  CASE 
    WHEN NVL(TP_FEE, 0) < 1 THEN '5. Прочее'
    WHEN NVL(TP_FEE, 0) <= 200 THEN '1. До 200 руб.'
    WHEN NVL(TP_FEE, 0) <= 500 THEN '2. 201-500 руб.'
    WHEN NVL(TP_FEE, 0) <= 1000 THEN '3. 501-1000 руб.'
    ELSE '4. Более 1000 руб.' 
  END AS PRODUCT_TP_FEE,

  -- Утилизация голосового пакета (распределение по долям)
  CASE 
    WHEN SPLIT_UTIL_VOICE IS NULL THEN 'Прочее'
    WHEN SPLIT_UTIL_VOICE IN ('a) 0-10%', 'b) 10-20%', 'c) 20-30%') THEN '1. 0-30%'
    WHEN SPLIT_UTIL_VOICE IN ('d) 30-40%', 'e) 40-50%', 'f) 50-60%',
                               'g) 60-70%', 'h) 70-80%', 'i) 80-90%',
                               'j) 90-100%') THEN '2. 30-100%'
    WHEN SPLIT_UTIL_VOICE IN ('t) 0% (нет тариф. трафика)', 'u) 0% (нет трафика)',
                               'w) нет пакета голоса') THEN '5. Нет трафика/пакета'
    WHEN SPLIT_UTIL_VOICE IN ('v) unlim') THEN '4. Безлимит'
    ELSE '3. >100%' 
  END AS PROFILE_UTIL_VOICE,

  -- Утилизация пакета данных
  CASE 
    WHEN SPLIT_UTIL_DATA IS NULL THEN 'Прочее'
    WHEN SPLIT_UTIL_DATA IN ('a) 0-10%', 'b) 10-20%', 'c) 20-30%') THEN '1. 0-30%'
    WHEN SPLIT_UTIL_DATA IN ('d) 30-40%', 'e) 40-50%', 'f) 50-60%',
                              'g) 60-70%', 'h) 70-80%', 'i) 80-90%',
                              'j) 90-100%') THEN '2. 30-100%'
    WHEN SPLIT_UTIL_DATA IN ('t) 0% (нет тариф. трафика)', 'u) 0% (нет трафика)',
                              'w) нет пакета голоса') THEN '5. Нет трафика/пакета'
    WHEN SPLIT_UTIL_DATA IN ('v) unlim') THEN '4. Безлимит'
    ELSE '3. >100%' 
  END AS PROFILE_UTIL_DATA,

  -- Название тарифной линейки
  NVL(tariff.NAME_LINE_TP, 'Прочее') AS PRODUCT_NAME_LINE_TP,

  -- Группировка тарифов
  NVL(tariff.group_tariff, 'Прочее') AS PRODUCT_GROUP_TARIFF,

  -- Доля 4G в общем времени подключения
  NETWORK_4G_SHARE,

  -- Простой: продолжительность неработающей связи
  CASE 
    WHEN NVL(PROSTOI_DUR, 0) >= 25 THEN '1. Более 25 часов'
    WHEN NVL(PROSTOI_DUR, 0) >= 5 THEN '2. 5-25 часов'
    WHEN NVL(PROSTOI_DUR, 0) >= 1 THEN '3. 1-5 часов'
    ELSE '4. Нет простоя' 
  END AS NETWORK_PROSTOI,

  -- VoLTE – наличие голосовой связи через LTE
  CASE 
    WHEN NVL(SHARE_VOLTE, 0) >= 80 THEN '1. Более 80%'
    WHEN NVL(SHARE_VOLTE, 0) >= 1 THEN '2. До 80%'
    ELSE '3. Не VoLTE' 
  END AS NETWORK_VOLTE,

  -- VoWiFi – наличие голосовой связи через WiFi
  CASE 
    WHEN NVL(SHARE_VOWIFI, 0) >= 0.01 THEN 'Есть'
    ELSE 'Нет' 
  END AS NETWORK_VOWIFI,

  -- Признак участия в репрайсе 2022
  CASE 
    WHEN NVL(reprice_2022, 0) > 0 THEN 'Да'
    ELSE 'Нет' 
  END AS PRODUCT_REPRICE_2022,

  -- Признак участия в первой волне репрайса 2023
  CASE 
    WHEN NVL(reprice_2023, 0) > 0 THEN 'Да'
    ELSE 'Нет' 
  END AS PRODUCT_REP1_2023,

  -- Признак оптаута 2023 (отказ от изменений)
  CASE 
    WHEN NVL(optout_2023, 0) > 0 THEN 'Да'
    ELSE 'Нет' 
  END AS PRODUCT_REPOP_2023,

  -- Признак участия в «упрощенном» репрайсе 2023
  CASE 
    WHEN NVL(simpl_2023, 0) > 0 THEN 'Да'
    ELSE 'Нет' 
  END AS PRODUCT_REPSIMP_2023,

  -- Кол-во дней перегрузки на сети
  CASE 
    WHEN NVL(OVERLOAD, 0) = 0 THEN '1. Нет'
    WHEN OVERLOAD >= 15 THEN '4. Более 15 дней'
    WHEN OVERLOAD >= 5 THEN '3. 5-15 дней'
    WHEN OVERLOAD >= 0 THEN '2. До 5 дней'
    ELSE 'Прочее' 
  END AS NETWORK_OVERLOAD,

  -- Операционная система устройства
  CASE 
    WHEN DEVICE_OS IN ('Android', 'iOS') THEN DEVICE_OS 
    ELSE 'Прочее' 
  END AS PROFILE_DEVICE_OS,

  -- SQI (качество голоса)
  CASE 
    WHEN SQI_VOICE IS NULL THEN '4. Прочее'
    WHEN SQI_VOICE > 99.5 THEN '1. >99.5%'
    WHEN SQI_VOICE > 98 THEN '2. 98..99.5%'
    ELSE '3. <98%' 
  END AS NETWORK_SQI_VOICE,

  -- Скорость LTE (интервалы в Mbps)
  CASE 
    WHEN SPEED_LTE IS NULL THEN '5. Прочее' 
    WHEN SPEED_LTE > 10 THEN '3. >10 Mbps'
    WHEN SPEED_LTE > 2.5 THEN '2. 2.5...10 Mbps'
    ELSE '1. <2.5 Mbps' 
  END AS NETWORK_SPEED_LTE,

  -- Признак бонусных опций (например, 3 месяца бесплатно)
  CASE 
    WHEN BNS IS NULL THEN 'Прочее'
    WHEN BNS = 1 THEN 'Да'
    ELSE 'Нет' 
  END AS PRODUCT_BNS,

  -- Доля времени в 2G (при наличии 4G и смартфона)
  CASE 
    WHEN DURATION_2G_PERCENT IS NULL 
         OR NVL(PROFILE_BEARER_4G, 0) = 0 
         OR NVL(DEVICE_TYPE_END, '') <> 'SMARTPHONE' THEN 'Прочее'
    WHEN DURATION_2G_PERCENT > 20 THEN '3. Более 20%'
    WHEN DURATION_2G_PERCENT > 2.5 THEN '2. 2.5...20%'  
    ELSE '1. Менее 2.5%' 
  END AS NETWORK_DURATION_2G,

  -- Доля времени в 4G
  CASE 
    WHEN DURATION_4G_PERCENT IS NULL OR DURATION_4G_PERCENT < 1 THEN 'Прочее'
    WHEN DURATION_4G_PERCENT > 80 THEN 'Более 80%'  
    ELSE 'Менее 80%' 
  END AS NETWORK_DURATION_4G,

  -- Доля трафика со скоростью ниже 2 Мбит/с
  CASE 
    WHEN SPD_LOWER_THAN_2000_KBPS_RATE IS NULL THEN 'Прочее'
    WHEN SPD_LOWER_THAN_2000_KBPS_RATE > 80 THEN '3. Более 80%'  
    WHEN SPD_LOWER_THAN_2000_KBPS_RATE > 20 THEN '2. 20...80%'    
    ELSE '1. Менее 20%' 
  END AS NETWORK_SPD_LOWER_THAN_2000,

  -- Признак подключения переносом номера
  CASE 
    WHEN NVL(PRODUCT_PERENOS, 0) = 1 THEN 'Да' 
    ELSE 'Нет' 
  END AS PRODUCT_PERENOS,

  -- Признак подключения услуги ЕВА
  CASE 
    WHEN NVL(PRODUCT_EVA, 0) = 1 THEN 'Да' 
    ELSE 'Нет' 
  END AS PRODUCT_EVA,

  -- Семейные продукты
  CASE 
    WHEN NVL(PRODUCT_FAMILY_MANAGER, 0) = 1 THEN 'Управление номерами'
    WHEN NVL(PRODUCT_FAMILY, 0) = 1 THEN 'МегаСемья' 
    ELSE 'Нет' 
  END AS PRODUCT_FAMILY,

  -- Пред5G
  CASE 
    WHEN NVL(PRODUCT_PRE5G, 0) = 1 THEN 'Да' 
    ELSE 'Нет' 
  END AS PRODUCT_PRE5G,

  -- МегаСильный бесплатно
  CASE 
    WHEN NVL(PRODUCT_MEGASIL_BESPL, 0) = 1 THEN 'Да' 
    ELSE 'Нет' 
  END AS PRODUCT_MEGASIL_BESPL,

  -- Улучшение тарифа
  CASE 
    WHEN NVL(PRODUCT_ULUCH, 0) = 1 THEN 'Да' 
    ELSE 'Нет' 
  END AS PRODUCT_ULUCH,

  -- Признаки ЦД-кампаний (перенос, ЕВА и т.п.)
  CASE WHEN NVL(PRODUCT_CD_PERENOS, 0) = 1 THEN 'Да' ELSE 'Нет' END AS PRODUCT_CD_PERENOS,
  CASE WHEN NVL(PRODUCT_CD_EVA, 0) = 1 THEN 'Да' ELSE 'Нет' END AS PRODUCT_CD_EVA,

  -- ЦД: семейные продукты
  CASE 
    WHEN NVL(PRODUCT_CD_FAMILY_MANAGER, 0) = 1 THEN 'Управление номерами' 
    WHEN NVL(PRODUCT_CD_FAMILY, 0) = 1 THEN 'МегаСемья' 
    ELSE 'Нет' 
  END AS PRODUCT_CD_FAMILY,

  CASE WHEN NVL(PRODUCT_CD_PRE5G, 0) = 1 THEN 'Да' ELSE 'Нет' END AS PRODUCT_CD_PRE5G,
  CASE WHEN NVL(PRODUCT_CD_MEGASIL_BESPL, 0) = 1 THEN 'Да' ELSE 'Нет' END AS PRODUCT_CD_MEGASIL_BESPL,
  CASE WHEN NVL(PRODUCT_CD_ULUCH, 0) = 1 THEN 'Да' ELSE 'Нет' END AS PRODUCT_CD_ULUCH,

  -- Признак переплат (общая сумма по видам > 40 руб)
  CASE 
    WHEN NVL(PRODUCT_PEREPL_MN,0)+NVL(PRODUCT_PEREPL_GOR,0)+  
         NVL(PRODUCT_PEREPL_OPER,0)+NVL(PRODUCT_PEREPL_SMS,0)+
         NVL(PRODUCT_UDERZH,0) > 40 THEN '1. Более 40 руб'
    WHEN NVL(PRODUCT_PEREPL_MN,0)+NVL(PRODUCT_PEREPL_GOR,0)+  
         NVL(PRODUCT_PEREPL_OPER,0)+NVL(PRODUCT_PEREPL_SMS,0)+
         NVL(PRODUCT_UDERZH,0) > 0 THEN '2. До 40 руб'     
    ELSE '3. Нет переплат' 
  END AS PRODUCT_PEREPLATY,

  -- Использование Кинопоиска
  CASE 
    WHEN NVL(PROFILE_KINOPOISK,0) > 1000000000 THEN '1. Более 1GB'
    WHEN NVL(PROFILE_KINOPOISK,0) > 100000000  THEN '2. 100MB - 1GB'
    WHEN NVL(PROFILE_KINOPOISK,0) > 100        THEN '3. До 100MB'
    ELSE '4. Нет' 
  END AS PROFILE_KINOPOISK,

  -- Telegram
  CASE 
    WHEN NVL(PROFILE_TELEGRAM,0) > 1000000000 THEN '1. Более 1GB'
    WHEN NVL(PROFILE_TELEGRAM,0) > 100000000  THEN '2. 100MB - 1GB'
    WHEN NVL(PROFILE_TELEGRAM,0) > 100        THEN '3. До 100MB'
    ELSE '4. Нет' 
  END AS PROFILE_TELEGRAM,

  -- ВКонтакте (основной трафик)
  CASE 
    WHEN NVL(PROFILE_VKONTAKTE,0) > 1000000000 THEN '1. Более 1GB'
    WHEN NVL(PROFILE_VKONTAKTE,0) > 100000000  THEN '2. 100MB - 1GB'
    WHEN NVL(PROFILE_VKONTAKTE,0) > 100        THEN '3. До 100MB'
    ELSE '4. Нет' 
  END AS PROFILE_VKONTAKTE,

  -- ВКонтакте Видео
  CASE 
    WHEN NVL(PROFILE_VKONTAKTE_VIDEO,0) > 1000000000 THEN '1. Более 1GB'
    WHEN NVL(PROFILE_VKONTAKTE_VIDEO,0) > 100000000  THEN '2. 100MB - 1GB'
    WHEN NVL(PROFILE_VKONTAKTE_VIDEO,0) > 100        THEN '3. До 100MB'
    ELSE '4. Нет' 
  END AS PROFILE_VKONTAKTE_VIDEO,

  -- WhatsApp (текст + media)
  CASE 
    WHEN NVL(PROFILE_WHATSAPP,0) + NVL(PROFILE_WHATSAPP_MEDIA,0) > 1000000000 THEN '1. Более 1GB'
    WHEN NVL(PROFILE_WHATSAPP,0) + NVL(PROFILE_WHATSAPP_MEDIA,0) > 100000000  THEN '2. 100MB - 1GB'
    WHEN NVL(PROFILE_WHATSAPP,0) + NVL(PROFILE_WHATSAPP_MEDIA,0) > 100        THEN '3. До 100MB'
    ELSE '4. Нет' 
  END AS PROFILE_WHATSAPP,

  -- WhatsApp аудио-звонки
  CASE 
    WHEN NVL(PROFILE_WHTSAPP_CALL,0) > 1000000000 THEN '1. Более 1GB'
    WHEN NVL(PROFILE_WHTSAPP_CALL,0) > 100000000  THEN '2. 100MB - 1GB'
    WHEN NVL(PROFILE_WHTSAPP_CALL,0) > 100        THEN '3. До 100MB'
    ELSE '4. Нет' 
  END AS PROFILE_WHTSAPP_CALL,

  -- WhatsApp видеозвонки
  CASE 
    WHEN NVL(PROFILE_WHATSAPP_VIDEOCALL,0) > 1000000000 THEN '1. Более 1GB'
    WHEN NVL(PROFILE_WHATSAPP_VIDEOCALL,0) > 100000000  THEN '2. 100MB - 1GB'
    WHEN NVL(PROFILE_WHATSAPP_VIDEOCALL,0) > 100        THEN '3. До 100MB'
    ELSE '4. Нет' 
  END AS PROFILE_WHATSAPP_VIDEOCALL,

  -- YouTube
  CASE 
    WHEN NVL(PROFILE_YOUTUBE,0) > 1000000000 THEN '1. Более 1GB'
    WHEN NVL(PROFILE_YOUTUBE,0) > 100000000  THEN '2. 100MB - 1GB'
    WHEN NVL(PROFILE_YOUTUBE,0) > 100        THEN '3. До 100MB'
    ELSE '4. Нет' 
  END AS PROFILE_YOUTUBE,

  -- VPN
  CASE 
    WHEN NVL(PROFILE_VPN,0) > 1000000000 THEN '1. Более 1GB'
    WHEN NVL(PROFILE_VPN,0) > 100000000  THEN '2. 100MB - 1GB'
    WHEN NVL(PROFILE_VPN,0) > 100        THEN '3. До 100MB'
    ELSE '4. Нет' 
  END AS PROFILE_VPN,

  -- Региональные признаки
  REGION_ABC_TOTAL,
  REGION_SQI_TREND,
  REGION_COMPETITOR,
  REGION_ABC_COVERAGE,

  -- Название тарифной линейки на текущий момент
  NVL(cur_tariff.NAME_LINE_TP, 'Прочее') AS PRODUCT_CD_LINE_TP,

  -- Группировка тарифа (например, флаг архивности)
  NVL(cur_tariff.GROUP_TARIFF, 'Прочее') AS PRODUCT_CD_GROUP_TARIFF,

  -- Признак участия в персонализированных кампаниях
  CASE 
    WHEN PRODUCT_PERS IS NULL THEN 'Нет миграции'
    WHEN PRODUCT_PERS = 'Без информирования' THEN 'Без инф.'
    ELSE 'Склейка и пр.' 
  END AS PRODUCT_PERS,

  REGION_REFARMING,
  REGION_IMPROVEMENT,
  REGION_SVO,
  REGION_FILIAL,

  -- Сетевая модель на текущий день
  CASE 
    WHEN NVL(NETWORK_SERV_MODEL,0) = 2 THEN 'PRIOR'
    WHEN NVL(NETWORK_SERV_MODEL,0) = 3 THEN 'NORMAL'
    WHEN NVL(NETWORK_SERV_MODEL,0) = 4 THEN 'BASE'
    ELSE 'Прочее' 
  END AS NETWORK_SERV_MODEL,

  -- Целевая сетевая модель по плану
  CASE 
    WHEN NVL(NETWORK_CD_SERV_MODEL,0) = 2 THEN 'PRIOR'
    WHEN NVL(NETWORK_CD_SERV_MODEL,0) = 3 THEN 'NORMAL'
    WHEN NVL(NETWORK_CD_SERV_MODEL,0) = 4 THEN 'BASE'
    ELSE 'Прочее' 
  END AS NETWORK_CD_SERV_MODEL,

  -- Признак участия в дебиторской кампании
  CASE 
    WHEN DEBITOR IS NULL THEN 'нет' 
    ELSE 'Да' 
  END AS PRODUCT_CD_DEBITOR,

  -- Признак реактивации
  CASE 
    WHEN IN_REACT IS NULL THEN 'нет' 
    ELSE 'Да' 
  END AS PRODUCT_CD_REACTIV,

  -- Принадлежность к риск-базе
  CASE 
    WHEN RISK_BASE IS NULL THEN 'нет' 
    ELSE 'Риск база' 
  END AS PRODUCT_CD_RISK_BASE,

  -- Дебитор (основная версия)
  NVL(PRODUCT_DEBITOR, 'Прочее') AS PRODUCT_DEBITOR,

  -- SQI Data
  CASE 
    WHEN SQI_DATA IS NULL THEN '4. Прочее'
    WHEN SQI_DATA > 99.5 THEN '1. >99.5%'
    WHEN SQI_DATA > 98 THEN '2. 98...99.5%'
    ELSE '3. <98%' 
  END AS NETWORK_SQI_DATA,

  -- Признак помех на радиосегменте
  CASE 
    WHEN NVL(PROBLEMCELL5,0) >= 1 THEN '1. Топ 5 с помехами'
    WHEN NVL(PROBLEMCELL,0) >= 10 THEN '2. Помехи более 10 площадок'
    WHEN NVL(PROBLEMCELL,0) >= 1 THEN '3. Прочие помехи'  
    ELSE '4. Нет' 
  END AS NETWORK_RADIOBLOCK,

  -- Комплексный фактор — продуктовый
  CASE 
    WHEN NVL(PRODUCT_DEBITOR, 'Прочее') <> 'Прочее'
      OR PRODUCT_PERS IS NOT NULL
      OR NVL(REPRICE_2023,0) > 0
      OR NVL(OPT_OUT_2023,0) > 0
      OR NVL(SIMPL_2023,0) > 0
    THEN 'Негативный фактор'
    ELSE 'Прочее' 
  END AS PROFILE_COMPLEX_FACTOR_PRODUCT,

  -- Пол оператора (если опрос касался другого номера)
  NVL(mr_nps_operator.gender, 'Прочее') AS SURVEY_OPERATOR_GENDER,

  -- Возраст оператора
  NVL(mr_nps_operator.age, 'Прочее') AS SURVEY_OPERATOR_AGE,

  -- Признак участия в репрайсе январь 2024
  NVL(rep1_2024, 'Нет') AS PRODUCT_CD_REP1_2024,

  -- Мультисим-сегментация
  CASE 
    WHEN NVL(f2_class, '') = 'single_sim' THEN '1. Одна SIM'
    WHEN NVL(f2_class, '') = 'ms_main_not_mf' THEN '5. Мульти, МФ-дополн.'
    WHEN NVL(f2_class, '') = 'inner_ms_not_main' THEN '3. Внутр. дополн.'
    WHEN NVL(f2_class, '') = 'inner_ms_main' THEN '2. Внутр. основная'
    WHEN NVL(f2_class, '') = 'ms_main_mf' THEN '4. Мульти, МФ-основной'
    ELSE '6. Прочее' 
  END AS PROFILE_CD_MULTI_SIM,

  -- Признак участия в репрайсе 2кв. 2024 (июнь и др.)
  NVL(rep6_2024, 'Нет') AS PRODUCT_CD_REP6_2024,

  -- Сезонка: сегмент региона по трафику (город / область)
  CASE 
    WHEN tariff.snap_week < TO_DATE('15.01.2024', 'DD.MM.YYYY') THEN NULL
    WHEN seas.segment = 'Digital' THEN '1. Преобладает город'
    WHEN seas.segment = 'Oblast' THEN '2. Преобладает область'
    WHEN seas.segment = 'Equals' THEN '3. Город/Область'
    ELSE '4. Нет трафика' 
  END AS NETWORK_SEASON_TRAF,

  -- Тип опции EVA (модификации: +, MLK, VoLTE)
  CASE 
    WHEN NVL(PRODUCT_CD_EVA_PLUS,0) = 1 THEN 'Ева+'
    WHEN NVL(PRODUCT_CD_EVA_MLK,0) = 1 THEN 'Ева МЛК'
    WHEN NVL(PRODUCT_CD_EVA_VOLTE,0) = 1 THEN 'Ева Volte'
    ELSE 'Нет' 
  END AS PRODUCT_CD_EVA_TYPE,

  -- То же самое в другом представлении
  CASE 
    WHEN NVL(PRODUCT_EVA_PLUS,0) = 1 THEN 'Ева+'
    WHEN NVL(PRODUCT_EVA_MLK,0) = 1 THEN 'Ева МЛК'
    WHEN NVL(PRODUCT_EVA_VOLTE,0) = 1 THEN 'Ева Volte'
    ELSE 'Нет' 
  END AS PRODUCT_EVA_TYPE,

  -- Признак участия в сентябрьском репрайсе (или BI, отмена скидки)
  NVL(rep9_2024, 'Нет') AS PRODUCT_CD_REP9_2024,

  -- Признак регионального округа RO (например: СЗФО, ПФО и т.д.)
  reg.region_ro AS REGION_RO,

  -- Признак участия в январском репрайсе 2025
  NVL(rep01_2025, 'Нет') AS PRODUCT_CD_REP01_2025,

  -- Тип SIM (eSIM, USIM и др.)
  NVL(SIM_GROUP, 'Прочее') AS PRODUCT_SIM_TYPE,

  -- Фиксированный профиль потребления YouTube в апреле-мае 2024
  NVL(CD_PROFILE_YOUTUBE, '4. Нет') AS PROFILE_CD_YOUTUBE,

  -- То же для Telegram
  NVL(CD_PROFILE_TELEGRAM, '4. Нет') AS PROFILE_CD_TELEGRAM,

  -- Признак участия в MNP (входящий порт из другого оператора)
  CASE 
    WHEN STLS_NUM_DATE IS NULL THEN '2. Нет' 
    ELSE '1. MNP_IN' 
  END AS PRODUCT_MNP_IN
FROM 
  --РЕЗУЛЬТАТЫ ОПРОСА
  (SELECT * FROM mr_nps_survey_results_tariff tariff) tariff
  --УСЛУГИ
  LEFT OUTER JOIN mr_nps_survey_results_service_view service
  ON tariff.sk_subs_id = service.sk_subs_id
     AND tariff.snap_date = service.snap_date
  --VAS
  LEFT OUTER JOIN mr_nps_survey_results_vas vas
  ON tariff.sk_subs_id = vas.sk_subs_id
     AND tariff.snap_date = vas.snap_date
  --OTT
  LEFT OUTER JOIN fin_ba.mr_nps_survey_results_ott_view ott
  ON tariff.sk_subs_id = ott.sk_subs_id
     AND tariff.snap_date = ott.snap_date
  --РЕГИОН
  LEFT OUTER JOIN mr_nps_region reg
  ON tariff.os_os_id = reg.os_os_id
  --ТАРИФ НА ТЕКУЩУЮ ДАТУ
  LEFT OUTER JOIN
  (
    SELECT /*+ parallel (10)*/ 
            sk_subs_id, name_r, name_line_tp, group_tariff
    FROM PUB_DS.F_SUBS_CHURN_TRG_WEEKLY churn
    LEFT OUTER JOIN fin_ba.mr_tariff tariff_report
      ON churn.rtpl_rtpl_id = tariff_report.rtpl_id
    WHERE start_date = trunc(sysdate, 'iw')-7 
          AND billing_filial_id=10
  ) cur_tariff
  ON tariff.sk_subs_id = cur_tariff.sk_subs_id
  --СЕРВИСНАЯ МОДЕЛЬ НА СЕГОДНЯ
  LEFT OUTER JOIN
  (
    SELECT --+parallel(10)
           sk_subs_id, priority_priority_id AS network_cd_serv_model
    FROM pub_ds.s_subs_priority_daily
    WHERE billing_filial_id = 10 
          AND snap_date=trunc(sysdate,'dd')-7
  ) sm
  ON tariff.sk_subs_id = sm.sk_subs_id
  --РАБОТА С ДЕБИТОРКОЙ
  LEFT OUTER JOIN
  (
    SELECT --+parallel(10)
           DISTINCT cc.sk_subs_id, 1 AS debitor
    FROM PUB_STG.D_CIM_COMPAIGN_CUSTOMERS CC  
    WHERE RUN_DTTM between date '2023-04-01' and date '2025-01-01'     ---------- даты запусков кампаний
    and cc.communication_id in (select distinct COMMUNICATION_ID from MAXIM_MAKARENKOV.CIM_COMPAIGN)
    and cc.disposition_type=1 and cc.response_type>0
  ) deb
  ON tariff.sk_subs_id = deb.sk_subs_id
  --РЕАКТИВАЦИИ
  LEFT OUTER JOIN
  (
     SELECT /*+ parallel(10)*/ 
            DISTINCT subs_subs_id, 'Реактивация' AS in_react 
     FROM pub_ds.bis_subs_packs y
     where 1=1 and (y.navi_user like '%IVAN_CHISTOV_NOSMS%' or y.navi_user like '%KAMIL_IBRAGIMOV%')
             and y.start_date >= '01.08.2022'
  ) react
  ON tariff.subs_subs_id = react.subs_subs_id
  --РИСКОВАЯ БАЗА
  LEFT OUTER JOIN
  (
     SELECT  --+parallel(10) 
             DISTINCT sk_subs_id, 'Риск база' AS risk_base
     FROM AME_RISK_COMPETITOR_SUBS_v2
     WHERE flag_risk_big4=1 and flag_base_for_risk=1 
              AND report_month = TRUNC(SYSDATE-20, 'mm')
  ) rb
  ON tariff.sk_subs_id = rb.sk_subs_id
  --РЕПРАЙС 2023
  LEFT OUTER JOIN 
  (
    SELECT  --+parallel(10) 
           DISTINCT sk_subs_id, 1 AS reprice_2023
    FROM 
    ((SELECT sk_subs_id FROM alexey_v_trukhachev.REPRICE_1Q23_DB_TP_PROD_final_best_s3)
    UNION
    (SELECT sk_subs_id FROM alexey_v_trukhachev.REPRICE_1Q23_DB_OPC_PROD_final)
    UNION
    (SELECT sk_subs_id FROM alexey_v_trukhachev.REPRICE_1Q23_Bundly_Final))
  ) rep_2023
  ON tariff.sk_subs_id = rep_2023.sk_subs_id
  --СИМПЛИФИКАЦИЯ 2023
  LEFT OUTER JOIN 
  (
     SELECT --+parallel(10) 
            DISTINCT sk_subs_id, 1 AS simpl_2023
     FROM alexey_v_trukhachev.REPRICE_1Q23_Simply_fin_AllAbon
  ) simpl_2023
  ON tariff.sk_subs_id = simpl_2023.sk_subs_id
  --ОПТАУТ
  LEFT OUTER JOIN
  (
    SELECT  --+parallel(10) 
            DISTINCT sk_subs_id, 1 AS optout_2023
    FROM 
    ((SELECT sk_subs_id FROM alexey_v_trukhachev.TAV_REPRICE_1Q23_OPTOUT )
    UNION
    (SELECT sk_subs_id FROM alexey_v_trukhachev.TAV_REPRICE_2Q23_OPTOUT ))
  ) oo_2023
  ON tariff.sk_subs_id = oo_2023.sk_subs_id
  --РЕПРАЙС 2022
  LEFT OUTER JOIN 
  (
      SELECT --+parallel(10) 
             DISTINCT sk_subs_id, 1 AS reprice_2022
      FROM 
      ((SELECT sk_subs_id FROM KEA_REPRICE_2022 )
      UNION
      (SELECT sk_subs_id FROM KEA_REPRICE_2022_2 )
      UNION
      (SELECT sk_subs_id FROM KEA_REPRICE_2022_3 )
      UNION
      (SELECT sk_subs_id FROM KEA_REPRICE_2022_SNG )
      UNION
      (SELECT sk_subs_id FROM KEA_REPRICE_2022_2_SNG )
      UNION
      (SELECT sk_subs_id FROM KEA_REPRICE_2022_B2B ))
  ) rep_2022
  ON tariff.sk_subs_id = rep_2022.sk_subs_id
  --ПЕРСОНАЛЬНЫЙ
  LEFT OUTER JOIN 
  (
        SELECT --+parallel(10) 
              sk_subs_id, MAX(case_) AS PRODUCT_PERS
        FROM
        (
              select  --+parallel(10)
                      distinct vv.subs_subs_id, x.sk_subs_id, CASE_, dispatch_date, date_change 
              from (
                   select t1.subs_subs_id, t1.CASE_, t1.dispatch_date, t1.date_change from rep_core_b2c.Reprice_3Q23_freeze t1      
                   union select t2.subs_subs_id,t2.CASE_, t2.dispatch_date, t2.date_change from rep_core_b2c.Reprice_3Q23_freeze_no_inf t2      
                   union select subs_subs_id,case when COMMENT_S = '800_no_inf' or COMMENT_S = '20_no_inf' then 'Без информирования' when COMMENT_S = '316_ottok' then 'Отток' when COMMENT_S = '316_no_gold_fikt' then 'Фиктивная скидка' when COMMENT_S = '316_no_no' then 'Исключение' end CASE_, t.dispatch_date, t.date_change_new as date_change from alexey_ponomarev.PAV_REPRICE_3Q23_1_VOLNA_FOR_IT t
              ) vv
              left join alexey_v_Trukhachev.TAV_REPRICE_3Q23_SMALL_TRANSFER_Xv3 x on vv.subs_subs_id = x.subs_subs_id  
         )
         GROUP BY sk_subs_id 
  ) pers
  ON tariff.sk_subs_id = pers.sk_subs_id 
  
  
  
  --ПОМЕХИ НА РАДИО
  LEFT OUTER JOIN 
  (
       SELECT --+parallel(10)
          start_date, sk_subs_id, 
          MAX(has_traf_in_problemcell) problemcell, 
          MAX(has_traf_in_problemcell_top5) problemcell5
       FROM fin_ba.mr_nps_survey_radioblock
       GROUP BY start_date, sk_subs_id) radioblock
       ON tariff.sk_subs_id = radioblock.sk_subs_id AND tariff.snap_month = radioblock.start_date
  --РЕПРАЙС 2024 ЯНВАРЬ
       LEFT OUTER JOIN
       (
            SELECT --+parallel(10)
                   NVL(rep_fact.sk_subs_id, rep_plan.sk_subs_id) AS sk_subs_id,
                   CASE WHEN NVL(date_change_itog, SYSDATE+1)<SYSDATE THEN 'Переведен'
                        WHEN NVL(dispatch_date, SYSDATE+1)<SYSDATE THEN 'Проинформирован'
                        ELSE 'Нет' 
                   END AS rep1_2024
        
           FROM 
           (
               SELECT sk_subs_id, MIN(dispatch_date) AS dispatch_date,
                      MIN(date_change_itog) AS date_change_itog
               FROM rep_b2b.pav_reprice_1q2024_freeze
               GROUP BY sk_subs_id
           ) rep_fact
           FULL OUTER JOIN
           (
               SELECT --+parallel(10)
                      distinct sk_subs_id
               FROM alexey_ponomarev.PAV_REPRICE_1Q24_FOR_CASE
               WHERE CASE_LIFT in ('base_mig_2023','Opt_out_and_BP_2X','Pers_3_0')
               group by sk_subs_id
           ) rep_plan
           ON rep_fact.sk_subs_id = rep_plan.sk_subs_id
           ) reprice1_2024
           ON tariff.sk_subs_id = reprice1_2024.sk_subs_id
        --РЕПРАЙС 2024 ИЮНЬ + отмена скидки + репрайс БИ
       LEFT OUTER JOIN
       (
            SELECT /*+ parallel(10)*/
                   t.sk_subs_id,
                   CASE
                   WHEN s.msisdn IS NOT NULL THEN 'Проинформирован'
                   WHEN act_subs_tp.rtpl_rtpl_id = 502865 THEN 'Переведен' ELSE 'План' END rep6_2024
            FROM rep_core_b2c.pav_reprice_2q2024_bundle_freeze_history_copy t -- общий массив всех фризов
            LEFT JOIN alexey_linyov.msisdn_reprice_cvm_rezerv s -- таблица всех СМС, кого информировали 
            ON t.msisdn = CASE
            WHEN s.dispatch_date = '27.06.2024' THEN substr(s.msisdn, -10) ELSE s.msisdn END
            LEFT JOIN (
                 SELECT sk_subs_id, rtpl_rtpl_id 
                 FROM pub_ds.s_subs_activities
                 WHERE snap_date = trunc(SYSDATE - 1, 'dd')
                 AND billing_filial_id = 10
            ) act_subs_tp -- проверка наличия активного перс тарифа
            ON t.sk_subs_id = act_subs_tp.sk_subs_id
  WHERE
  VERS IN (0.1, 1.2, 2.1, 3.1, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.8, 4.9, 5.1, 6.1, 6.2, 6.3, 
           7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6)) rep6_2024
  on tariff.sk_subs_id = rep6_2024.sk_subs_id
  LEFT OUTER JOIN(
  --РЕПРАЙС 2024 СЕНТЯБРЬ
  --РЕПРАЙС БИ 2024 (с архивными тарифами, с бесплатной опцией БИ)
  SELECT --+parallel(10)
         distinct sk_subs_id, 
         CASE WHEN NVL(REPRICE_DATE_BI_V2, SYSDATE+1)<SYSDATE THEN 'Переведен'
              WHEN NVL(DISPATCH_DATE_BI_V3, SYSDATE+1)<SYSDATE THEN 'Проинформирован' ELSE 'План' END AS rep9_2024
  FROM rep_core_b2c.mta_reprice_unlim_data_id_503694_4q2024_freeze
  union
  --РЕПРАЙС ОТМЕНА СКИДКИ 2024 (абоны с бессрочной скидкой - либо полностью отменяем, либо уменьшаем)
  SELECT --+parallel(10)
         distinct sk_subs_id, 
         CASE WHEN NVL(REPRICE_DATE_skidka_V2, SYSDATE+1)<SYSDATE THEN 'Переведен'
              WHEN NVL(DISPATCH_DATE_SKIDKA_V3, SYSDATE+1)<SYSDATE THEN 'Проинформирован' ELSE 'План' END AS rep9_2024
  FROM rep_core_b2c.mta_reprice_skidka_4q2024_freeze
  -- updated by a_gavr 07/10/2024 end
  ) rep9_2024
  on tariff.sk_subs_id = rep9_2024.sk_subs_id
  --СЕГМЕНТ МУЛЬТИСИМ 
  LEFT OUTER JOIN
  (SELECT --+parallel(10)
          sk_subs_id, 
         MAX(f2_class) AS f2_class
  FROM BD_SCIENTIST_USER.OLD_BASE_MULTISIM_PREPREDICT_V3
  WHERE score_date = (SELECT /*+ parallel (8)*/ 
                      MAX(score_date)
                      FROM BD_SCIENTIST_USER.OLD_BASE_MULTISIM_PREPREDICT_V3
                      WHERE score_date>='26.02.2024')
  GROUP BY sk_subs_id) multi_sim
  ON tariff.sk_subs_id = multi_sim.sk_subs_id
  --ОПЕРАТОР 
  LEFT OUTER JOIN 
  mr_nps_operator 
  ON mr_nps_operator.op_ID = tariff.SURVEY_OPERATOR_ID
  -- updated by a_gavr 20/08/2024
  -- ФИКСИРОВАННАЯ КОГОРТА ЮТУБА И ТГ НА СРЕДНЕЕ ПОТРЕБЛЕНИЕ АПРЕЛЯ-МАЯ
  LEFT JOIN 
  (
       SELECT --+parallel(10)
         subs_subs_id, 
         CASE WHEN NVL(PROFILE_YOUTUBE,0)>1000000000 THEN '1. Более 1GB'
              WHEN NVL(PROFILE_YOUTUBE,0)>100000000 THEN '2. 100MB - 1GB'
              WHEN NVL(PROFILE_YOUTUBE,0)>100 THEN '3. До 100MB'
              ELSE '4. Нет' 
          END AS cd_PROFILE_YOUTUBE
       FROM(
             SELECT /*+parallel(10)*/ subs_subs_id, (SUM(dl_volume)+SUM(ul_volume))/2 AS PROFILE_YOUTUBE
             FROM pub_ds.a_subs_app_data
             WHERE month_stamp IN ('01.04.2024', '01.05.2024') AND service_id LIKE '%youtube%'
             GROUP BY subs_subs_id
           )
  ) cd_y on cd_y.subs_subs_id = tariff.SUBS_SUBS_ID
  LEFT JOIN 
  (
       SELECT --+parallel(10)
       subs_subs_id, 
         CASE WHEN NVL(PROFILE_TELEGRAM,0)>1000000000 THEN '1. Более 1GB'
              WHEN NVL(PROFILE_TELEGRAM,0)>100000000 THEN '2. 100MB - 1GB'
              WHEN NVL(PROFILE_TELEGRAM,0)>100 THEN '3. До 100MB'
              ELSE '4. Нет' 
          END AS cd_PROFILE_TELEGRAM
       FROM(
             SELECT /*+parallel(10)*/ subs_subs_id, (SUM(dl_volume)+SUM(ul_volume))/2 AS PROFILE_TELEGRAM
             FROM pub_ds.a_subs_app_data
             WHERE month_stamp IN ('01.04.2024', '01.05.2024') AND service_id LIKE '%telegram%'
             GROUP BY subs_subs_id
           )  
  ) cd_t on cd_t.subs_subs_id = tariff.SUBS_SUBS_ID
  --СЕЗОНКА
  LEFT JOIN 
  (select distinct * from fin_ba.mr_nps_segments_season_traf_seg) seas
  ON tariff.sk_subs_id = seas.sk_subs_id
  AND tariff.snap_week = seas.snap_week
  -- updated by a_gavr 20/08/2024
  --РЕПРАЙС ЯНВАРЯ 2025
  LEFT JOIN 
  ( SELECT --+parallel(10)
           subs_subs_id, 
           CASE WHEN NVL(date_change_itog, SYSDATE+1)<SYSDATE THEN 'Переведен'
                WHEN NVL(date_inform, SYSDATE+1)<SYSDATE THEN 'Проинформирован' ELSE 'План' END AS rep01_2025
        FROM (
             --Bundle размеченый
              (SELECT subs_subs_id, trunc(migr_date_new, 'dd') AS date_change_itog, date_inform, 'Bundle' rep_type
               FROM rep_core_b2c.pav_reprice_1q2025_freeze_bundle_and_pd_24_12_24 b
               WHERE vers IN ('0,2', '0,5', '1,2', '1,3', '3,2', '4,2', '5,4', '5,5', '5,6', '6,5', '6,7', '6,8')) UNION
             --Перс 3.0 размеченый
              (SELECT subs_subs_id, trunc(date_change_itog, 'dd') AS date_change_itog, dispatch_date date_inform, 'Pers 3.0' rep_type
               FROM rep_core_b2c.lai_reprice_1q25_pers_freeze p
               WHERE vers IN (0.1, 0.111, 0.2, 0.3, 0.4, 0.5, 1.1, 1.2, 1.3, 1.4, 1.44, 1.5, 1.55, 1.6, 1.66, 1.6, 1.7, 1.9)
                     AND round(p.vers, 0) != p.vers -- Базовое правило 
                     AND p.vers not in (0.222, 1.8) -- Иногда будут версии-исключения. Определить можно по полю comm 
                     AND p.vers < 100) UNION
             --ДД
              SELECT subs_subs_id, DATE '2025-01-14' AS date_change_itog, DATE '2024-12-23' date_inform, 'ДД' rep_type
              FROM
              rep_core_b2c.reprice_1q25_classic_dd_freeaze d
              WHERE(ap_to_be_with_discount - ap_as_is_with_discount) > 1 AND ap_to_be_with_discount > 1 AND
              ap_as_is_with_discount <> 0 AND svo = 0 UNION
             --СНГ + FMC
              SELECT subs_subs_id, date_change_itog, dispatch_date, 'СНГ + FMC' rep_type
              FROM rep_core_b2c.lai_reprice_1q25_fmc_warm_freeze fre2
              WHERE round(fre2.vers, 0) != fre2.vers -- Базовое правило 
              and fre2.vers not in (0.222) -- Иногда будут версии-исключения. Определить можно по полю comm 
              and fre2.vers < 100
       )
       WHERE date_change_itog < SYSDATE
  ) rep01_2025 
  on tariff.subs_subs_id = rep01_2025.subs_subs_id
  --КОРРЕКТНЫЙ РАСЧЕТ КОЛ-ВА ОТПРАВЛЕННЫХ СМС ОТ МФ
  
  LEFT JOIN (
       SELECT --+parallel(10)
              nps.sk_subs_id, nps.snap_date, 
              case when nps.measure_id in (-643,-644) then sum(s.all_mega_sms_cnt) else COUNT(DISTINCT notification_id) end all_mega_sms_cnt
              FROM fin_ba.mr_nps_survey_results_tariff nps
              LEFT JOIN pub_ds.h_cnc_notifications sms -- все нотифы из CNС (без васов,95%)
                        ON nps.msisdn = sms.address_to AND sms.order_send_date BETWEEN nps.snap_date - 30 AND nps.snap_date
              LEFT JOIN (select distinct * from fin_ba.mr_nps_survey_results_b2x_sms) s
                        ON nps.sk_subs_id = s.sk_subs_id AND nps.snap_date = s.snap_date
              WHERE 1 = 1
              AND smgs_smgs_id IN (2, 3, 8) -- факт отправки (до конца ноября должен быть факт доставки)
              AND lower(address_from) = 'megafon'
              AND chnl_code = 'sms'
              GROUP BY nps.snap_date, nps.sk_subs_id,nps.measure_id
  ) sms
  ON tariff.snap_date = sms.snap_date AND tariff.sk_subs_id = sms.sk_subs_id 
  --ESIM
  LEFT JOIN (
       SELECT --+parallel(10)
              SK_SUBS_ID, MAX(subs_connect_date) AS subs_connect_date,
              MAX(CASE WHEN b.def LIKE 'ESIM%' THEN 'ESIM'
              WHEN b.def LIKE '%USIM%' THEN 'USIM'
              WHEN b.def LIKE '%ISIM%' THEN 'ISIM'
              WHEN b.def LIKE '%M2M%' THEN 'M2M' ELSE 'Прочее' END) AS SIM_GROUP
       FROM PUB_DS.A_SUBS_KPIS_MONTHLY a
       LEFT OUTER JOIN PUB_DS.BIS_SIM_TYPES b
                       ON b.STYP_ID=a.STYP_STYP_ID AND b.BILLING_FILIAL_ID=a.BILLING_FILIAL_ID  
       WHERE start_date='01.01.2023' 
       GROUP BY SK_SUBS_ID
  ) esim
  ON tariff.sk_subs_id =  esim.sk_subs_id and tariff.snap_date >= esim.subs_connect_date
  --MNP-IN
  LEFT JOIN (
       select --+parallel(10) 
              distinct sk_subs_id mnp_in_subs, msisdn, coalesce(STLS_NUM_DATE,NAVI_DATE) STLS_NUM_DATE
       from
       (
              select --+parallel(10)
                     sk_subs_id,
                     MSISDN,
                     trunc (STLS_NUM_DATE) STLS_NUM_DATE, trunc (NAVI_DATE) NAVI_DATE,
                     case when TRANSFER_ROLE_ID = '1' then 'Исходящий MNP' else 'Входящий MNP' end TRANSFER_ROLE,
                     row_number() over (partition by MSISDN order by STLS_NUM_DATE desc) rn
              from pub_ds.f_mnp_subs_clnt 
              where STLS_NUM_DATE < sysdate and (TRANSFERRED = '1' and STLS_STLS_NUM = '10')
              and (R_MNCO_ID = '02' or R_MNCO_ID = '11') and TRANSFER_ROLE_ID = '2' )
              where rn = '1' and TRANSFER_ROLE = 'Входящий MNP'
  ) mnp
  ON tariff.msisdn = mnp.msisdn and tariff.snap_date >= mnp.STLS_NUM_DATE; 
commit;              

--КОПИРОВАНИЕ ДАННЫХ В ЦЕЛЕВУЮ ТАБЛИЦУ
DELETE FROM fin_ba.mr_nps_survey_results
WHERE 1=1; 
INSERT INTO fin_ba.mr_nps_survey_results
SELECT * FROM fin_ba.mr_nps_survey_results_hist
WHERE snap_date >= '01.01.2023'; 