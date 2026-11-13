//! Настройки индекса: то, что лежит рядом с ним и меняется на живом узле.
//!
//! ## Почему в индексе, а не только во флагах
//!
//! Прежде всё это жило в аргументах запуска и в локальных переменных `cmd_serve`.
//! Следствий было два, и оба неприятные. Первое: настройка терялась при
//! перезапуске, если её не вписали в systemd-юнит, — то есть индекс, собранный с
//! одной нарезкой, поднимался с другой. Второе: поменять её на работающем узле
//! было нельзя вовсе, а именно это и нужно, когда приём внезапно вырос.
//!
//! Настройки принадлежат **индексу**, а не запуску: целевой размер сегмента
//! осмыслен только вместе с тем, что уже нарезано. Поэтому они лежат в его
//! каталоге и переживают перезапуск.
//!
//! ## Порядок старшинства
//!
//! Аргумент запуска → файл настроек → умолчание. Аргумент старше нарочно: им
//! чинят узел, который сам себя настроил во вред, и для этого он не должен
//! требовать правки файла в каталоге, который, может, и не открывается.
//!
//! Аргумент при этом **не записывается**: он на один запуск. Записывает только
//! `PUT /_settings`.
//!
//! ## Что можно менять на ходу, а что нет
//!
//! Обслуживание читает свои настройки на каждом заходе, поэтому целевой размер,
//! окно укрупнения, порог уборки и фора применяются со следующего захода —
//! секунды. Свежесть тоже: поток сброса по времени сверяется с настройками на
//! каждом круге.
//!
//! Ширина слияния — со следующего слияния, а не со следующего захода: уже
//! идущее держит ту ширину, с которой начало. Поменять её на ходу значило бы
//! либо бросить работу, либо соврать про число потоков.
//!
//! Число документов на сегмент — иное дело. Оно задано буферу приёма при
//! создании, и уже набранное им останется набранным: сменить потолок посреди
//! набора значит либо выбросить набранное, либо соврать про размер. Поэтому оно
//! применяется **со следующего сегмента**, и ответ об этом говорит прямо.

use std::path::Path;

use crate::json::{self, Json};

/// Имя файла настроек в каталоге индекса.
pub const FILE: &str = "settings.json";

/// Настройки индекса.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Settings {
    /// Сколько ждать, прежде чем сбросить непустой буфер. `0` — не ждать вовсе,
    /// то есть сбрасывать только по числу документов.
    ///
    /// ## Зачем это вообще есть
    ///
    /// Сброс по одному лишь счётчику даёт свежесть, обратную скорости приёма:
    /// `flush_docs / (документов в секунду)`. При двухстах тысячах на сегмент и
    /// десяти документах в секунду это **пять с половиной часов** до видимости, и
    /// столько же растёт журнал, потому что отметка двигается только при сбросе.
    /// Медленная база получала и невидимые документы, и разбухший журнал, и
    /// долгий подъём.
    ///
    /// Секунда — то же умолчание, что у `index.refresh_interval` в
    /// Elasticsearch, и по той же причине: это порог, ниже которого человек не
    /// отличит «сразу» от «почти сразу», а выше — начинает замечать.
    pub refresh_interval_ms: u64,
    /// Сколько документов должно накопиться для обычного сброса по времени.
    ///
    /// Единица сохраняет прежнюю семантику: первый же документ станет видимым
    /// после `refresh_interval_ms`. Для служебных update-heavy индексов это
    /// можно поднять, чтобы редкие lease/heartbeat не превращались каждый в
    /// отдельный сегмент. Долговечность от этого не меняется: ответ записи всё
    /// равно даётся только после `fsync` журнала.
    pub refresh_min_docs: u32,
    /// Жёсткий предел ожидания малой пачки до публикации.
    ///
    /// `refresh_min_docs` без этого предела мог бы навсегда оставить редкий
    /// документ только в WAL и памяти. Предел начинает действовать с первого
    /// документа непустого буфера и гарантирует eventual visibility даже при
    /// единственной записи. При `refresh_min_docs = 1` поле ни на что не влияет.
    pub refresh_max_interval_ms: u64,
    /// До скольких документов буфер приёма участвует в поиске.
    ///
    /// Буфер становится видимым поиску так: из него собирается живой сегмент,
    /// который входит в состав наравне с обычными. Собирается он **целиком и
    /// заново** на каждом refresh — так живой сегмент всегда один, состав не
    /// растёт и слияние живых не нужно. Платой за это служит пересборка размером
    /// с буфер.
    ///
    /// Для служебных индексов это копейки: между сбросами там лежат десятки
    /// документов, и запись становится видимой почти сразу после ACK. Для
    /// массовой загрузки — разорительно: миллион документов, пересобираемый раз
    /// в секунду, съел бы больше, чем сама загрузка.
    ///
    /// Отсюда порог. Выше него живой сегмент не собирается, и видимость
    /// возвращается к прежнему правилу — по сбросу на диск. Это не отказ и не
    /// деградация, а другой режим работы: массовой загрузке нужна пропускная
    /// способность, а не свежесть.
    ///
    /// Ноль выключает живой сегмент совсем: поиск снова видит только
    /// опубликованное.
    pub live_max_docs: u32,
    /// Окно durable group commit журнала в миллисекундах.
    ///
    /// Первый ожидающий `fsync` ненадолго уступает соседним запросам, после чего
    /// один `sync_all` фиксирует их общий рубеж. ACK по-прежнему не выходит до
    /// диска; меняются только число системных вызовов и добавленная задержка.
    pub wal_group_commit_ms: u64,
    /// Сколько запросов записи одновременно допускается внутрь узла.
    ///
    /// Сюда входят работающий запрос и ожидающие ingest/layout. Следующий
    /// получает быстрый `429`, а не занимает поток соединения на минуты за
    /// большим flush. Это предел запросов, не документов: размер тела отдельно
    /// ограничен HTTP-приёмником.
    pub max_pending_writes: usize,
    /// С какого долга L0 приём начинает пропорционально уступать compaction.
    pub l0_soft_segments: usize,
    /// С какого долга новые записи получают bounded retryable backpressure.
    /// Обязан быть строго выше мягкого порога.
    pub l0_hard_segments: usize,
    /// Сколько `/_flush` ждёт завершения фоновой публикации.
    ///
    /// По истечении срока запрос отвечает `202`, а уже начатая публикация
    /// продолжается. Ноль означает немедленный `202`. Так внешний watchdog не
    /// висит за общей операцией, но сам flush не бросается посередине.
    pub flush_wait_ms: u64,
    /// Освобождать page cache опубликованных сегментов после сброса.
    /// По умолчанию выключено: свежие сегменты остаются тёплыми для поиска.
    pub drop_flushed_page_cache: bool,
    /// Документов на сегмент у приёма.
    ///
    /// ## Почему столько же, сколько `target_docs`
    ///
    /// Меньшее значение означает, что содержимое каждого документа сжимается
    /// **дважды**: один раз при сбросе, второй — при укрупнении до целевого
    /// размера. Сжатие содержимого — самая дорогая часть сборки сегмента; на
    /// профиле она перевешивает и разбор, и постинги вместе взятые.
    ///
    /// Замерено на 10 млн реальных сообщений, целевой сегмент 1 млн:
    ///
    /// | сброс по | до готовности | док/с | сегментов | на диске |
    /// |---|---:|---:|---:|---:|
    /// | 200 000 (прежнее умолчание) | 136.5 с | 73 272 | 16 | 746 МБ |
    /// | **1 000 000** | **73.2 с** | **136 664** | 18 | **718 МБ** |
    ///
    /// То есть **вдвое быстрее**, и сегментов столько же, и на диске меньше.
    ///
    /// Свежесть от этого не страдает: её держит `refresh_interval_ms`, то есть
    /// время, а не счётчик. Память — `flush_bytes` у приёма, второй порог рядом
    /// с этим.
    pub flush_docs: u32,
    /// Потолок буфера приёма в мегабайтах. `0` — «сколько решит узел».
    ///
    /// Второй порог рядом со счётчиком документов, и он не дублирует его:
    /// миллион сообщений по семьдесят байт и миллион статей по мегабайту — это
    /// семьдесят мегабайт и терабайт, а «на сегмент» у них одно и то же число.
    ///
    /// Умолчание — ноль, и это значит **не «без предела»**, а «десятая часть
    /// доступной узлу памяти, но не меньше 256 МБ и не больше 4 ГБ». Зашитое
    /// число здесь неверно дважды: на машине в четыре гигабайта оно отправило
    /// бы узел в своп, на машине в сто двадцать восемь — заставило бы резать
    /// сегменты вчетверо мельче, чем стоило бы. Доля памяти — та же форма
    /// ответа, что у `indices.memory.index_buffer_size` в Elasticsearch.
    ///
    /// Предел контрольной группы узел учитывает: контейнер на два гигабайта
    /// видит в `/proc/meminfo` всю память машины, и поверить ей значило бы
    /// завести буфер, за который придёт ядро.
    pub flush_mb: u64,
    /// Целевой размер сегмента для укрупнения.
    ///
    /// Замеренный минимум держится за **размер** сегмента, а не за их число: на
    /// 10 млн он там же, где на 50 млн. Поэтому подгонять его под число ядер или
    /// под размер корпуса не нужно — число сегментов получается само.
    ///
    /// Но потолок у него есть, и не круглый: `postings.bin` и `pos.bin` внутри
    /// сегмента адресуются четырьмя байтами. Поэтому [`Settings::check`] держит
    /// его в разумных границах, а не принимает что попало.
    pub target_docs: usize,
    /// Сколько сегментов брать за одно укрупнение.
    pub merge_at_once: usize,
    /// С какой доли мёртвого сегмент стоит переписать.
    pub dead_fraction: f64,
    /// Сколько ждать перед удалением исходных каталогов после укрупнения.
    pub grace_ms: u64,
    /// Сколько потоков отдавать слиянию. `0` — четверть ядер, но не меньше
    /// одного.
    ///
    /// ## Зачем это настраивается
    ///
    /// Сборка хранимого содержимого поднимает столько потоков, сколько у узла
    /// ядер. Пока слияние занимало секунды, это было незаметно; на сегменте в
    /// пять гигабайт оно занимает минуты, и всё это время фоновое обслуживание
    /// отбирает ядра у приёма, который сжимает своё содержимое ровно теми же
    /// ядрами. Узел при этом выглядит так, будто приём беспричинно проседает
    /// втрое, — а причина в том, что «фоновая» работа фоновой не была.
    ///
    /// Умолчание — доля, а не число: узел на четырёх ядрах и узел на
    /// шестидесяти четырёх нуждаются в разном. Тот же ответ, что у
    /// `index.merge.scheduler.max_thread_count` в Elasticsearch, только считаный
    /// от ядер, а не от дисков.
    pub merge_threads: usize,
    /// Потолок слитого сегмента в мегабайтах. `0` — движок решает сам.
    ///
    /// ## Почему потолок в байтах, а не в документах
    ///
    /// Одинаковые по числу документов сегменты отличаются по байтам на порядок:
    /// в реальных корпусах текстов хвост бывает тяжелее середины впятеро (805 байт на документ
    /// против 150). «Миллион документов» означает то 150 МБ, то 800 МБ, и мерить
    /// им размер значит не мерить его вовсе. У Lucene по той же причине
    /// `maxMergedSegmentMB`, а не число документов.
    ///
    /// Достижимый размер — около **половины** потолка: сегмент замораживается на
    /// половине, чтобы всегда оставалось место слить его с равным. Хотите
    /// пятигигабайтные сегменты — ставьте десять тысяч.
    ///
    /// Ноль значит «как решит движок» (сейчас 2 ГБ), а не нулевой потолок:
    /// настройка, у которой ноль выключал бы укрупнение, наказывала бы за
    /// попытку её не настраивать.
    pub max_merged_mb: u64,
    /// Пол яруса в мегабайтах: всё мельче считается одного размера. `0` — сам.
    ///
    /// Без пола мелочь не сливается с мелочью: окно набирается по близости
    /// размеров, а у двух сегментов по килобайту и по десяти килобайтам разница
    /// десятикратная. Пол их выравнивает, и они уходят пачкой по `merge_at_once`,
    /// а не парами. У Lucene это `floorSegmentMB` = 2 МБ, у нас больше: сегмент
    /// приёма и так около семидесяти мегабайт, и двухмегабайтный пол не
    /// сработал бы никогда.
    pub floor_mb: u64,
    /// Сколько сегментов позволено одному ярусу. `0` — сам.
    ///
    /// Это и есть тормоз против бури слияний: пока сегментов не больше
    /// позволенного, индекс не считается перегруженным и укрупнение молчит.
    /// Меньше — слияния идут чаще, чем окупаются; больше — каждый запрос платит
    /// за лишние сегменты.
    pub per_tier: usize,
    /// Чем укрупнять: `tiered` (умолчание) или `flat` — прежняя плоская.
    ///
    /// Рычаг оператору: сказать ярусному укрупнению «не сейчас» иначе можно
    /// только через `--no-maintain`, а он гасит заодно и уборку, то есть мёртвое
    /// перестаёт возвращаться на диск. Непонятное слово — не отказ, а ярусная и
    /// жалоба: узел, не поднявшийся из-за опечатки в необязательном ключе, хуже
    /// узла, который поднялся с умолчанием и сказал об этом.
    pub merge_policy: crate::merge::MergeKind,
    /// Предел рабочей памяти одного запроса, в мегабайтах. `0` — без предела.
    ///
    /// Размыкатель, как `indices.breaker.request` у Elasticsearch. Запрос из
    /// тридцати байт умеет попросить сотню мегабайт — тридцать две агрегации по
    /// шестьдесят пять тысяч корзин, — и цена его зависит ещё и от числа
    /// сегментов, которого спрашивающий не видит. Узел, умерший по памяти,
    /// теряет все остальные запросы, а не только этот, поэтому правильный ответ
    /// — отказ с причиной. Разбор — у [`crate::budget`].
    pub memory_budget_mb: u64,
    /// Отвергать ли документ целиком, если часть значений не прочлась.
    ///
    /// По умолчанию нет: узел принимает годное, а о непринятом **говорит** в
    /// ответе. Это середина между молчанием (было раньше) и отказом: заливка
    /// на миллион документов не должна валиться из-за одного кривого поля, но и
    /// узнавать о потере через недели по пустой выдаче — не дело.
    ///
    /// Как поступать с пачкой, в которой что-то не дошло до индекса.
    ///
    /// ## Почему по умолчанию отказ
    ///
    /// Прежде пачка с потерями отвечала двумястами, а о потерях говорилось
    /// только в теле. Клиент с `raise_for_status()` или `curl -f` терял данные
    /// **молча** — а это поведение по умолчанию почти у всякой обёртки. Код
    /// ответа обязан быть громким сигналом; обязанность читать тело, не
    /// подкреплённая кодом, не работает.
    ///
    /// Логическое значение принимается по-прежнему: `true` — это `batch`,
    /// `false` — `report`, чтобы прежние файлы настроек поднимались как есть.
    pub strict_ingest: Strict,
}

/// Что делать с пачкой, в которой что-то не дошло до индекса.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Strict {
    /// Принять годное, о потерях сказать в теле. Ответ 200.
    Report,
    /// Негодные документы не принимать, остальные принять. Ответ 400.
    Document,
    /// Отвергнуть всю пачку. Ответ 400.
    Batch,
}

impl Strict {
    pub fn name(self) -> &'static str {
        match self {
            Strict::Report => "report",
            Strict::Document => "document",
            Strict::Batch => "batch",
        }
    }

    pub fn parse(word: &str) -> Option<Strict> {
        Some(match word {
            "report" => Strict::Report,
            "document" => Strict::Document,
            "batch" => Strict::Batch,
            _ => return None,
        })
    }

    /// Принимается ли пачка целиком, несмотря на потери.
    pub fn lenient(self) -> bool {
        self == Strict::Report
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            refresh_interval_ms: 1000,
            refresh_min_docs: 1,
            refresh_max_interval_ms: 30_000,
            // Десять тысяч — порядок, на котором пересборка ещё дешевле сети:
            // сегмент такого размера собирается единицы миллисекунд, а типовой
            // служебный буфер меньше на два порядка. Массовая загрузка проходит
            // порог на первой же секунде и дальше идёт прежним путём.
            live_max_docs: 10_000,
            wal_group_commit_ms: 2,
            max_pending_writes: 64,
            l0_soft_segments: 8,
            l0_hard_segments: 24,
            flush_wait_ms: 5_000,
            drop_flushed_page_cache: false,
            flush_docs: 1_000_000,
            flush_mb: 0,
            target_docs: 1_000_000,
            merge_at_once: 8,
            dead_fraction: 0.20,
            grace_ms: 2000,
            merge_threads: 0,
            // Все три — ноль, то есть «как решит движок». Числа живут у
            // `merge::Tier`, а не здесь: файл настроек не должен повторять
            // умолчания движка, иначе они разойдутся, и никто не заметит.
            max_merged_mb: 0,
            floor_mb: 0,
            per_tier: 0,
            merge_policy: crate::merge::MergeKind::default(),
            // Двести пятьдесят шесть мегабайт: выше самого дорогого разумного
            // запроса и заметно ниже памяти, которой узел располагает. Ноль
            // выключает размыкатель, и узел работает как до его появления.
            memory_budget_mb: 256,
            strict_ingest: Strict::Batch,
        }
    }
}

/// Потолок целевого размера сегмента.
///
/// Не круглое число, а следствие **смещений**: они внутри сегмента
/// четырёхбайтные, поэтому ни `postings.bin`, ни `pos.bin`, ни `terms.bin`, ни
/// `dict.blob`, ни `ids.bin` не могут перевалить за четыре гигабайта — на
/// текстовом корпусе это наступает в районе десятков миллионов документов. Выше
/// — отказ, а не «попробуем»: сторожа (`merge::fits_offset`, `ids::fits_anchor`,
/// `dict::fits_anchor`) остановят укрупнение посреди работы, и узел останется с
/// сегментами, которые не сливаются.
///
/// ## Про память здесь больше ничего нет, и это перемена
///
/// Прежде вторым пределом стоял пик памяти слияния: оно держало позиции целиком,
/// все идентификаторы отдельными строками и словарь в двух представлениях, то
/// есть требовало примерно байт памяти на байт сегмента. Теперь всё это пишется
/// потоком (`merge::Sink`, `dict::Writer`, `ids::Writer`), и от размера сегмента
/// пик больше не зависит: линейными по числу документов остались полоса норм —
/// байт на поле на документ, — карта перенумерации погашенных и сам фильтр
/// Блума. Замерено на четырёх миллионах документов: 748 МБ против 390 МБ пика
/// на одном и том же входе, вывод побайтово тот же.
///
/// Потолок от этого не поднялся, потому что держат его не память, а смещения.
///
/// ## Но выбирать `target_docs` по этому потолку нельзя
///
/// Потолок говорит, какой сегмент **представим** форматом, а не какой узел
/// сможет собрать. Держит его вторая величина — память слияния, и она зависит
/// не от числа документов, а от того, сколько у них хранимого содержимого.
///
/// Замерено 12 августа 2026 счётчиком выделений ([`crate::alloc::peak`], то есть
/// куча, а не `RSS`): пик слияния обгоняет размер собираемого сегмента в 3.1–3.3
/// раза в один поток сжатия и в 5.2–6.6 при восьми. Проверено на двух корпусах,
/// отличающихся длиной тела вчетверо, и на обоих путях содержимого — переносе и
/// пересборке; разница между путями в пределах шума. Сегмент без хранимого стоит
/// куда меньше: там линейного по документам единицы байт, и это закреплено
/// отдельно (`merge::peak_tests`).
///
/// Отсюда правило: `target_docs` выбирается по памяти узла и по тому, сколько
/// байт хранимого приходится на документ. Сегмент в пятьдесят миллионов
/// документов по сотне байт хранимого — это гигабайты содержимого и десятки
/// гигабайт пика.
///
/// ## Почему это не проверяется здесь
///
/// Настройки не знают корпуса. Одно и то же `target_docs` при коротких
/// документах без хранимого и при длинных с хранимым отличается по памяти на
/// два порядка, и всякое число, проверенное на этой строке, было бы выдумано.
///
/// Поэтому предел стоит там, где корпус уже известен: ярусная политика меряет
/// окно не только байтами и документами, но и памятью ([`crate::merge::Tier`],
/// поле `max_peak`), и набирает его, пока влезает. Не влезает — окно короче; не
/// влезает даже одиночный сегмент — работа не берётся вовсе.
///
/// Последствие — сегментов в индексе больше обычного, и это осознанный размен:
/// запрос платит за сегмент около микросекунды, а слияние, не влезшее в память,
/// не отказывает — оно гибнет от ядра посреди работы, унося с собой все
/// соединения узла.
///
pub const MAX_TARGET_DOCS: usize = 50_000_000;

impl Settings {
    /// Проверяет настройки на осмысленность. `Err` — отказ с причиной.
    ///
    /// Проверка отдельная от разбора нарочно: одни и те же границы нужны и файлу,
    /// и запросу `PUT`, и аргументам запуска. Разложи их по трём местам — и
    /// однажды одно из трёх пропустит то, что двое других ловят.
    pub fn check(&self) -> Result<(), String> {
        if self.flush_docs == 0 {
            return Err(
                "flush_docs: ноль документов на сегмент — приём никогда не сбросится".into()
            );
        }
        if self.refresh_min_docs == 0 {
            return Err("refresh_min_docs: ноль сделал бы условие публикации недостижимым".into());
        }
        if self.refresh_min_docs > self.flush_docs {
            return Err(format!(
                "refresh_min_docs {} больше flush_docs {} — раньше всегда сработает предел сегмента",
                self.refresh_min_docs, self.flush_docs
            ));
        }
        if self.refresh_min_docs > 1
            && self.refresh_interval_ms > 0
            && self.refresh_max_interval_ms < self.refresh_interval_ms
        {
            return Err(format!(
                "refresh_max_interval_ms {} меньше refresh_interval_ms {} — жёсткий предел наступал бы раньше обычного срока",
                self.refresh_max_interval_ms, self.refresh_interval_ms
            ));
        }
        if self.refresh_max_interval_ms > 3_600_000 {
            return Err(
                "refresh_max_interval_ms: больше часа перестаёт быть пределом видимости".into()
            );
        }
        if self.wal_group_commit_ms > 100 {
            return Err("wal_group_commit_ms: больше ста миллисекунд превращает durability в видимую задержку".into());
        }
        if self.max_pending_writes == 0 || self.max_pending_writes > 4096 {
            return Err("max_pending_writes: ожидается число от 1 до 4096".into());
        }
        if self.l0_soft_segments == 0 || self.l0_soft_segments >= self.l0_hard_segments {
            return Err(format!(
                "l0_soft_segments {} обязан быть положительным и меньше l0_hard_segments {}",
                self.l0_soft_segments, self.l0_hard_segments
            ));
        }
        if self.l0_hard_segments > 4096 {
            return Err(
                "l0_hard_segments: больше 4096 уже означает потерю управления долгом".into()
            );
        }
        if self.flush_wait_ms > 60_000 {
            return Err(
                "flush_wait_ms: больше минуты снова превращает bounded wait в зависание".into()
            );
        }
        if self.target_docs < self.flush_docs as usize {
            return Err(format!(
                "target_docs {} меньше flush_docs {} — укрупнять было бы нечего: \
                 свежий сегмент уже крупнее цели",
                self.target_docs, self.flush_docs
            ));
        }
        if self.target_docs > MAX_TARGET_DOCS {
            return Err(format!(
                "target_docs {}: потолок {MAX_TARGET_DOCS}. Выше него смещения внутри \
                 сегмента перестают влезать в четыре байта, и укрупнение отказывает \
                 посреди работы",
                self.target_docs
            ));
        }
        if self.merge_at_once < 2 {
            return Err("merge_at_once: меньше двух — это не слияние, а переписывание".into());
        }
        if self.merge_at_once > 256 {
            return Err("merge_at_once: больше двухсот пятидесяти шести — одно слияние \
                        держало бы в памяти слишком много сегментов разом"
                .into());
        }
        // Потолок не про возможности машины, а про смысл: слияние — работа
        // фоновая, и отдать ей больше потоков, чем у узла ядер, значит только
        // добавить переключений там, где и без них тесно.
        if self.merge_threads > 1024 {
            return Err(format!(
                "merge_threads {}: больше тысячи потоков на одно слияние — это \
                 переключения контекста вместо работы",
                self.merge_threads
            ));
        }
        if !(0.0..=1.0).contains(&self.dead_fraction) {
            return Err(format!(
                "dead_fraction {}: доля мёртвого лежит между нулём и единицей",
                self.dead_fraction
            ));
        }
        // Час — не предел возможностей, а предел осмысленности: свежесть, которую
        // ждут час, не свежесть, а недоразумение.
        if self.refresh_interval_ms > 3_600_000 {
            return Err("refresh_interval_ms: больше часа — это уже не свежесть".into());
        }
        Ok(())
    }

    /// Настройки обслуживания, собранные из этих.
    pub fn maintain(&self) -> crate::maintain::Config {
        crate::maintain::Config {
            target_docs: self.target_docs,
            at_once: self.merge_at_once,
            dead_fraction: self.dead_fraction,
            grace_ms: self.grace_ms,
            merge_threads: self.merge_threads,
            max_merged_mb: self.max_merged_mb,
            floor_mb: self.floor_mb,
            per_tier: self.per_tier,
            // Слово разбирает `merge`, а не мы: читателей у него двое — этот
            // файл и переменная среды, — и слова у них обязаны быть одни.
            merge_policy: self.merge_policy,
            // Ноль значит «вывести из машины». Памяти у слияния не настройка, а
            // обстоятельство: файл настроек переезжает между узлами вместе с
            // индексом, а память у них разная.
            merge_budget_bytes: 0,
            // Свободное место узнаёт узел: у ядра индекса нет и не должно быть
            // сведений о файловых системах.
            disk_free_bytes: 0,
        }
    }

    pub fn to_json(&self) -> Json {
        json::obj(vec![
            ("refresh_interval_ms", Json::Int(self.refresh_interval_ms as i64)),
            ("refresh_min_docs", Json::Int(self.refresh_min_docs as i64)),
            ("refresh_max_interval_ms", Json::Int(self.refresh_max_interval_ms as i64)),
            ("live_max_docs", Json::Int(self.live_max_docs as i64)),
            ("wal_group_commit_ms", Json::Int(self.wal_group_commit_ms as i64)),
            ("max_pending_writes", Json::Int(self.max_pending_writes as i64)),
            ("l0_soft_segments", Json::Int(self.l0_soft_segments as i64)),
            ("l0_hard_segments", Json::Int(self.l0_hard_segments as i64)),
            ("flush_wait_ms", Json::Int(self.flush_wait_ms as i64)),
            ("drop_flushed_page_cache", Json::Bool(self.drop_flushed_page_cache)),
            ("flush_docs", Json::Int(self.flush_docs as i64)),
            ("flush_mb", Json::Int(self.flush_mb as i64)),
            ("target_docs", Json::Int(self.target_docs as i64)),
            ("merge_at_once", Json::Int(self.merge_at_once as i64)),
            ("dead_fraction", Json::Num(self.dead_fraction)),
            ("grace_ms", Json::Int(self.grace_ms as i64)),
            ("merge_threads", Json::Int(self.merge_threads as i64)),
            ("max_merged_mb", Json::Int(self.max_merged_mb as i64)),
            ("floor_mb", Json::Int(self.floor_mb as i64)),
            ("per_tier", Json::Int(self.per_tier as i64)),
            ("merge_policy", Json::Str(self.merge_policy.word().to_string())),
            ("memory_budget_mb", Json::Int(self.memory_budget_mb as i64)),
            ("strict_ingest", Json::Str(self.strict_ingest.name().to_string())),
        ])
    }

    /// Накладывает на себя то, что названо в объекте. Неназванное не трогается.
    ///
    /// Частичное обновление, как `PUT /<индекс>/_settings` у Elasticsearch:
    /// прислать одну настройку и не перечислять остальные — обычное дело, а
    /// требование прислать всё означало бы, что клиент обязан сначала прочитать
    /// и переписать, то есть гонку на двух администраторах.
    ///
    /// Незнакомый ключ — **отказ**. Опечатка в имени настройки иначе выглядела бы
    /// как успех, а настройка осталась бы прежней.
    pub fn patch(&mut self, v: &Json) -> Result<(), String> {
        let Some(kv) = v.as_obj() else {
            return Err("настройки: ожидается объект".into());
        };
        for (k, val) in kv.iter() {
            match k.as_str() {
                "refresh_interval_ms" => self.refresh_interval_ms = want_u64(val, k)?,
                "refresh_min_docs" => {
                    self.refresh_min_docs = u32::try_from(want_u64(val, k)?)
                        .map_err(|_| format!("{k}: не влезает в четыре байта"))?
                }
                "refresh_max_interval_ms" => self.refresh_max_interval_ms = want_u64(val, k)?,
                "live_max_docs" => {
                    self.live_max_docs = u32::try_from(want_u64(val, k)?)
                        .map_err(|_| format!("{k}: не влезает в четыре байта"))?
                }
                "wal_group_commit_ms" => self.wal_group_commit_ms = want_u64(val, k)?,
                "max_pending_writes" => self.max_pending_writes = want_u64(val, k)? as usize,
                "l0_soft_segments" => self.l0_soft_segments = want_u64(val, k)? as usize,
                "l0_hard_segments" => self.l0_hard_segments = want_u64(val, k)? as usize,
                "flush_wait_ms" => self.flush_wait_ms = want_u64(val, k)?,
                "drop_flushed_page_cache" => {
                    self.drop_flushed_page_cache =
                        val.as_bool().ok_or_else(|| format!("{k}: ожидается true или false"))?;
                }
                "flush_mb" => self.flush_mb = want_u64(val, k)?,
                "flush_docs" => {
                    self.flush_docs = u32::try_from(want_u64(val, k)?)
                        .map_err(|_| format!("{k}: не влезает в четыре байта"))?
                }
                "target_docs" => self.target_docs = want_u64(val, k)? as usize,
                "merge_at_once" => self.merge_at_once = want_u64(val, k)? as usize,
                "dead_fraction" => {
                    self.dead_fraction =
                        val.as_f64().ok_or_else(|| format!("{k}: ожидается число"))?
                }
                "grace_ms" => self.grace_ms = want_u64(val, k)?,
                "merge_threads" => self.merge_threads = want_u64(val, k)? as usize,
                "max_merged_mb" => self.max_merged_mb = want_u64(val, k)?,
                "floor_mb" => self.floor_mb = want_u64(val, k)?,
                "per_tier" => self.per_tier = want_u64(val, k)? as usize,
                "merge_policy" => {
                    let w = val.as_str().ok_or_else(|| format!("{k}: ожидается строка"))?;
                    // Отказ, а не тихое умолчание: здесь оператор пишет ключ
                    // явно, и опечатка в явном — это опечатка, а не «как выйдет».
                    // Переменная среды прощает опечатку по другой причине: там
                    // ключ необязательный, и не подняться из-за него хуже.
                    self.merge_policy = crate::merge::MergeKind::parse(w).ok_or_else(|| {
                        format!("{k}: ожидается tiered, update_heavy или flat, а не {w:?}")
                    })?;
                }
                "memory_budget_mb" => self.memory_budget_mb = want_u64(val, k)?,
                "strict_ingest" if matches!(val, Json::Str(_)) => {
                    let word = val.as_str().unwrap_or("");
                    self.strict_ingest = Strict::parse(word).ok_or_else(|| {
                        format!(
                            "strict_ingest: {word:?} — бывает report (принять годное и \
                             сказать о потерях), document (отвергнуть негодные документы) \
                             или batch (отвергнуть всю пачку)"
                        )
                    })?;
                }
                "strict_ingest" => {
                    // Логическое принимается как прежде: истина — отказ всей
                    // пачки, ложь — приём с отчётом. Иначе прежние файлы
                    // настроек перестали бы подниматься.
                    let on = val
                        .as_bool()
                        .ok_or_else(|| format!("{k}: ожидается true, false или слово"))?;
                    self.strict_ingest = if on { Strict::Batch } else { Strict::Report };
                }
                "_" | "comment" => {}
                other => {
                    return Err(format!(
                        "ключ {other:?} неизвестен; ожидались: refresh_interval_ms, \
                         refresh_min_docs, refresh_max_interval_ms, wal_group_commit_ms, \
                         max_pending_writes, l0_soft_segments, l0_hard_segments, \
                         flush_wait_ms, drop_flushed_page_cache, flush_docs, flush_mb, \
                         target_docs, merge_at_once, dead_fraction, \
                         grace_ms, merge_threads, max_merged_mb, floor_mb, per_tier, \
                         merge_policy, \
                         memory_budget_mb, strict_ingest"
                    ))
                }
            }
        }
        self.check()
    }
}

fn want_u64(v: &Json, k: &str) -> Result<u64, String> {
    match v.as_i64() {
        Some(n) if n >= 0 => Ok(n as u64),
        Some(n) => Err(format!("{k}: {n} меньше нуля")),
        None => Err(format!("{k}: ожидается целое число")),
    }
}

/// Читает настройки индекса. Файла нет — умолчания.
///
/// Нечитаемый или негодный файл — **отказ**, а не молчаливый возврат к
/// умолчаниям: узел, поднявшийся с чужой нарезкой из-за опечатки, нарежет
/// сегменты не так, как весь остальной индекс, и заметят это по памяти через
/// недели.
pub fn load(dir: &str) -> Result<Settings, String> {
    let path = format!("{dir}/{FILE}");
    let bytes = match crate::fs::read_regular_limited(Path::new(&path), 64 * 1024, false) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Settings::default()),
        Err(e) => return Err(format!("{path}: {e}")),
    };
    let text = String::from_utf8(bytes).map_err(|e| format!("{path}: не UTF-8: {e}"))?;
    let doc = json::parse(&text).map_err(|e| format!("{path}: {e}"))?;
    let mut out = Settings::default();
    out.patch(&doc).map_err(|e| format!("{path}: {e}"))?;
    Ok(out)
}

/// Записывает настройки атомарно, как список сегментов и реестр полей.
pub fn save(dir: &str, s: &Settings) -> Result<(), String> {
    s.check()?;
    let mut body = s.to_json().to_pretty();
    body.push('\n');
    crate::fs::atomic_write(Path::new(dir).join(FILE).as_path(), body.as_bytes(), 0o600)
        .map_err(|e| format!("{dir}/{FILE}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir().join(format!("svindb-settings-{name}-{}", std::process::id()));
        std::fs::remove_dir_all(&d).ok();
        std::fs::create_dir_all(&d).unwrap();
        d.to_string_lossy().to_string()
    }

    #[test]
    fn absent_file_means_defaults() {
        let dir = tmp("absent");
        assert_eq!(load(&dir).unwrap(), Settings::default());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_round_trip_keeps_every_field() {
        let dir = tmp("round");
        let want = Settings {
            refresh_interval_ms: 250,
            refresh_min_docs: 32,
            refresh_max_interval_ms: 5_000,
            live_max_docs: 4_096,
            wal_group_commit_ms: 4,
            max_pending_writes: 12,
            l0_soft_segments: 6,
            l0_hard_segments: 18,
            flush_wait_ms: 750,
            drop_flushed_page_cache: true,
            flush_docs: 50_000,
            target_docs: 4_000_000,
            merge_at_once: 16,
            dead_fraction: 0.35,
            grace_ms: 500,
            merge_threads: 3,
            max_merged_mb: 4096,
            floor_mb: 32,
            per_tier: 6,
            merge_policy: crate::merge::MergeKind::Flat,
            memory_budget_mb: 64,
            flush_mb: 512,
            strict_ingest: Strict::Batch,
        };
        save(&dir, &want).unwrap();
        assert_eq!(load(&dir).unwrap(), want);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn dropping_flushed_page_cache_is_opt_in_and_requires_a_boolean() {
        let mut settings = Settings::default();
        assert!(!settings.drop_flushed_page_cache);
        settings.patch(&json::parse(r#"{"flush_wait_ms":100}"#).unwrap()).unwrap();
        assert!(!settings.drop_flushed_page_cache);
        settings.patch(&json::parse(r#"{"drop_flushed_page_cache":true}"#).unwrap()).unwrap();
        assert!(settings.drop_flushed_page_cache);
        settings.patch(&json::parse(r#"{"drop_flushed_page_cache":false}"#).unwrap()).unwrap();
        assert!(!settings.drop_flushed_page_cache);
        for value in ["1", "0", r#""true""#, "null"] {
            let patch = json::parse(&format!("{{\"drop_flushed_page_cache\":{value}}}")).unwrap();
            assert!(settings.patch(&patch).is_err(), "принято значение {value}");
        }
    }

    /// Частичное обновление меняет названное и не трогает остальное.
    #[test]
    fn a_patch_touches_only_what_it_names() {
        let mut s = Settings::default();
        s.patch(&json::parse(r#"{"refresh_interval_ms":100}"#).unwrap()).unwrap();
        assert_eq!(s.refresh_interval_ms, 100);
        assert_eq!(s.flush_docs, Settings::default().flush_docs, "тронуто неназванное");
        assert_eq!(s.target_docs, Settings::default().target_docs);
    }

    /// Опечатка в имени настройки — отказ, а не молчаливый успех.
    #[test]
    fn an_unknown_key_is_refused_not_ignored() {
        let mut s = Settings::default();
        let e = s
            .patch(&json::parse(r#"{"refresh_intervall_ms":100}"#).unwrap())
            .expect_err("опечатка принята");
        assert!(e.contains("refresh_intervall_ms"), "отказ не назвал ключ: {e}");
        assert!(e.contains("refresh_interval_ms"), "отказ не подсказал верный: {e}");
        assert_eq!(s, Settings::default(), "настройки поехали при отказе");
    }

    /// Негодный файл — отказ, а не тихий возврат к умолчаниям.
    ///
    /// Узел, поднявшийся с чужой нарезкой из-за опечатки, режет сегменты не так,
    /// как весь остальной индекс, и заметят это через недели по памяти.
    #[test]
    fn a_broken_file_refuses_instead_of_falling_back() {
        let dir = tmp("broken");
        std::fs::write(format!("{dir}/{FILE}"), "{\"target_docs\": ").unwrap();
        assert!(load(&dir).is_err(), "битый файл принят за умолчания");
        std::fs::write(format!("{dir}/{FILE}"), "{\"опечатка\": 1}").unwrap();
        assert!(load(&dir).is_err(), "незнакомый ключ принят за умолчания");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Границы проверяются, и каждая называет причину.
    #[test]
    fn nonsense_is_refused_with_a_reason() {
        let bad: Vec<(&str, &str)> = vec![
            (r#"{"flush_docs":0}"#, "никогда не сбросится"),
            (r#"{"target_docs":1000}"#, "меньше flush_docs"),
            (r#"{"target_docs":900000000}"#, "потолок"),
            (r#"{"merge_at_once":1}"#, "не слияние"),
            (r#"{"merge_at_once":9999}"#, "двухсот пятидесяти шести"),
            (r#"{"dead_fraction":1.5}"#, "между нулём и единицей"),
            (r#"{"refresh_interval_ms":99999999}"#, "не свежесть"),
            (r#"{"refresh_min_docs":0}"#, "недостижимым"),
            (r#"{"refresh_min_docs":2000000}"#, "больше flush_docs"),
            (r#"{"refresh_max_interval_ms":3600001}"#, "пределом видимости"),
            (r#"{"wal_group_commit_ms":101}"#, "ста миллисекунд"),
            (r#"{"max_pending_writes":0}"#, "от 1 до 4096"),
            (r#"{"l0_soft_segments":0}"#, "положительным"),
            (r#"{"l0_soft_segments":24}"#, "меньше l0_hard_segments"),
            (r#"{"l0_hard_segments":4097}"#, "больше 4096"),
            (r#"{"flush_wait_ms":60001}"#, "больше минуты"),
            (r#"{"grace_ms":-1}"#, "меньше нуля"),
        ];
        for (body, want) in bad {
            let mut s = Settings::default();
            let e = s
                .patch(&json::parse(body).unwrap())
                .err()
                .unwrap_or_else(|| panic!("принято негодное: {body}"));
            assert!(e.contains(want), "отказ на {body} не про то: {e}");
        }
    }

    /// Верхняя граница очереди записи принимается ровно, а не с ошибкой на
    /// единицу; следующее значение отвергается тем же валидатором.
    #[test]
    fn the_pending_write_limit_is_checked_on_both_sides() {
        let mut accepted = Settings::default();
        accepted
            .patch(&json::parse(r#"{"max_pending_writes":4096}"#).unwrap())
            .expect("сам верхний предел отвергнут");
        assert_eq!(accepted.max_pending_writes, 4096);

        let mut refused = Settings::default();
        let error = refused
            .patch(&json::parse(r#"{"max_pending_writes":4097}"#).unwrap())
            .expect_err("значение за верхним пределом принято");
        assert!(error.contains("4096"), "отказ не называет границу: {error}");
    }

    /// Настройки обслуживания собираются один в один.
    #[test]
    fn maintenance_config_follows_the_settings() {
        let s = Settings {
            target_docs: 3_000_000,
            merge_at_once: 4,
            dead_fraction: 0.5,
            grace_ms: 7,
            merge_threads: 2,
            ..Settings::default()
        };
        let c = s.maintain();
        assert_eq!(c.target_docs, 3_000_000);
        assert_eq!(c.at_once, 4);
        assert_eq!(c.dead_fraction, 0.5);
        assert_eq!(c.grace_ms, 7);
        assert_eq!(c.merge_threads, 2, "ширина слияния не дошла до обслуживания");
    }

    /// Пачка с потерями по умолчанию отвергается, а не принимается молча.
    ///
    /// Прежде она отвечала двумястами, и о потерях говорилось только в теле.
    /// Клиент с `raise_for_status()` или `curl -f` терял данные молча — а это
    /// поведение по умолчанию почти у всякой обёртки. Обязанность читать тело,
    /// не подкреплённая кодом ответа, не работает.
    #[test]
    fn a_batch_with_losses_is_refused_by_default() {
        assert_eq!(
            Settings::default().strict_ingest,
            Strict::Batch,
            "умолчание перестало быть строгим"
        );
        assert!(!Settings::default().strict_ingest.lenient());

        // Мягкое поведение остаётся настройкой, и логическое значение
        // принимается как прежде: иначе существующие файлы настроек перестали
        // бы подниматься.
        let mut s = Settings::default();
        s.patch(&json::parse(r#"{"strict_ingest":"report"}"#).unwrap()).unwrap();
        assert_eq!(s.strict_ingest, Strict::Report);
        assert!(s.strict_ingest.lenient());

        let mut s = Settings::default();
        s.patch(&json::parse(r#"{"strict_ingest":false}"#).unwrap()).unwrap();
        assert_eq!(s.strict_ingest, Strict::Report, "ложь обязана значить report");
        let mut s = Settings::default();
        s.patch(&json::parse(r#"{"strict_ingest":true}"#).unwrap()).unwrap();
        assert_eq!(s.strict_ingest, Strict::Batch, "истина обязана значить batch");

        // Негодное слово — отказ с перечнем того, что бывает.
        let mut s = Settings::default();
        let e = s
            .patch(&json::parse(r#"{"strict_ingest":"строго"}"#).unwrap())
            .expect_err("негодное слово принято");
        assert!(e.contains("report") && e.contains("batch"), "не тот отказ: {e}");
    }
}
