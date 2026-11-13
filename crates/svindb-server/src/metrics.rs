//! Метрики узла: что считаем и как отдаём.
//!
//! Механизм — в [`svindb_core::metrics`]; здесь только то, что относится к
//! **этому** узлу: виды запросов, общее состояние и печать.
//!
//! ## Почему текстовый вид Prometheus
//!
//! Он читается любым сборщиком и глазами, не требует ни зависимости, ни схемы,
//! ни рукопожатия. Двоичные виды экономят байты там, где их и так немного:
//! точку метрик спрашивают раз в пятнадцать секунд.

use svindb_core::metrics::{Counter, Hist};

/// Вид запроса. Разделение — по тому, **чем занят узел**, а не по пути.
///
/// Иначе `POST /` был бы одним видом на поиск, поиск по вектору и рассылку
/// шардам, у которых цена отличается на порядки, и общая гистограмма не сказала
/// бы ни о чём.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Search,
    Knn,
    StatsOnly,
    Fanout,
    Bulk,
    Put,
    Delete,
    Flush,
    /// Чтение одного документа по идентификатору. Отдельно от [`Kind::Search`]:
    /// у него нет ни обхода списков, ни ранжирования — только проба словаря
    /// идентификаторов и чтение хранилища. В общей гистограмме с поиском он
    /// сдвигал бы квантили вниз, изображая узел быстрее, чем он есть.
    Get,
    Health,
    Metrics,
    Settings,
    Snapshot,
    Security,
    Other,
}

impl Kind {
    pub const fn name(self) -> &'static str {
        match self {
            Kind::Search => "search",
            Kind::Knn => "knn",
            Kind::StatsOnly => "stats",
            Kind::Fanout => "fanout",
            Kind::Bulk => "bulk",
            Kind::Put => "put",
            Kind::Delete => "delete",
            Kind::Flush => "flush",
            Kind::Get => "get",
            Kind::Health => "health",
            Kind::Metrics => "metrics",
            Kind::Settings => "settings",
            Kind::Snapshot => "snapshot",
            Kind::Security => "security",
            Kind::Other => "other",
        }
    }

    pub const ALL: [Kind; 15] = [
        Kind::Search,
        Kind::Knn,
        Kind::StatsOnly,
        Kind::Fanout,
        Kind::Bulk,
        Kind::Put,
        Kind::Delete,
        Kind::Flush,
        Kind::Get,
        Kind::Health,
        Kind::Metrics,
        Kind::Settings,
        Kind::Snapshot,
        Kind::Security,
        Kind::Other,
    ];

    /// Вид по методу и пути. Тело не разбирается — это дорого и рано.
    ///
    /// Поиск по вектору и запрос одной статистики отличаются от обычного поиска
    /// только телом, поэтому вид у них уточняется позже, уже в обработчике
    /// (`refine`). Здесь — то, что видно из заголовка.
    /// Разряд запроса — по **разобранной** цели, а не по строке.
    ///
    /// Прежде здесь было своё сопоставление, третье по счёту после прав и
    /// выдачи. Оно тоже сравнивало путь точно, поэтому `POST /_bulk?refresh=true`
    /// считался «прочим» — то есть метрика молчала ровно про тот запрос, на
    /// котором расходились и права, и маршрут. Разбор один, в [`crate::route`].
    pub fn of(r: &crate::route::Route, coordinator: bool) -> Kind {
        use crate::route::Endpoint;
        match r.endpoint {
            Endpoint::Health => Kind::Health,
            Endpoint::Index
            | Endpoint::CatIndices
            | Endpoint::CatAliases
            | Endpoint::Alias
            | Endpoint::Metadata
            | Endpoint::Blocks
            | Endpoint::Open
            | Endpoint::Close
            | Endpoint::Rollover => Kind::Other,
            Endpoint::Metrics => Kind::Metrics,
            Endpoint::Schema | Endpoint::Validate | Endpoint::Stats | Endpoint::Inspect => {
                Kind::Other
            }
            Endpoint::Settings => Kind::Settings,
            Endpoint::Bulk | Endpoint::Doc => Kind::Bulk,
            // Пакетное чтение считается тем же разрядом, что и одиночное:
            // разряд отвечает на вопрос «сколько узел читает по идентификатору»,
            // а не «сколькими запросами это оформлено».
            Endpoint::DocGet | Endpoint::MGet => Kind::Get,
            Endpoint::Put | Endpoint::Mutate => Kind::Put,
            Endpoint::Delete => Kind::Delete,
            // Репликация считается приёмом: для узла это та же работа, и
            // отдельный разряд разошёлся бы с тем, что показывает нагрузку.
            Endpoint::Replicate => Kind::Bulk,
            Endpoint::Reshard => Kind::Delete,
            Endpoint::Flush => Kind::Flush,
            Endpoint::Search if coordinator => Kind::Fanout,
            Endpoint::Search => Kind::Search,
            Endpoint::Snapshot => Kind::Snapshot,
            Endpoint::Security => Kind::Security,
            Endpoint::Unknown => Kind::Other,
        }
    }
}

/// Всё, что узел считает о себе.
pub struct Metrics {
    per_kind: [KindStat; Kind::ALL.len()],
    /// Ответы по разрядам кода: 2xx, 4xx, 5xx. Разряд, а не код: рядов должно
    /// быть столько, сколько решений по ним принимают.
    ok: Counter,
    client_error: Counter,
    server_error: Counter,
    /// Приём: документы и отказы.
    docs_added: Counter,
    docs_deleted: Counter,
    ingest_request_bytes: Counter,
    /// Отказы размыкателя по памяти — их отдельно, потому что это не ошибка
    /// клиента и не поломка узла, а осознанный сброс нагрузки.
    breaker: Counter,
    /// Быстрые отказы ограниченной очереди записи.
    write_queue_refused: Counter,
    /// Сколько раз шард ответил **не** предпочтённой репликой.
    ///
    /// Величина узла, а не запроса: одиночный переход ничего не значит, а
    /// растущий счётчик означает, что копия лежит и никто этого не заметил —
    /// выдача-то приходит.
    fallbacks: Counter,
    /// Слияний и сколько времени на них ушло.
    merges: Counter,
    merge_ms: Hist,
    compaction_input_bytes: Counter,
    compaction_output_bytes: Counter,
    compaction_physical_read_bytes: Counter,
    compaction_physical_write_bytes: Counter,
    compaction_logical_read_bytes: Counter,
    compaction_logical_write_bytes: Counter,
    /// Реальная работа WAL и фоновой публикации, а не время HTTP-ожидания.
    wal_commits: Counter,
    wal_commit_failures: Counter,
    wal_records: Counter,
    wal_commit_ms: Hist,
    wal_batch_records: Hist,
    flush_ms: Hist,
    /// До-WAL стадии HTTP-приёма и кодирование записи журнала.
    ingest_schema_ms: Hist,
    ingest_json_ms: Hist,
    wal_encode_ms: Hist,
    write_id_lock_ms: Hist,
    write_ingest_lock_ms: Hist,
    /// Удержание буфера приёма — доля занятости исключительного участка.
    write_ingest_held_ms: Hist,
    /// Ожидание своей очереди применения.
    write_apply_order_ms: Hist,
    /// Разбор слов пачки вне замка.
    pretokenize_ms: Hist,
    /// Разложение поискового запроса по крупным стадиям. Эти величины нельзя
    /// помещать в JSON ответа: фактическое время недетерминировано и нарушает
    /// побайтовую эквивалентность выдачи после pack/unpack.
    search_core_ms: Hist,
    search_hydrate_ms: Hist,
    search_aggregate_ms: Hist,
    search_source_values: Counter,
    search_source_bytes: Counter,
}

struct KindStat {
    total: Counter,
    latency: Hist,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics::new()
    }
}

impl Metrics {
    pub fn new() -> Metrics {
        Metrics {
            per_kind: std::array::from_fn(|_| KindStat {
                total: Counter::new(),
                latency: Hist::new(),
            }),
            ok: Counter::new(),
            client_error: Counter::new(),
            server_error: Counter::new(),
            docs_added: Counter::new(),
            docs_deleted: Counter::new(),
            ingest_request_bytes: Counter::new(),
            breaker: Counter::new(),
            write_queue_refused: Counter::new(),
            fallbacks: Counter::new(),
            merges: Counter::new(),
            merge_ms: Hist::new(),
            compaction_input_bytes: Counter::new(),
            compaction_output_bytes: Counter::new(),
            compaction_physical_read_bytes: Counter::new(),
            compaction_physical_write_bytes: Counter::new(),
            compaction_logical_read_bytes: Counter::new(),
            compaction_logical_write_bytes: Counter::new(),
            wal_commits: Counter::new(),
            wal_commit_failures: Counter::new(),
            wal_records: Counter::new(),
            wal_commit_ms: Hist::new(),
            wal_batch_records: Hist::new(),
            flush_ms: Hist::new(),
            ingest_schema_ms: Hist::new(),
            ingest_json_ms: Hist::new(),
            wal_encode_ms: Hist::new(),
            write_id_lock_ms: Hist::new(),
            write_ingest_lock_ms: Hist::new(),
            write_ingest_held_ms: Hist::new(),
            write_apply_order_ms: Hist::new(),
            pretokenize_ms: Hist::new(),
            search_core_ms: Hist::new(),
            search_hydrate_ms: Hist::new(),
            search_aggregate_ms: Hist::new(),
            search_source_values: Counter::new(),
            search_source_bytes: Counter::new(),
        }
    }

    /// Записывает завершённый запрос.
    pub fn request(&self, kind: Kind, code: u16, ms: f64) {
        let at = Kind::ALL.iter().position(|k| *k == kind).unwrap_or(Kind::ALL.len() - 1);
        self.per_kind[at].total.add(1);
        self.per_kind[at].latency.observe(ms);
        match code {
            0..=399 => self.ok.add(1),
            400..=499 => self.client_error.add(1),
            _ => self.server_error.add(1),
        }
    }

    pub fn added(&self, n: u64) {
        self.docs_added.add(n);
    }

    pub fn deleted(&self, n: u64) {
        self.docs_deleted.add(n);
    }

    pub fn ingest_bytes(&self, bytes: u64) {
        self.ingest_request_bytes.add(bytes);
    }

    pub fn shard_fallback(&self, n: u64) {
        self.fallbacks.add(n);
    }

    pub fn refused_by_breaker(&self) {
        self.breaker.add(1);
    }

    pub fn refused_by_write_queue(&self) {
        self.write_queue_refused.add(1);
    }

    pub fn wal_commit(&self, records: u64, ms: f64) {
        self.wal_commits.add(1);
        self.wal_records.add(records);
        self.wal_commit_ms.observe(ms);
        self.wal_batch_records.observe(records as f64);
    }

    pub fn wal_commit_failed(&self) {
        self.wal_commit_failures.add(1);
    }

    /// Записывает подготовительные стадии приёма, которые живут выше ядра.
    pub fn ingest_schema(&self, ms: f64) {
        self.ingest_schema_ms.observe(ms);
    }

    pub fn ingest_json(&self, ms: f64) {
        self.ingest_json_ms.observe(ms);
    }

    pub fn wal_encode(&self, ms: f64) {
        self.wal_encode_ms.observe(ms);
    }

    pub fn write_lock(&self, lock: &str, ms: f64) {
        match lock {
            "id_stripes" => self.write_id_lock_ms.observe(ms),
            "ingest" => self.write_ingest_lock_ms.observe(ms),
            "apply_order" => self.write_apply_order_ms.observe(ms),
            _ => {}
        }
    }

    /// Сколько исключительный участок был **занят**, а не сколько его ждали.
    pub fn write_held(&self, lock: &str, ms: f64) {
        if lock == "ingest" {
            self.write_ingest_held_ms.observe(ms);
        }
    }

    /// Разбор слов пачки — вне замка, но не бесплатно.
    pub fn pretokenize(&self, ms: f64) {
        self.pretokenize_ms.observe(ms);
    }

    pub fn flushed(&self, ms: f64) {
        self.flush_ms.observe(ms);
    }

    pub fn merged(&self, ms: f64) {
        self.merges.add(1);
        self.merge_ms.observe(ms);
    }

    pub fn compaction_io(&self, input: u64, output: u64, io: crate::host::Io) {
        self.compaction_input_bytes.add(input);
        self.compaction_output_bytes.add(output);
        self.compaction_physical_read_bytes.add(io.read_bytes.unwrap_or(0));
        self.compaction_physical_write_bytes.add(io.write_bytes.unwrap_or(0));
        self.compaction_logical_read_bytes.add(io.rchar.unwrap_or(0));
        self.compaction_logical_write_bytes.add(io.wchar.unwrap_or(0));
    }

    /// Записывает разложение успешно выполненного поиска.
    pub fn search_stages(
        &self,
        core_ms: f64,
        hydrate_ms: f64,
        aggregate_ms: f64,
        source_values: u64,
        source_bytes: u64,
    ) {
        self.search_core_ms.observe(core_ms);
        self.search_hydrate_ms.observe(hydrate_ms);
        self.search_aggregate_ms.observe(aggregate_ms);
        self.search_source_values.add(source_values);
        self.search_source_bytes.add(source_bytes);
    }

    /// Печатает всё, что накопилось. Состояние индекса приходит снаружи: его
    /// знает узел, а не счётчики.
    pub fn render(&self, state: &State) -> String {
        use svindb_core::metrics::{gauge, header};
        let mut o = String::with_capacity(8 << 10);

        header(&mut o, "svindb_requests_total", "Запросов по видам", "counter");
        for (i, k) in Kind::ALL.iter().enumerate() {
            let n = self.per_kind[i].total.get();
            if n > 0 {
                o.push_str(&format!("svindb_requests_total{{kind=\"{}\"}} {n}\n", k.name()));
            }
        }

        if self.search_core_ms.count() > 0 {
            header(
                &mut o,
                "svindb_search_stage_ms",
                "Время поискового запроса по внутренним стадиям",
                "histogram",
            );
            self.search_core_ms.write(&mut o, "svindb_search_stage_ms", "stage=\"core\"");
            self.search_hydrate_ms.write(&mut o, "svindb_search_stage_ms", "stage=\"hydrate\"");
            self.search_aggregate_ms.write(&mut o, "svindb_search_stage_ms", "stage=\"aggregate\"");
            header(
                &mut o,
                "svindb_search_source_values_total",
                "Значений stored source, прочитанных поиском",
                "counter",
            );
            gauge(&mut o, "svindb_search_source_values_total", self.search_source_values.get());
            header(
                &mut o,
                "svindb_search_source_bytes_total",
                "Байт stored source, прочитанных поиском",
                "counter",
            );
            gauge(&mut o, "svindb_search_source_bytes_total", self.search_source_bytes.get());
        }

        header(&mut o, "svindb_responses_total", "Ответов по разряду кода", "counter");
        for (cls, c) in
            [("2xx", &self.ok), ("4xx", &self.client_error), ("5xx", &self.server_error)]
        {
            o.push_str(&format!("svindb_responses_total{{class=\"{cls}\"}} {}\n", c.get()));
        }

        header(&mut o, "svindb_request_ms", "Время ответа по видам", "histogram");
        for (i, k) in Kind::ALL.iter().enumerate() {
            if self.per_kind[i].total.get() > 0 {
                let labels = format!("kind=\"{}\"", k.name());
                self.per_kind[i].latency.write(&mut o, "svindb_request_ms", &labels);
            }
        }

        header(&mut o, "svindb_docs_added_total", "Принято документов", "counter");
        gauge(&mut o, "svindb_docs_added_total", self.docs_added.get());
        header(&mut o, "svindb_docs_deleted_total", "Погашено документов", "counter");
        gauge(&mut o, "svindb_docs_deleted_total", self.docs_deleted.get());
        header(
            &mut o,
            "svindb_ingest_request_bytes_total",
            "Логических байт успешных ingest-тел",
            "counter",
        );
        gauge(&mut o, "svindb_ingest_request_bytes_total", self.ingest_request_bytes.get());
        header(&mut o, "svindb_breaker_refused_total", "Отказов размыкателя по памяти", "counter");
        gauge(&mut o, "svindb_breaker_refused_total", self.breaker.get());
        header(
            &mut o,
            "svindb_write_queue_refused_total",
            "Быстрых отказов переполненной очереди записи",
            "counter",
        );
        gauge(&mut o, "svindb_write_queue_refused_total", self.write_queue_refused.get());

        header(
            &mut o,
            "svindb_shard_fallbacks_total",
            "Переходов на запасную реплику шарда",
            "counter",
        );
        gauge(&mut o, "svindb_shard_fallbacks_total", self.fallbacks.get());
        // Число берётся у самого приёма, а не у своего счётчика.
        //
        // Свой считал только явные сбросы — по времени, по запросу `/_flush`, из
        // обслуживания, — а сброс по счётчику документов происходит **внутри**
        // `Indexer::add` и мимо сервера. На десяти миллионах это давало три
        // вместо десяти: метрика, недосчитывающая втрое, хуже отсутствующей,
        // потому что по ней принимают решения.
        header(&mut o, "svindb_flushes_total", "Сбросов буфера в сегмент", "counter");
        gauge(&mut o, "svindb_flushes_total", state.flushes);
        header(
            &mut o,
            "svindb_live_twins_total",
            "Живых двойников буфера: собрано и пропущено",
            "counter",
        );
        o.push_str(&format!(
            "svindb_live_twins_total{{outcome=\"built\"}} {}\n",
            state.twins_built
        ));
        o.push_str(&format!(
            "svindb_live_twins_total{{outcome=\"skipped\"}} {}\n",
            state.twins_skipped
        ));
        header(
            &mut o,
            "svindb_segments_created_total",
            "Создано сегментов приёмом за жизнь процесса",
            "counter",
        );
        gauge(&mut o, "svindb_segments_created_total", state.segments_created);
        if self.flush_ms.count() > 0 {
            header(&mut o, "svindb_flush_ms", "Время фоновой публикации сегмента", "histogram");
            self.flush_ms.write(&mut o, "svindb_flush_ms", "");
        }
        header(&mut o, "svindb_merges_total", "Укрупнений сегментов", "counter");
        gauge(&mut o, "svindb_merges_total", self.merges.get());
        if self.merges.get() > 0 {
            header(&mut o, "svindb_merge_ms", "Время укрупнения", "histogram");
            self.merge_ms.write(&mut o, "svindb_merge_ms", "");
        }
        header(
            &mut o,
            "svindb_compaction_io_bytes_total",
            "Байты compaction по логическим входу/выходу и process physical I/O",
            "counter",
        );
        for (kind, direction, value) in [
            ("logical", "read", self.compaction_input_bytes.get()),
            ("logical", "write", self.compaction_output_bytes.get()),
            ("physical", "read", self.compaction_physical_read_bytes.get()),
            ("physical", "write", self.compaction_physical_write_bytes.get()),
            ("syscall", "read", self.compaction_logical_read_bytes.get()),
            ("syscall", "write", self.compaction_logical_write_bytes.get()),
        ] {
            o.push_str(&format!(
                "svindb_compaction_io_bytes_total{{kind=\"{kind}\",direction=\"{direction}\"}} {value}\n"
            ));
        }
        header(
            &mut o,
            "svindb_compaction_physical_amplification",
            "Физические байты compaction на логический байт входа/выхода",
            "gauge",
        );
        let ratio = |physical: u64, logical: u64| {
            if logical == 0 {
                0.0
            } else {
                physical as f64 / logical as f64
            }
        };
        o.push_str(&format!(
            "svindb_compaction_physical_amplification{{direction=\"read\"}} {:.6}\n",
            ratio(self.compaction_physical_read_bytes.get(), self.compaction_input_bytes.get())
        ));
        o.push_str(&format!(
            "svindb_compaction_physical_amplification{{direction=\"write\"}} {:.6}\n",
            ratio(self.compaction_physical_write_bytes.get(), self.compaction_output_bytes.get())
        ));
        header(
            &mut o,
            "svindb_compaction_pending_segments",
            "Лишних сегментов, ожидающих укрупнения",
            "gauge",
        );
        gauge(&mut o, "svindb_compaction_pending_segments", state.compaction_pending);
        header(
            &mut o,
            "svindb_compaction_pending_bytes",
            "Байтов в текущем составе, относимом к compaction debt",
            "gauge",
        );
        gauge(&mut o, "svindb_compaction_pending_bytes", state.compaction_pending_bytes);
        header(
            &mut o,
            "svindb_compaction_completed_sources_total",
            "Сегментов-источников успешно опубликованных compaction jobs",
            "counter",
        );
        gauge(
            &mut o,
            "svindb_compaction_completed_sources_total",
            state.compaction_completed_sources,
        );
        header(
            &mut o,
            "svindb_compaction_throughput_bytes_per_second",
            "Средняя скорость по логическим входным байтам активной compaction",
            "gauge",
        );
        let throughput = if state.compaction_elapsed_micros == 0 {
            0.0
        } else {
            state.compaction_completed_input_bytes as f64 * 1_000_000.0
                / state.compaction_elapsed_micros as f64
        };
        o.push_str(&format!("svindb_compaction_throughput_bytes_per_second {throughput:.3}\n"));
        header(
            &mut o,
            "svindb_compaction_eta_seconds",
            "ETA текущего compaction debt по измеренной активной скорости",
            "gauge",
        );
        let eta =
            if throughput > 0.0 { state.compaction_pending_bytes as f64 / throughput } else { 0.0 };
        if state.compaction_pending_bytes > 0 && throughput > 0.0 {
            o.push_str(&format!("svindb_compaction_eta_seconds {eta:.3}\n"));
        }
        header(
            &mut o,
            "svindb_compaction_eta_known",
            "Есть ли измеренная сходящаяся скорость для ETA compaction",
            "gauge",
        );
        gauge(
            &mut o,
            "svindb_compaction_eta_known",
            u64::from(state.compaction_pending_bytes == 0 || throughput > 0.0),
        );
        header(
            &mut o,
            "svindb_l0_throttled_writes_total",
            "Записей, замедленных мягким пределом долга L0",
            "counter",
        );
        gauge(&mut o, "svindb_l0_throttled_writes_total", state.l0_throttled);
        header(
            &mut o,
            "svindb_l0_rejected_writes_total",
            "Записей, отвергнутых жёстким пределом долга L0",
            "counter",
        );
        gauge(&mut o, "svindb_l0_rejected_writes_total", state.l0_rejected);
        header(
            &mut o,
            "svindb_ingest_watermark",
            "WAL seq по стадиям ack, visible, ready и settled",
            "gauge",
        );
        o.push_str(&format!("svindb_ingest_watermark{{stage=\"ack\"}} {}\n", state.ack_seq));
        o.push_str(&format!(
            "svindb_ingest_watermark{{stage=\"visible\"}} {}\n",
            state.visible_seq
        ));
        o.push_str(&format!("svindb_ingest_watermark{{stage=\"ready\"}} {}\n", state.ready_seq));
        o.push_str(&format!(
            "svindb_ingest_watermark{{stage=\"settled\"}} {}\n",
            state.settled_seq
        ));
        header(
            &mut o,
            "svindb_resource_queue_active",
            "Активных работ по отдельным resource queues",
            "gauge",
        );
        header(
            &mut o,
            "svindb_resource_queue_limit",
            "Предел активных работ resource queue",
            "gauge",
        );
        header(
            &mut o,
            "svindb_resource_queue_rejected_total",
            "Быстрых отказов resource queue",
            "counter",
        );
        for (name, active, limit, rejected, waiters, waited_ms) in &state.resource_queues {
            o.push_str(&format!(
                "svindb_resource_queue_active{{queue=\"{name}\"}} {active}\n\
                 svindb_resource_queue_limit{{queue=\"{name}\"}} {limit}\n\
                 svindb_resource_queue_waiter_limit{{queue=\"{name}\"}} {}\n\
                 svindb_resource_queue_rejected_total{{queue=\"{name}\"}} {rejected}\n\
                 svindb_resource_queue_waiters{{queue=\"{name}\"}} {waiters}\n\
                 svindb_resource_queue_wait_ms_total{{queue=\"{name}\"}} {waited_ms}\n",
                waiter_limit(name),
            ));
        }
        header(
            &mut o,
            "svindb_search_cpu_units_used",
            "Зарезервированные CPU units поиска",
            "gauge",
        );
        gauge(&mut o, "svindb_search_cpu_units_used", state.search_cpu_units_used);
        header(&mut o, "svindb_search_cpu_units_limit", "Общий предел CPU units поиска", "gauge");
        gauge(&mut o, "svindb_search_cpu_units_limit", state.search_cpu_units_limit);
        header(
            &mut o,
            "svindb_workload_cpu_units_limit",
            "Эффективный общий бюджет WorkloadGovernor",
            "gauge",
        );
        gauge(&mut o, "svindb_workload_cpu_units_limit", state.search_cpu_units_limit);
        header(
            &mut o,
            "svindb_workload_interactive_units_limit",
            "Максимум CPU units одного interactive запроса",
            "gauge",
        );
        gauge(&mut o, "svindb_workload_interactive_units_limit", state.interactive_max_units);
        header(
            &mut o,
            "svindb_workload_analytics_units_limit",
            "Максимум CPU units одного analytics запроса",
            "gauge",
        );
        gauge(&mut o, "svindb_workload_analytics_units_limit", state.analytics_max_units);
        header(
            &mut o,
            "svindb_workload_io_units_used",
            "Зарезервированные логические I/O units governor",
            "gauge",
        );
        gauge(&mut o, "svindb_workload_io_units_used", state.io_units_used);
        header(
            &mut o,
            "svindb_workload_io_units_limit",
            "Предел логических I/O units governor",
            "gauge",
        );
        gauge(&mut o, "svindb_workload_io_units_limit", state.io_units_limit);
        header(
            &mut o,
            "svindb_workload_latency_reserve_bytes",
            "Резерв latency admission",
            "gauge",
        );
        gauge(&mut o, "svindb_workload_latency_reserve_bytes", state.latency_reserve_bytes);
        header(
            &mut o,
            "svindb_workload_wal_replay_bytes",
            "Предел WAL tail до физического flush",
            "gauge",
        );
        gauge(&mut o, "svindb_workload_wal_replay_bytes", state.wal_replay_bytes);
        header(
            &mut o,
            "svindb_workload_frozen_generations_limit",
            "Предел frozen поколений до coalescing",
            "gauge",
        );
        gauge(&mut o, "svindb_workload_frozen_generations_limit", state.max_frozen_memtables);
        header(
            &mut o,
            "svindb_workload_background_io_bytes_per_second",
            "Предел полосы фонового I/O",
            "gauge",
        );
        gauge(
            &mut o,
            "svindb_workload_background_io_bytes_per_second",
            state.background_io_bytes_per_second,
        );
        header(&mut o, "svindb_workload_background_io_burst_bytes", "Burst фонового I/O", "gauge");
        gauge(&mut o, "svindb_workload_background_io_burst_bytes", state.background_io_burst_bytes);
        header(
            &mut o,
            "svindb_workload_background_io_inflight_limit",
            "Предел фоновых I/O операций в полёте",
            "gauge",
        );
        gauge(
            &mut o,
            "svindb_workload_background_io_inflight_limit",
            state.background_io_max_inflight,
        );
        header(
            &mut o,
            "svindb_source_cache_capacity_bytes",
            "Предел распакованного source cache",
            "gauge",
        );
        gauge(&mut o, "svindb_source_cache_capacity_bytes", state.source_cache_capacity_bytes);
        header(
            &mut o,
            "svindb_source_cache_bytes",
            "Текущий объём распакованного source cache",
            "gauge",
        );
        gauge(&mut o, "svindb_source_cache_bytes", state.source_cache_bytes);
        header(
            &mut o,
            "svindb_source_cache_entries",
            "Число записей распакованного source cache",
            "gauge",
        );
        gauge(&mut o, "svindb_source_cache_entries", state.source_cache_entries);
        header(
            &mut o,
            "svindb_source_cache_hits_total",
            "Попадания распакованного source cache",
            "counter",
        );
        gauge(&mut o, "svindb_source_cache_hits_total", state.source_cache_hits);
        header(
            &mut o,
            "svindb_source_cache_misses_total",
            "Промахи распакованного source cache",
            "counter",
        );
        gauge(&mut o, "svindb_source_cache_misses_total", state.source_cache_misses);
        header(
            &mut o,
            "svindb_source_cache_evictions_total",
            "Вытеснения распакованного source cache",
            "counter",
        );
        gauge(&mut o, "svindb_source_cache_evictions_total", state.source_cache_evictions);
        if let Some(seconds) = state.oldest_query_seconds {
            header(
                &mut o,
                "svindb_oldest_active_query_seconds",
                "Возраст самого старого активного поиска",
                "gauge",
            );
            o.push_str(&format!("svindb_oldest_active_query_seconds {seconds:.3}\n"));
        }
        header(
            &mut o,
            "svindb_compaction_running_segments",
            "Сегментов в текущем укрупнении",
            "gauge",
        );
        gauge(&mut o, "svindb_compaction_running_segments", state.compaction_running);
        header(
            &mut o,
            "svindb_compaction_jobs_total",
            "Завершённые и неуспешные compaction jobs для этого индекса",
            "counter",
        );
        gauge(&mut o, "svindb_compaction_jobs_total", state.compaction_jobs);
        header(
            &mut o,
            "svindb_compaction_active_ms_total",
            "Активное wall time compaction jobs",
            "counter",
        );
        gauge(&mut o, "svindb_compaction_active_ms_total", state.compaction_active_ms);
        header(
            &mut o,
            "svindb_compaction_paused_ms_total",
            "Время cooperative пауз compaction jobs",
            "counter",
        );
        gauge(&mut o, "svindb_compaction_paused_ms_total", state.compaction_paused_ms);
        header(
            &mut o,
            "svindb_compaction_query_preemptions_total",
            "Переходов compaction в паузу из-за спроса поиска",
            "counter",
        );
        gauge(
            &mut o,
            "svindb_compaction_query_preemptions_total",
            state.compaction_query_preemptions,
        );
        header(
            &mut o,
            "svindb_compaction_checkpoints_total",
            "Cooperative checkpoints compaction",
            "counter",
        );
        gauge(&mut o, "svindb_compaction_checkpoints_total", state.compaction_checkpoints);
        header(
            &mut o,
            "svindb_compaction_paced_bytes_total",
            "Логические байты, пропущенные через I/O pacing",
            "counter",
        );
        gauge(&mut o, "svindb_compaction_paced_bytes_total", state.compaction_paced_bytes);
        header(
            &mut o,
            "svindb_compaction_paced_wait_us_total",
            "Время ожидания compaction I/O pacing",
            "counter",
        );
        gauge(&mut o, "svindb_compaction_paced_wait_us_total", state.compaction_paced_waited_us);

        header(&mut o, "svindb_wal_commits_total", "Физических fsync журнала", "counter");
        gauge(&mut o, "svindb_wal_commits_total", self.wal_commits.get());
        header(
            &mut o,
            "svindb_wal_commit_failures_total",
            "Отказов физической фиксации журнала",
            "counter",
        );
        gauge(&mut o, "svindb_wal_commit_failures_total", self.wal_commit_failures.get());
        header(&mut o, "svindb_wal_records_total", "Записей, накрытых fsync журнала", "counter");
        gauge(&mut o, "svindb_wal_records_total", self.wal_records.get());
        if self.wal_commits.get() > 0 {
            header(&mut o, "svindb_wal_commit_ms", "Время durable commit журнала", "histogram");
            self.wal_commit_ms.write(&mut o, "svindb_wal_commit_ms", "");
            header(
                &mut o,
                "svindb_wal_batch_records",
                "Число записей на один fsync журнала",
                "histogram",
            );
            self.wal_batch_records.write(&mut o, "svindb_wal_batch_records", "");
        }
        if svindb_core::stats::ENABLED && self.ingest_schema_ms.count() > 0 {
            header(
                &mut o,
                "svindb_ingest_frontend_ms",
                "Время подготовительных стадий HTTP-приёма",
                "histogram",
            );
            self.ingest_schema_ms.write(&mut o, "svindb_ingest_frontend_ms", "stage=\"schema\"");
            self.ingest_json_ms.write(&mut o, "svindb_ingest_frontend_ms", "stage=\"json\"");
            self.wal_encode_ms.write(&mut o, "svindb_ingest_frontend_ms", "stage=\"wal_encode\"");
            header(
                &mut o,
                "svindb_write_lock_wait_ms",
                "Ожидание внутренних замков на пути записи",
                "histogram",
            );
            self.write_id_lock_ms.write(&mut o, "svindb_write_lock_wait_ms", "lock=\"id_stripes\"");
            self.write_ingest_lock_ms.write(&mut o, "svindb_write_lock_wait_ms", "lock=\"ingest\"");
            self.write_apply_order_ms.write(
                &mut o,
                "svindb_write_lock_wait_ms",
                "lock=\"apply_order\"",
            );
            self.write_ingest_held_ms.write(&mut o, "svindb_write_lock_held_ms", "lock=\"ingest\"");
            self.pretokenize_ms.write(&mut o, "svindb_ingest_frontend_ms", "stage=\"pretokenize\"");
        }

        header(&mut o, "svindb_searches_inflight", "Поисков прямо сейчас", "gauge");
        gauge(&mut o, "svindb_searches_inflight", state.inflight);

        header(&mut o, "svindb_docs", "Документов в индексе", "gauge");
        gauge(&mut o, "svindb_docs", state.docs);
        header(&mut o, "svindb_docs_dead", "Погашенных, но занимающих место", "gauge");
        gauge(&mut o, "svindb_docs_dead", state.dead);
        header(&mut o, "svindb_tombstone_ratio", "Доля погашенных физических версий", "gauge");
        // `Segments::docs()` уже включает и живые, и погашенные номера. Ещё
        // раз прибавить `dead` означало бы занизить долю: при 90 погашенных из
        // 100 метрика показывала бы 47%, хотя на очистку претендуют 90%.
        // Доля считается по диску, а не по всему, что видно поиску.
        //
        // Смысл этой метрики — «сколько места вернёт уборка», и живому сегменту
        // в ней делать нечего: убирать его никто не будет, а места он не
        // занимает. Под потоком замен он к тому же наполовину состоит из
        // погашенных версий, и, попади он в счёт, метрика показывала бы
        // накопленный долг там, где долга нет вовсе.
        let ratio = if state.disk_docs == 0 {
            0.0
        } else {
            (state.disk_dead as f64 / state.disk_docs as f64).clamp(0.0, 1.0)
        };
        o.push_str(&format!("svindb_tombstone_ratio {ratio:.6}\n"));
        header(&mut o, "svindb_segments", "Сегментов", "gauge");
        gauge(&mut o, "svindb_segments", state.segments);
        header(&mut o, "svindb_frozen_generations", "Immutable frozen generations", "gauge");
        gauge(&mut o, "svindb_frozen_generations", state.frozen_generations);
        header(
            &mut o,
            "svindb_frozen_memory_bytes",
            "Полезные байты frozen memory sources",
            "gauge",
        );
        gauge(&mut o, "svindb_frozen_memory_bytes", state.frozen_memory_bytes);
        header(&mut o, "svindb_buffered_docs", "В буфере приёма", "gauge");
        gauge(&mut o, "svindb_buffered_docs", state.buffered);
        // Отдельно от `buffered_docs`, а не вместо: первое говорит, сколько
        // принято сверх опубликованного, второе — сколько из этого **видно
        // поиску**. Разница между ними и есть то, чего поиск сейчас не знает.
        // Ноль при непустом буфере — живой сегмент выключен или буфер перерос
        // `live_max_docs`; тогда свежесть снова стоит сброса на диск.
        header(&mut o, "svindb_live_docs", "В живом сегменте, видном поиску", "gauge");
        gauge(&mut o, "svindb_live_docs", state.live_docs);
        header(&mut o, "svindb_write_queue_pending", "Запросов внутри очереди записи", "gauge");
        gauge(&mut o, "svindb_write_queue_pending", state.write_queue_pending);
        header(
            &mut o,
            "svindb_write_queue_highwater",
            "Наибольшая занятость очереди записи",
            "gauge",
        );
        gauge(&mut o, "svindb_write_queue_highwater", state.write_queue_highwater);
        header(&mut o, "svindb_mmaps", "Областей отображения", "gauge");
        gauge(&mut o, "svindb_mmaps", state.mmaps);
        if let Some(limit) = state.mmaps_limit {
            header(&mut o, "svindb_mmaps_limit", "Предел областей отображения", "gauge");
            gauge(&mut o, "svindb_mmaps_limit", limit);
        }
        header(&mut o, "svindb_wal_seq", "Номер последней записи журнала", "gauge");
        gauge(&mut o, "svindb_wal_seq", state.wal_seq);
        header(&mut o, "svindb_wal_checkpoint", "Отметка журнала", "gauge");
        gauge(&mut o, "svindb_wal_checkpoint", state.wal_checkpoint);
        header(&mut o, "svindb_reload_failures", "Неудачных перечитываний состава", "gauge");
        gauge(&mut o, "svindb_reload_failures", state.reload_failures);
        // Единица значит «принимает», ноль — «приём остановлен». Это то, по чему
        // ставят тревогу: узел отвечает на поиск и при отказе записи, и по
        // живости процесса такого не отличить.
        header(&mut o, "svindb_ingest_healthy", "Принимает ли узел записи", "gauge");
        gauge(&mut o, "svindb_ingest_healthy", u64::from(state.ingest_healthy));

        #[cfg(feature = "alloc-stats")]
        {
            let (allocations, bytes) = svindb_core::alloc::totals();
            header(
                &mut o,
                "svindb_allocations_total",
                "Вызовов распределителя в профилировочной сборке",
                "counter",
            );
            gauge(&mut o, "svindb_allocations_total", allocations);
            header(
                &mut o,
                "svindb_allocated_bytes_total",
                "Запрошено байт у распределителя в профилировочной сборке",
                "counter",
            );
            gauge(&mut o, "svindb_allocated_bytes_total", bytes);
            // Отношение «оценка к измеренному» у последнего слияния.
            //
            // Ворота выпуска смотрят на его девяносто девятый процентиль:
            // единица снизу — это граница между «оценка мажоранта» и «узел
            // будет снят ядром посреди работы». Метрика не для красоты — она
            // единственное, что делает обещание проверяемым на боевых
            // сегментах, а не на игрушечных корпусах теста.
            if let Some(ratio) = svindb_core::maintain::estimate_ratio() {
                header(
                    &mut o,
                    "svindb_permit_estimate_ratio",
                    "Оценка расхода слияния к измеренному пику; ниже единицы — не мажоранта",
                    "gauge",
                );
                o.push_str(&format!(
                    "svindb_permit_estimate_ratio{{class=\"merge\"}} {ratio:.6}\n"
                ));
            }
        }

        if svindb_core::stats::ENABLED {
            header(
                &mut o,
                "svindb_ingest_stage_seconds_total",
                "Накопленное процессорное время стадий приёма",
                "counter",
            );
            for &(stage, seconds) in state.ingest_phases.iter() {
                let stable = match stage {
                    "разбор" => "document",
                    "токенизация" => "tokenize",
                    "словарь и постинги" => "dictionary_postings_build",
                    "списки" => "postings_encode",
                    "идентификаторы и нормы" => "ids_norms",
                    "колонки" => "columns",
                    "векторы" => "vectors",
                    "содержимое" => "stored_source_compress",
                    "заголовок и суммы" => "metadata_checksums",
                    other => other,
                };
                o.push_str(&format!(
                    "svindb_ingest_stage_seconds_total{{stage=\"{stable}\"}} {seconds:.6}\n"
                ));
            }
            header(
                &mut o,
                "svindb_ingest_documents_profiled_total",
                "Документов, учтённых профилем стадий приёма",
                "counter",
            );
            gauge(&mut o, "svindb_ingest_documents_profiled_total", state.ingest_total_docs);
            header(
                &mut o,
                "svindb_ingest_tokens_profiled_total",
                "Токенов до схлопывания повторов, учтённых профилем приёма",
                "counter",
            );
            gauge(&mut o, "svindb_ingest_tokens_profiled_total", state.ingest_total_tokens);
            header(
                &mut o,
                "svindb_ingest_postings_profiled_total",
                "Постингов, учтённых профилем стадий приёма",
                "counter",
            );
            gauge(&mut o, "svindb_ingest_postings_profiled_total", state.ingest_total_postings);
            header(
                &mut o,
                "svindb_ingest_buffer_bytes",
                "Оценка памяти активного буфера приёма",
                "gauge",
            );
            gauge(&mut o, "svindb_ingest_buffer_bytes", state.ingest_buffer_bytes);
        }

        self.render_host(&mut o, state);
        o
    }

    /// Расход самого процесса и место на диске.
    ///
    /// Отдельным методом, а не строками в `render`: у этих величин другой
    /// источник — не счётчики узла, а ядро системы, — и часть из них система
    /// может не сообщить. Метрика, которой нет, **не печатается вовсе**: строка
    /// `svindb_disk_free_bytes 0` означала бы «места нет», а не «не спросили».
    fn render_host(&self, o: &mut String, state: &State) {
        use svindb_core::metrics::{gauge, header};

        // Версия — меткой на постоянной единице: так это делает всякий
        // экспортёр, и так её видно в наборе из узлов разных выпусков.
        header(o, "svindb_build_info", "Версия сборки", "gauge");
        o.push_str(&format!(
            "svindb_build_info{{version=\"{}\",revision=\"{}\",target=\"{}\",source_date_epoch=\"{}\"}} 1\n",
            crate::host::version(),
            crate::host::revision(),
            crate::host::target(),
            crate::host::build_epoch(),
        ));

        header(o, "svindb_uptime_seconds", "Сколько узел работает", "gauge");
        o.push_str(&format!("svindb_uptime_seconds {:.3}\n", crate::host::uptime_seconds()));

        if let Some(sample) = crate::pressure::latest() {
            header(o, "svindb_pressure_sample_age_seconds", "Возраст cgroup снимка", "gauge");
            o.push_str(&format!(
                "svindb_pressure_sample_age_seconds {:.6}\n",
                sample.at.elapsed().as_secs_f64()
            ));
            for (name, value) in [
                ("svindb_memory_full_short_percent", sample.delta.memory_full_percent),
                ("svindb_io_full_short_percent", sample.delta.io_full_percent),
            ] {
                if let Some(value) = value {
                    header(o, name, "Доля full stall в последнем измеренном окне", "gauge");
                    o.push_str(&format!("{name} {value:.6}\n"));
                }
            }
            for (name, value) in [
                ("svindb_memory_max_short_delta", sample.delta.events_max),
                ("svindb_direct_reclaim_short_delta", sample.delta.direct_reclaim),
                ("svindb_file_refault_short_delta", sample.delta.file_refaults),
            ] {
                if let Some(value) = value {
                    header(o, name, "Прирост счётчика за последнее измеренное окно", "gauge");
                    gauge(o, name, value);
                }
            }
        }

        let u = crate::host::usage();
        if let Some(rss) = u.resident_bytes {
            header(o, "svindb_process_resident_bytes", "Занятая физическая память", "gauge");
            gauge(o, "svindb_process_resident_bytes", rss);
        }
        if let Some(vsz) = u.virtual_bytes {
            header(o, "svindb_process_virtual_bytes", "Отведённое адресное пространство", "gauge");
            gauge(o, "svindb_process_virtual_bytes", vsz);
        }
        if let Some(cpu) = u.cpu_seconds {
            header(o, "svindb_process_cpu_seconds_total", "Время процессора", "counter");
            o.push_str(&format!("svindb_process_cpu_seconds_total {cpu:.3}\n"));
        }
        if let Some(threads) = u.threads {
            header(o, "svindb_process_threads", "Потоков у процесса", "gauge");
            gauge(o, "svindb_process_threads", threads);
        }
        if let Some(bytes) = state.index_bytes {
            header(o, "svindb_index_bytes", "Занято каталогом индекса", "gauge");
            gauge(o, "svindb_index_bytes", bytes);
        }
        if let Some(free) = state.disk_free_bytes {
            header(o, "svindb_disk_free_bytes", "Свободно на томе с индексом", "gauge");
            gauge(o, "svindb_disk_free_bytes", free);
        }
        for (name, help, value) in [
            (
                "svindb_cgroup_memory_current_bytes",
                "Текущий расход memory cgroup",
                state.memory_current,
            ),
            ("svindb_cgroup_memory_limit_bytes", "Предел memory cgroup", state.memory_limit),
            ("svindb_cgroup_memory_anon_bytes", "Anonymous memory в cgroup", state.memory_anon),
            ("svindb_cgroup_memory_file_bytes", "Page cache в cgroup", state.memory_file_cache),
            ("svindb_cgroup_memory_slab_bytes", "Kernel slab в cgroup", state.memory_slab),
            (
                "svindb_cgroup_memory_events_max_total",
                "Срабатываний memory.max",
                state.memory_events_max,
            ),
        ] {
            if let Some(value) = value {
                header(o, name, help, "gauge");
                gauge(o, name, value);
            }
        }
        if let Some(value) = state.psi_some_avg10 {
            header(o, "svindb_memory_psi_some_avg10", "Memory PSI some avg10, проценты", "gauge");
            o.push_str(&format!("svindb_memory_psi_some_avg10 {value:.3}\n"));
        }
        if let Some(value) = state.psi_full_avg60 {
            header(o, "svindb_memory_psi_full_avg60", "Memory PSI full avg60, проценты", "gauge");
            o.push_str(&format!("svindb_memory_psi_full_avg60 {value:.3}\n"));
        }
        header(
            o,
            "svindb_memory_internal_floor_bytes",
            "Нижняя граница удерживаемых frozen/PIT объектов; не складывается с cgroup current",
            "gauge",
        );
        gauge(o, "svindb_memory_internal_floor_bytes", state.internal_memory_floor);
        header(
            o,
            "svindb_id_directory_ready",
            "Готов ли быстрый generation-aware ID directory",
            "gauge",
        );
        gauge(o, "svindb_id_directory_ready", u64::from(state.id_directory_ready));
        header(
            o,
            "svindb_process_io_bytes_total",
            "Process I/O с начала запуска: physical и syscall bytes",
            "counter",
        );
        for (kind, direction, value) in [
            ("physical", "read", state.process_read_bytes),
            ("physical", "write", state.process_write_bytes),
            ("syscall", "read", state.process_rchar),
            ("syscall", "write", state.process_wchar),
        ] {
            if let Some(value) = value {
                o.push_str(&format!(
                    "svindb_process_io_bytes_total{{kind=\"{kind}\",direction=\"{direction}\"}} {value}\n"
                ));
            }
        }
        header(
            o,
            "svindb_process_write_amplification",
            "Физические write bytes процесса на байт успешных ingest-тел",
            "gauge",
        );
        o.push_str(&format!(
            "svindb_process_write_amplification {:.6}\n",
            if self.ingest_request_bytes.get() == 0 {
                0.0
            } else {
                state.process_write_bytes.unwrap_or(0) as f64
                    / self.ingest_request_bytes.get() as f64
            }
        ));
    }
}

fn waiter_limit(queue: &str) -> u64 {
    match queue {
        "control" => 32,
        "point_read" => 128,
        "interactive_search" => 64,
        "analytics" => 16,
        "maintenance_scan" => 8,
        "write" => 64,
        "compaction" => 1,
        _ => 0,
    }
}

/// Состояние индекса на момент запроса метрик.
#[derive(Default)]
pub struct State {
    /// Документы живого сегмента: принятые, ещё не легшие на диск и уже видные
    /// поиску. Ноль — буфер приёма поиску не виден.
    pub live_docs: u64,
    /// Документы и погашенные версии **на диске** — без живого сегмента.
    /// По ним считается доля tombstone: она про работу уборки, а живой сегмент
    /// уборке не подлежит.
    pub disk_docs: u64,
    pub disk_dead: u64,
    pub docs: u64,
    pub dead: u64,
    pub segments: u64,
    /// Число immutable memory generations, включая inflight flush. Это не
    /// прибавляется к `segments`: последний остаётся логическим tier-счётом.
    pub frozen_generations: u64,
    pub frozen_memory_bytes: u64,
    pub buffered: u64,
    pub mmaps: u64,
    pub mmaps_limit: Option<u64>,
    pub wal_seq: u64,
    pub wal_checkpoint: u64,
    pub reload_failures: u64,
    pub ingest_healthy: bool,
    /// Сколько сегментов приём записал за жизнь процесса — он же счёт сбросов.
    pub flushes: u64,
    /// Живых двойников собрано и пропущено.
    ///
    /// Двойник собирается под замком приёма, и вопрос «сколько это стоит»
    /// решается этой парой, а не рассуждением: на боевой настройке, где порог
    /// сегмента в сотни раз больше предела видимого буфера, он пропускается
    /// всегда — и стоит ноль.
    pub twins_built: u64,
    pub twins_skipped: u64,
    /// Сколько сегментов приём создал после старта этого процесса.
    pub segments_created: u64,
    pub write_queue_pending: u64,
    pub write_queue_highwater: u64,
    pub compaction_pending: u64,
    pub compaction_pending_bytes: u64,
    pub compaction_completed_sources: u64,
    pub compaction_completed_input_bytes: u64,
    pub compaction_elapsed_micros: u64,
    pub compaction_running: u64,
    pub l0_throttled: u64,
    pub l0_rejected: u64,
    pub ack_seq: u64,
    pub visible_seq: u64,
    pub ready_seq: u64,
    pub settled_seq: u64,
    pub memory_current: Option<u64>,
    pub memory_limit: Option<u64>,
    pub memory_anon: Option<u64>,
    pub memory_file_cache: Option<u64>,
    pub memory_slab: Option<u64>,
    pub memory_events_max: Option<u64>,
    pub psi_some_avg10: Option<f64>,
    pub psi_full_avg60: Option<f64>,
    pub process_read_bytes: Option<u64>,
    pub process_write_bytes: Option<u64>,
    pub process_rchar: Option<u64>,
    pub process_wchar: Option<u64>,
    /// (имя, active, limit, rejected, ожидающие, суммарное ожидание в мс).
    ///
    /// Очередь — часть latency, а не только число отказов. Отдельные gauges и
    /// counter позволяют отличить полезный bounded backpressure от скрытого
    /// зависания до внешнего таймаута.
    pub resource_queues: Vec<(&'static str, u64, u64, u64, u64, u64)>,
    pub search_cpu_units_used: u64,
    pub search_cpu_units_limit: u64,
    pub interactive_max_units: u64,
    pub analytics_max_units: u64,
    pub io_units_used: u64,
    pub io_units_limit: u64,
    pub latency_reserve_bytes: u64,
    pub wal_replay_bytes: u64,
    pub max_frozen_memtables: u64,
    pub background_io_bytes_per_second: u64,
    pub background_io_burst_bytes: u64,
    pub background_io_max_inflight: u64,
    pub source_cache_capacity_bytes: u64,
    pub source_cache_bytes: u64,
    pub source_cache_entries: u64,
    pub source_cache_hits: u64,
    pub source_cache_misses: u64,
    pub source_cache_evictions: u64,
    pub oldest_query_seconds: Option<f64>,
    pub id_directory_ready: bool,
    pub internal_memory_floor: u64,
    pub compaction_jobs: u64,
    pub compaction_active_ms: u64,
    pub compaction_paused_ms: u64,
    pub compaction_query_preemptions: u64,
    pub compaction_checkpoints: u64,
    pub compaction_paced_bytes: u64,
    pub compaction_paced_waited_us: u64,
    /// Снимок диагностического профиля индексатора. Заполняется одним
    /// захватом его замка, чтобы запрос метрик не видел стадии разных эпох.
    pub ingest_phases: Vec<(&'static str, f64)>,
    pub ingest_total_docs: u64,
    pub ingest_total_tokens: u64,
    pub ingest_total_postings: u64,
    pub ingest_buffer_bytes: u64,
    /// Сколько поисков идёт прямо сейчас.
    ///
    /// Берётся у **того самого** счётчика, которым узел ограничивает
    /// одновременность (`Inflight`), а не у своего. Свой пришлось бы двигать из
    /// тех же мест, и первый же ранний выход развёл бы два числа — причём
    /// разошедшийся счётчик заметить нечем, он просто врёт.
    pub inflight: u64,
    /// Сколько байт занимает каталог индекса. `None` — каталог не читается.
    pub index_bytes: Option<u64>,
    /// Сколько байт свободно на томе с индексом. `None` — система не сообщает
    /// (macOS) либо путь не спросился.
    pub disk_free_bytes: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kind(method: &str, target: &str, coordinator: bool) -> Kind {
        Kind::of(&crate::route::Route::parse(method, target), coordinator)
    }

    /// Вид определяется по разобранной цели, а координатор отличается от узла.
    #[test]
    fn the_kind_is_read_from_the_head() {
        assert_eq!(kind("POST", "/", false), Kind::Search);
        assert_eq!(kind("POST", "/", true), Kind::Fanout);
        assert_eq!(kind("POST", "/_bulk", false), Kind::Bulk);
        assert_eq!(kind("GET", "/_health", false), Kind::Health);
        assert_eq!(kind("GET", "/_snapshot/ночной", false), Kind::Snapshot);
        assert_eq!(kind("DELETE", "/_security/api_key/x", false), Kind::Security);
        assert_eq!(kind("GET", "/что-то", false), Kind::Other);
    }

    /// Строка запроса не сбивает учёт.
    ///
    /// Прежде здесь стояло своё, третье по счёту сопоставление пути, и оно тоже
    /// сравнивало точно: `POST /_bulk?refresh=true` считался «прочим». То есть
    /// метрика молчала ровно про тот запрос, на котором расходились и права, и
    /// маршрут.
    #[test]
    fn a_query_string_does_not_blur_the_kind() {
        assert_eq!(kind("POST", "/_bulk?refresh=true", false), Kind::Bulk);
        assert_eq!(kind("POST", "/индекс/_bulk", false), Kind::Bulk);
        assert_eq!(kind("GET", "/_health?pretty", false), Kind::Health);
    }

    /// Выдача разбирается сборщиком: у каждого ряда есть заголовок TYPE, и ни
    /// один ряд не появляется дважды.
    ///
    /// Повтор имени с разным TYPE Prometheus считает ошибкой и выбрасывает всю
    /// выдачу — то есть один неаккуратный ряд гасит наблюдаемость целиком.
    #[test]
    fn the_output_is_well_formed() {
        let m = Metrics::new();
        m.request(Kind::Search, 200, 1.5);
        m.request(Kind::Search, 500, 40.0);
        m.request(Kind::Bulk, 200, 3.0);
        m.added(100);
        m.merged(120.0);
        let out = m.render(&State {
            docs: 100,
            dead: 90,
            // Доля tombstone считается по диску: живой сегмент уборке не
            // подлежит, и его погашенные версии в долг не входят. Здесь всё
            // лежит на диске, поэтому числа те же.
            disk_docs: 100,
            disk_dead: 90,
            segments: 2,
            ingest_healthy: true,
            ..State::default()
        });

        let mut declared: Vec<&str> = Vec::new();
        for line in out.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let name = rest.split(' ').next().expect("имя");
                assert!(!declared.contains(&name), "ряд {name} объявлен дважды:\n{out}");
                declared.push(name);
            }
        }
        for line in out.lines().filter(|l| !l.starts_with('#') && !l.is_empty()) {
            let name = line.split(['{', ' ']).next().expect("имя");
            let base = name
                .trim_end_matches("_bucket")
                .trim_end_matches("_sum")
                .trim_end_matches("_count");
            assert!(
                declared.contains(&base) || declared.contains(&name),
                "ряд {name} без заголовка TYPE:\n{out}"
            );
        }
        assert!(out.contains("svindb_requests_total{kind=\"search\"} 2"), "{out}");
        assert!(out.contains("svindb_responses_total{class=\"5xx\"} 1"), "{out}");
        assert!(out.contains("svindb_docs_added_total 100"), "{out}");
        assert!(out.contains("svindb_tombstone_ratio 0.900000"), "{out}");
        // Живой сегмент в долю не входит: те же сто документов при пустом диске
        // дают ноль, а не девять десятых.
        let live_only = m.render(&State {
            docs: 100,
            dead: 90,
            live_docs: 100,
            segments: 1,
            ingest_healthy: true,
            ..State::default()
        });
        assert!(
            live_only.contains("svindb_tombstone_ratio 0.000000"),
            "живой сегмент посчитан долгом уборки:\n{live_only}"
        );
        assert!(live_only.contains("svindb_live_docs 100"), "{live_only}");
        assert!(out.contains("svindb_ingest_healthy 1"), "{out}");
        // Вид, которого не было, не печатается вовсе: пустые ряды засоряют
        // выдачу и мешают отличить «не было» от «не считаем».
        assert!(!out.contains("kind=\"knn\""), "{out}");
    }
}
