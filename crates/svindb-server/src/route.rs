//! Разбор цели запроса — один раз на запрос.
//!
//! ## Почему отдельным типом, а не сравнением строк на месте
//!
//! Цель запроса читали в четырёх местах и **тремя разными способами**: права —
//! суффиксом (`auth::needed`), выдача — точным сравнением (`serve`), учёт —
//! своим сопоставлением (`metrics::Kind::of`), снимки и права — префиксом. Пока
//! способы расходились, расходились и решения, принятые по одной и той же
//! строке.
//!
//! Вот что из этого выходило на самом обычном запросе. `POST /_bulk?refresh=true`
//! — так пишет всякий клиент Elasticsearch:
//!
//! * права: суффикс `/_bulk` не совпал, потому что строка запроса — часть цели
//!   (`http.rs`, `parts[1]` берётся целиком), значит запрос получал право
//!   **читателя**;
//! * выдача: точное сравнение тоже не совпало, значит запрос уходил в **поиск**;
//! * учёт: считался как «прочее».
//!
//! То есть читатель получал доступ к пути записи, а приём молча превращался в
//! поиск и отвечал двумястами. Ни одна из трёх ошибок не видна ни в логе, ни в
//! ответе.
//!
//! Отсюда правило, ради которого заведён этот модуль: **цель разбирается один
//! раз, и все читают одно значение**. Строка запроса в сравнении путей не
//! участвует никогда.
//!
//! ## Почему перечисление, а не строка
//!
//! `auth::needed` сопоставляет [`Endpoint`] **исчерпывающе, без ветви `_`**.
//! Это не украшение: новая точка маршрутизации не соберётся, пока для неё не
//! написано право. Прежнее умолчание «незнакомому — чтение» осталось, но стало
//! явной ветвью [`Endpoint::Unknown`], а не молчаливым концом функции.

/// Точка маршрутизации.
///
/// Перечисляет то, что узел умеет **сегодня**. Точки, которых нет
/// (`/_doc`, `/_update` — они уже упомянуты в правах, но не обслуживаются
/// ничем), сюда не входят: право на несуществующую точку — это обещание,
/// которого никто не держит.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Endpoint {
    /// `PUT|GET|HEAD|DELETE /{индекс}` — жизненный цикл независимого индекса
    /// в многoиндексном режиме.
    Index,
    /// `GET /_cat/indices` — каталог индексов процесса.
    CatIndices,
    /// `GET /_cat/aliases` — каталог aliases процесса.
    CatAliases,
    /// Корневой `POST /_aliases` и совместимые `/_alias/...`.
    Alias,
    /// Описание и теги физического индекса.
    Metadata,
    /// Набор lifecycle-блоков физического индекса.
    Blocks,
    /// Настоящее открытие/закрытие runtime.
    Open,
    Close,
    /// Атомарная ротация write alias.
    Rollover,
    /// `GET /_health` — живость узла.
    Health,
    /// `GET /_metrics` — счётчики.
    Metrics,
    /// `GET /_schema` (он же `/_mapping`) — объявленная схема.
    Schema,
    /// `GET /_stats` — чем узел является: версия, режим, место, время работы.
    ///
    /// Отдельно от [`Endpoint::Health`], а не полями в нём: `/_health`
    /// спрашивает балансировщик много раз в секунду, и класть в его ответ
    /// обход каталога значило бы платить за это на каждой проверке живости.
    Stats,
    /// `GET /_inspect` — из чего состоит занятое место.
    Inspect,
    /// `POST /_validate` — сухой прогон приёма, без записи.
    Validate,
    /// `/_settings` — настройки на ходу.
    Settings,
    /// `/_snapshot…` — снимки. Хвост пути в [`Route::rest`].
    Snapshot,
    /// `/_security…` — учётные записи и ключи. Хвост в [`Route::rest`].
    Security,
    /// `POST /_bulk` — приём пачкой.
    Bulk,
    /// `POST|PUT|DELETE /{индекс}/_doc/{id}` — приём или гашение одного
    /// документа. Идентификатор в [`Route::rest`].
    Doc,
    /// `GET /{индекс}/_doc/{id}` — чтение одного документа по идентификатору.
    ///
    /// Отдельная точка, а не метод внутри [`Endpoint::Doc`]: у неё другое
    /// право (`read`, не `write`), другая цена и другой разряд в метриках.
    /// Пока она жила методом внутри пишущей точки, `GET` уходил в ветвь приёма
    /// и отвечал разбором пустого тела.
    DocGet,
    /// `POST /_mget` — чтение набора документов по списку идентификаторов.
    ///
    /// Отдельно от [`Endpoint::DocGet`], а не «тот же обработчик в цикле по
    /// запросам»: control plane читает набор синглтонов (пул аккаунтов, набор
    /// заданий панели) и обязан увидеть их в одном состоянии состава. Столько
    /// же отдельных `GET` берут снимок сегментов каждый свой, и между ними
    /// проходит публикация — набор собирается из разных поколений индекса.
    MGet,
    /// `POST /_put` — замена по идентификатору.
    Put,
    /// `POST /_mutate` — одна conditional mutation с replacements и deletes.
    Mutate,
    /// `POST /_delete` — гашение.
    Delete,
    /// `POST /_flush` — принудительный сброс.
    /// Сузить полосу ячеек этого узла, погасив всё, что ушло соседу.
    ///
    /// Отдельная точка, а не флаг у гашения: она удаляет не по списку, а **по
    /// принадлежности**, и потому обязана называться так, чтобы права на неё
    /// давались отдельно и осознанно.
    Reshard,
    /// Приём **уже применённой** записи от primary этого шарда.
    ///
    /// Отдельная точка, а не флаг у `_bulk`: у неё другой договор. Она несёт
    /// терм и номер, не разбирает условий (primary их уже рассудил), не
    /// пересылает дальше и отвергает отправителя с прежним термом — то есть
    /// делает ровно то, чего обычная запись делать не должна.
    Replicate,
    Flush,
    /// `POST /` — поиск (у координатора — веер).
    Search,
    /// Неизвестный путь или неподдерживаемый метод. Различие задаётся
    /// [`Route::allowed_methods`]: непустой список означает ответ 405.
    Unknown,
}

/// Разобранная цель запроса.
pub struct Route {
    /// Имя индекса, если цель начиналась с него: `/мойиндекс/_bulk`.
    /// В многoиндексном режиме по нему выбирается отдельный runtime.
    pub index: Option<String>,
    /// Точка маршрутизации.
    pub endpoint: Endpoint,
    /// Хвост после `/_snapshot` или `/_security` — вместе с ведущей косой
    /// чертой, как его отдавал прежний `strip_prefix`. У прочих точек пуст.
    pub rest: String,
    /// Строка запроса без `?`. Хранится, а не выбрасывается: `refresh` и
    /// прочие пригодятся совместимому приёму, а вот в сравнении путей ей
    /// делать нечего.
    #[allow(dead_code)]
    pub query: String,
}

impl Route {
    /// Разбирает цель запроса.
    ///
    /// Метод нужен только там, где от него зависит сама точка (`POST /` — это
    /// поиск, а `GET /` — нет), поэтому берётся отдельным доводом, а не
    /// вплетается в перечисление.
    pub fn parse(method: &str, target: &str) -> Route {
        // Строка запроса отрезается **первым делом** и до всякого сравнения:
        // именно её присутствие в цели и разводило четырёх читателей.
        let (path, query) = match target.split_once('?') {
            Some((p, q)) => (p, q.to_string()),
            None => (target, String::new()),
        };
        // Один хвостовой разделитель отбрасывается: `/_flush/` и `/_flush` —
        // одна точка, и отвечать на них по-разному не за что. Корень при этом
        // остаётся корнем.
        let path = if path.len() > 1 { path.trim_end_matches('/') } else { path };

        // Точки с хвостом разбираются до отделения имени индекса: у них хвост
        // произвольный (`/_security/api_key/тот-самый`), и принять его за имя
        // индекса было бы легко.
        if let Some(rest) = Route::endpoint_rest(path, "/_snapshot") {
            let endpoint = if method == "GET" || (method == "POST" && !rest.is_empty()) {
                Endpoint::Snapshot
            } else {
                Endpoint::Unknown
            };
            return Route::plain(endpoint, rest.to_string(), query);
        }
        if let Some(rest) = Route::endpoint_rest(path, "/_security") {
            let endpoint = match (method, rest) {
                ("GET" | "POST", "/api_key") => Endpoint::Security,
                ("DELETE", rest)
                    if rest
                        .strip_prefix("/api_key/")
                        .is_some_and(|id| !id.is_empty() && !id.contains('/')) =>
                {
                    Endpoint::Security
                }
                _ => Endpoint::Unknown,
            };
            return Route::plain(endpoint, rest.to_string(), query);
        }
        if path == "/_cat/indices" && method == "GET" {
            return Route::plain(Endpoint::CatIndices, String::new(), query);
        }
        if path == "/_cat/aliases" && method == "GET" {
            return Route::plain(Endpoint::CatAliases, String::new(), query);
        }
        if path == "/_aliases" && method == "POST" {
            return Route::plain(Endpoint::Alias, String::new(), query);
        }
        if let Some(alias) = path.strip_prefix("/_alias/") {
            if matches!(method, "GET" | "HEAD") && !alias.is_empty() {
                return Route::plain(Endpoint::Alias, Route::percent_decode(alias), query);
            }
        }

        // Имя индекса — первый сегмент, если он не начинается с подчёркивания.
        // Перечисление через запятую, `*` и `_all` пока не принимаются: такой
        // запрос требует честного межиндексного слияния top-K и агрегаций.
        // Молча ответить по первому индексу было бы правдоподобной ложью.
        let (index, tail) = match path.strip_prefix('/') {
            Some(rest) => match rest.split_once('/') {
                Some((head, tail))
                    if (!head.starts_with('_') || head == "_all") && !head.is_empty() =>
                {
                    (Some(Route::percent_decode(head)), format!("/{tail}"))
                }
                None if !rest.starts_with('_') && !rest.is_empty() => {
                    (Some(Route::percent_decode(rest)), "/".to_string())
                }
                _ => (None, path.to_string()),
            },
            None => (None, path.to_string()),
        };

        if let Some(rest) = Route::endpoint_rest(&tail, "/_snapshot") {
            let endpoint = if method == "GET" || (method == "POST" && !rest.is_empty()) {
                Endpoint::Snapshot
            } else {
                Endpoint::Unknown
            };
            return Route { index, endpoint, rest: rest.to_string(), query };
        }

        let mut doc_id = String::new();
        let endpoint = match (method, tail.as_str()) {
            ("GET" | "HEAD" | "PUT" | "DELETE", "/") if index.is_some() => Endpoint::Index,
            ("GET", "/_health") => Endpoint::Health,
            ("GET", "/_metrics") => Endpoint::Metrics,
            ("GET", "/_schema") | ("GET", "/_mapping") => Endpoint::Schema,
            ("GET", "/_stats") => Endpoint::Stats,
            ("GET", "/_inspect") => Endpoint::Inspect,
            ("POST", "/_validate") => Endpoint::Validate,
            ("GET" | "PUT" | "POST", "/_settings") => Endpoint::Settings,
            ("GET" | "PUT", "/_metadata") => Endpoint::Metadata,
            ("GET", "/_blocks") => Endpoint::Blocks,
            ("PUT" | "DELETE", t) if t.starts_with("/_block/") => Endpoint::Blocks,
            ("POST", "/_open") => Endpoint::Open,
            ("POST", "/_close") => Endpoint::Close,
            ("POST", "/_rollover") => Endpoint::Rollover,
            ("POST", t) if t.starts_with("/_rollover/") => Endpoint::Rollover,
            ("GET", "/_alias") => Endpoint::Alias,
            ("GET" | "PUT" | "DELETE", t) if t.starts_with("/_alias/") => Endpoint::Alias,
            ("POST", "/_bulk") => Endpoint::Bulk,
            // Привычная клиенту точка на один документ. Идентификатор кладётся
            // в `rest`, а не в отдельное поле: у снимков и прав хвост уже
            // хранится там, и заводить второе место под то же самое незачем.
            //
            // Чтение отделено от записи методом: право у них разное, и общая
            // точка означала бы, что `GET` получает право `write` либо что
            // запись получает право `read`. Оба исхода тихие.
            ("GET", t) if t.starts_with("/_doc/") => Endpoint::DocGet,
            ("POST" | "PUT" | "DELETE", t) if t.starts_with("/_doc/") => Endpoint::Doc,
            ("POST", "/_mget") => Endpoint::MGet,
            ("POST", "/_put") => Endpoint::Put,
            ("POST", "/_mutate") => Endpoint::Mutate,
            ("POST", "/_delete") => Endpoint::Delete,
            ("POST", "/_flush") => Endpoint::Flush,
            ("POST", "/_replicate") => Endpoint::Replicate,
            ("POST", "/_reshard") => Endpoint::Reshard,
            ("POST", "/") | ("POST" | "GET", "/_search") => Endpoint::Search,
            _ => Endpoint::Unknown,
        };
        if matches!(endpoint, Endpoint::Doc | Endpoint::DocGet) {
            doc_id = Route::percent_decode(tail.trim_start_matches("/_doc/"));
        } else if endpoint == Endpoint::Alias {
            if tail != "/_alias" {
                doc_id = Route::percent_decode(tail.trim_start_matches("/_alias/"));
            }
        } else if endpoint == Endpoint::Blocks {
            if tail != "/_blocks" {
                doc_id = Route::percent_decode(tail.trim_start_matches("/_block/"));
            }
        } else if endpoint == Endpoint::Rollover && tail != "/_rollover" {
            doc_id = Route::percent_decode(tail.trim_start_matches("/_rollover/"));
        }
        Route { index, endpoint, rest: doc_id, query }
    }

    /// Поддерживаемые методы именно этого пути. Пустой список означает 404,
    /// непустой при `Unknown` — 405. Таблица маршрутов остаётся единственной.
    pub fn allowed_methods(target: &str) -> Vec<&'static str> {
        ["GET", "HEAD", "POST", "PUT", "DELETE"]
            .into_iter()
            .filter(|method| Self::parse(method, target).endpoint != Endpoint::Unknown)
            .collect()
    }

    fn endpoint_rest<'a>(path: &'a str, endpoint: &str) -> Option<&'a str> {
        path.strip_prefix(endpoint).filter(|rest| rest.is_empty() || rest.starts_with('/'))
    }

    fn plain(endpoint: Endpoint, rest: String, query: String) -> Route {
        Route { index: None, endpoint, rest, query }
    }

    /// Возвращает `%XX` обратно в байты.
    ///
    /// ## Зачем это здесь
    ///
    /// Идентификатор документа — произвольная строка: у неё бывают и пробел, и
    /// косая черта, и кириллица. В пути они обязаны быть закодированы, и всякий
    /// клиент их кодирует. Пока разбор брал сегмент как есть, `PUT /_doc/м1`
    /// клал документ под именем `%d0%bc1`, а `POST /_bulk {"id":"м1"}` — под
    /// именем `м1`; два способа положить один документ давали два разных, и
    /// увидеть это было нечем — оба отвечали двумястами.
    ///
    /// ## Почему только идентификатор
    ///
    /// Хвост снимков проверяется `valid_name`, а идентификатор ключа узел
    /// выдаёт сам и делает его из безопасных знаков. Раскодировать заодно и их
    /// значило бы поменять поведение там, где ошибки не было.
    ///
    /// Неполная или неверная последовательность оставляется как есть: `%` —
    /// законный знак идентификатора, и отвергать из-за него весь запрос не за
    /// что. Если раскодированные байты не складываются в UTF-8, исходная строка
    /// также сохраняется целиком. Подмена каждого негодного байта на `�`
    /// схлопнула бы `%FF` и `%FE` в один идентификатор, то есть позволила бы
    /// одному пути прочитать или заменить документ другого.
    fn percent_decode(s: &str) -> String {
        if !s.contains('%') {
            return s.to_string();
        }
        let raw = s.as_bytes();
        let mut out: Vec<u8> = Vec::with_capacity(raw.len());
        let mut i = 0;
        while i < raw.len() {
            if raw[i] == b'%' && i + 2 < raw.len() {
                let hex = |b: u8| match b {
                    b'0'..=b'9' => Some(b - b'0'),
                    b'a'..=b'f' => Some(b - b'a' + 10),
                    b'A'..=b'F' => Some(b - b'A' + 10),
                    _ => None,
                };
                if let (Some(hi), Some(lo)) = (hex(raw[i + 1]), hex(raw[i + 2])) {
                    out.push(hi << 4 | lo);
                    i += 3;
                    continue;
                }
            }
            out.push(raw[i]);
            i += 1;
        }
        String::from_utf8(out).unwrap_or_else(|_| s.to_string())
    }

    /// Пишет ли эта точка. Одно место, а не перечисление в каждом читателе.
    ///
    /// [`Endpoint::DocGet`] сюда не входит намеренно: она читает. Пока чтение
    /// и запись жили одной точкой, `GET /_doc/{id}` числился пишущим — и на
    /// узле с `--read-only` отвечал «приём выключен» на запрос, который ничего
    /// не принимает.
    pub fn writes(&self) -> bool {
        matches!(
            self.endpoint,
            Endpoint::Bulk
                | Endpoint::Put
                | Endpoint::Mutate
                | Endpoint::Delete
                | Endpoint::Flush
                | Endpoint::Doc
                | Endpoint::Replicate
                | Endpoint::Reshard
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_lifecycle_and_search_routes_keep_the_index_name() {
        for method in ["PUT", "GET", "HEAD", "DELETE"] {
            let route = Route::parse(method, "/orders");
            assert_eq!(route.endpoint, Endpoint::Index);
            assert_eq!(route.index.as_deref(), Some("orders"));
        }
        for method in ["POST", "GET"] {
            let route = Route::parse(method, "/orders/_search");
            assert_eq!(route.endpoint, Endpoint::Search);
            assert_eq!(route.index.as_deref(), Some("orders"));
        }
        assert_eq!(Route::parse("GET", "/_cat/indices").endpoint, Endpoint::CatIndices);
        let unicode = Route::parse("GET", "/%D0%B7%D0%B0%D0%BA%D0%B0%D0%B7%D1%8B/_search");
        assert_eq!(unicode.index.as_deref(), Some("заказы"));
    }

    /// Строка запроса не меняет точку — ровно то, из-за чего заведён модуль.
    #[test]
    fn a_query_string_never_changes_the_endpoint() {
        for target in ["/_bulk", "/_bulk?refresh=true", "/_bulk?refresh=wait_for&pipeline=p"] {
            assert_eq!(
                Route::parse("POST", target).endpoint,
                Endpoint::Bulk,
                "цель {target:?} перестала быть приёмом"
            );
        }
        // И сама строка не теряется: она понадобится совместимому приёму.
        assert_eq!(Route::parse("POST", "/_bulk?refresh=true").query, "refresh=true");
        assert_eq!(Route::parse("POST", "/_bulk").query, "");
    }

    /// Имя индекса перед точкой — привычная клиенту форма.
    #[test]
    fn an_index_before_the_endpoint_is_taken_and_named() {
        let r = Route::parse("POST", "/мойиндекс/_bulk?refresh=true");
        assert_eq!(r.endpoint, Endpoint::Bulk);
        assert_eq!(r.index.as_deref(), Some("мойиндекс"));
        assert_eq!(r.query, "refresh=true");

        // Без имени индекса — то же самое, и это надо проверить отдельно:
        // иначе тест прошёл бы и у разбора, который имя просто выбрасывает.
        let r = Route::parse("POST", "/_bulk");
        assert_eq!(r.endpoint, Endpoint::Bulk);
        assert_eq!(r.index, None);
    }

    /// Селектор остаётся целиком: решение, допустим ли он для конкретной
    /// операции, принимает registry, а не синтаксический разборщик.
    #[test]
    fn multi_index_selectors_are_never_narrowed_to_the_first_name() {
        for target in ["/a,b/_bulk", "/*/_bulk", "/_all/_bulk"] {
            let route = Route::parse("POST", target);
            assert_eq!(route.endpoint, Endpoint::Bulk);
            assert_eq!(
                route.index.as_deref(),
                Some(target.trim_start_matches('/').trim_end_matches("/_bulk"))
            );
        }
    }

    #[test]
    fn alias_metadata_blocks_and_rollover_have_distinct_routes() {
        assert_eq!(Route::parse("POST", "/_aliases").endpoint, Endpoint::Alias);
        let alias = Route::parse("PUT", "/messages-000001/_alias/all_messages");
        assert_eq!(alias.endpoint, Endpoint::Alias);
        assert_eq!(alias.rest, "all_messages");
        assert_eq!(Route::parse("GET", "/messages-000001/_metadata").endpoint, Endpoint::Metadata);
        let block = Route::parse("PUT", "/messages-000001/_block/write");
        assert_eq!(block.endpoint, Endpoint::Blocks);
        assert_eq!(block.rest, "write");
        assert_eq!(Route::parse("POST", "/messages-000001/_close").endpoint, Endpoint::Close);
        assert_eq!(Route::parse("POST", "/messages-000001/_open").endpoint, Endpoint::Open);
        let rollover = Route::parse("POST", "/all_messages/_rollover/messages-000002");
        assert_eq!(rollover.endpoint, Endpoint::Rollover);
        assert_eq!(rollover.rest, "messages-000002");
    }

    /// Похожее имя точкой не становится.
    #[test]
    fn a_name_that_merely_starts_the_same_is_not_the_endpoint() {
        assert_eq!(Route::parse("POST", "/_bulkX").endpoint, Endpoint::Unknown);
        assert_eq!(Route::parse("POST", "/_bulk/лишнее").endpoint, Endpoint::Unknown);
        assert_eq!(Route::parse("GET", "/_snapshotХ").endpoint, Endpoint::Unknown);
        assert_eq!(Route::parse("GET", "/_securityX").endpoint, Endpoint::Unknown);
    }

    /// Хвост снимков и прав доезжает целиком.
    #[test]
    fn the_tail_of_a_prefix_endpoint_arrives_whole() {
        let r = Route::parse("DELETE", "/_security/api_key/тот-самый?x=1");
        assert_eq!(r.endpoint, Endpoint::Security);
        assert_eq!(r.rest, "/api_key/тот-самый");
        assert_eq!(r.query, "x=1");

        let r = Route::parse("POST", "/_snapshot/имя");
        assert_eq!(r.endpoint, Endpoint::Snapshot);
        assert_eq!(r.rest, "/имя");

        // Голый префикс даёт пустой хвост, а не отсутствие точки.
        assert_eq!(Route::parse("GET", "/_snapshot").rest, "");
    }

    /// Хвостовой разделитель не заводит вторую точку, а корень остаётся корнем.
    #[test]
    fn one_trailing_slash_is_the_same_endpoint_and_the_root_survives() {
        assert_eq!(Route::parse("POST", "/_flush/").endpoint, Endpoint::Flush);
        assert_eq!(Route::parse("POST", "/").endpoint, Endpoint::Search);
        assert_eq!(Route::parse("GET", "/").endpoint, Endpoint::Unknown);
    }

    /// Метод участвует в выборе точки там, где от него зависит смысл.
    #[test]
    fn the_method_decides_where_it_must() {
        assert_eq!(Route::parse("GET", "/_health").endpoint, Endpoint::Health);
        assert_eq!(Route::parse("POST", "/_health").endpoint, Endpoint::Unknown);
        assert_eq!(Route::parse("PUT", "/_settings").endpoint, Endpoint::Settings);
        assert_eq!(Route::parse("GET", "/_settings").endpoint, Endpoint::Settings);
    }

    #[test]
    fn unknown_paths_and_wrong_methods_are_distinguished() {
        for path in ["/_typo", "/orders/_typo", "/_snapshotX", "/_securityX", "/_security/typo"] {
            assert!(Route::allowed_methods(path).is_empty(), "{path}");
        }
        for (path, allowed) in [
            ("/_health", vec!["GET"]),
            ("/orders/_search?pretty=true", vec!["GET", "POST"]),
            ("/_settings", vec!["GET", "POST", "PUT"]),
            ("/_snapshot", vec!["GET"]),
            ("/_snapshot/name", vec!["GET", "POST"]),
            ("/_security/api_key", vec!["GET", "POST"]),
            ("/_security/api_key/id", vec!["DELETE"]),
            ("/orders", vec!["GET", "HEAD", "POST", "PUT", "DELETE"]),
        ] {
            assert_eq!(Route::parse("PATCH", path).endpoint, Endpoint::Unknown, "{path}");
            assert_eq!(Route::allowed_methods(path), allowed, "{path}");
        }
    }

    /// Пишущие точки перечислены в одном месте.
    #[test]
    fn writing_endpoints_are_listed_once() {
        for target in ["/_bulk", "/_put", "/_delete", "/_flush"] {
            assert!(Route::parse("POST", target).writes(), "{target} перестала писать");
        }
        for target in ["/", "/_health", "/_metrics"] {
            assert!(!Route::parse("GET", target).writes(), "{target} стала писать");
        }
        // Один документ: пишут три метода, читает четвёртый.
        for method in ["POST", "PUT", "DELETE"] {
            let r = Route::parse(method, "/_doc/м1");
            assert_eq!(r.endpoint, Endpoint::Doc, "{method} /_doc перестал писать");
            assert!(r.writes(), "{method} /_doc перестал писать");
        }
        let r = Route::parse("GET", "/_doc/м1");
        assert_eq!(r.endpoint, Endpoint::DocGet);
        assert!(!r.writes(), "чтение документа стало записью");
    }

    /// Пакетное чтение: `POST`, но не запись. Метод здесь взят из-за тела —
    /// список идентификаторов в строке запроса не помещается, — и это ровно тот
    /// случай, в котором «`POST` значит пишет» становится неправдой.
    #[test]
    fn mget_reads_despite_the_method() {
        let r = Route::parse("POST", "/_mget");
        assert_eq!(r.endpoint, Endpoint::MGet);
        assert!(!r.writes(), "/_mget стала записью");
        assert_eq!(Route::parse("POST", "/_mget/").endpoint, Endpoint::MGet);
        assert_eq!(Route::parse("POST", "/мойиндекс/_mget").endpoint, Endpoint::MGet);
        // Прочие методы сюда не попадают: у точки одно тело и один смысл.
        assert_eq!(Route::parse("GET", "/_mget").endpoint, Endpoint::Unknown);
        assert_eq!(Route::parse("POST", "/_mgetX").endpoint, Endpoint::Unknown);
    }

    /// Идентификатор доезжает до обработчика обоими методами, целиком и без
    /// строки запроса.
    #[test]
    fn the_document_id_arrives_whole_for_read_and_write() {
        let r = Route::parse("GET", "/сообщения/_doc/message:42?pretty=1");
        assert_eq!(r.endpoint, Endpoint::DocGet);
        assert_eq!(r.index.as_deref(), Some("сообщения"));
        assert_eq!(r.rest, "message:42");
        assert_eq!(r.query, "pretty=1");

        assert_eq!(Route::parse("PUT", "/_doc/м1").rest, "м1");
    }

    /// Закодированный идентификатор доезжает тем же, чем его положили.
    #[test]
    fn a_percent_encoded_id_arrives_decoded() {
        // Ровно то, что пошлёт curl или fetch на `_doc/м1`.
        assert_eq!(Route::parse("GET", "/_doc/%D0%BC1").rest, "м1");
        assert_eq!(Route::parse("PUT", "/сообщения/_doc/%D0%BC1").rest, "м1");
        // Пробел и косая черта внутри идентификатора.
        assert_eq!(Route::parse("GET", "/_doc/a%20b%2Fc").rest, "a b/c");
        // Строчный шестнадцатеричный — тот же байт.
        assert_eq!(Route::parse("GET", "/_doc/%d0%bc1").rest, "м1");
        // Незакодированное не трогается.
        assert_eq!(Route::parse("GET", "/_doc/message:42").rest, "message:42");
        // Одинокий процент — законный знак, а не повод отвергнуть запрос.
        assert_eq!(Route::parse("GET", "/_doc/100%").rest, "100%");
        assert_eq!(Route::parse("GET", "/_doc/%zz").rest, "%zz");
        // Хвост снимка не раскодируется: там своя проверка имени.
        assert_eq!(Route::parse("GET", "/_snapshot/%D0%BC1").rest, "/%D0%BC1");
    }

    /// Негодные UTF-8-байты не должны схлопывать разные пути в один документ.
    #[test]
    fn invalid_percent_encoded_bytes_do_not_alias_document_ids() {
        let a = Route::parse("GET", "/_doc/%FF");
        let b = Route::parse("GET", "/_doc/%FE");
        assert_eq!(a.rest, "%FF");
        assert_eq!(b.rest, "%FE");
        assert_ne!(a.rest, b.rest);
    }

    /// Новые точки узнаются только своим методом и не задевают соседей.
    #[test]
    fn stats_and_inspect_are_get_only() {
        assert_eq!(Route::parse("GET", "/_stats").endpoint, Endpoint::Stats);
        assert_eq!(Route::parse("GET", "/_inspect").endpoint, Endpoint::Inspect);
        // Другим методом — не эта точка. Иначе `POST /_stats` тихо ушёл бы в
        // выдачу сведений об узле вместо отказа.
        assert_eq!(Route::parse("POST", "/_stats").endpoint, Endpoint::Unknown);
        assert_eq!(Route::parse("DELETE", "/_inspect").endpoint, Endpoint::Unknown);
        // Похожее имя точкой не становится — то же правило, что и у `/_bulkX`.
        assert_eq!(Route::parse("GET", "/_statsX").endpoint, Endpoint::Unknown);
        assert!(!Route::parse("GET", "/_stats").writes());
        assert!(!Route::parse("GET", "/_inspect").writes());
    }
}
