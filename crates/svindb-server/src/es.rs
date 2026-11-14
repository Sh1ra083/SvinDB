//! Запрос в виде Elasticsearch, переведённый в родной.
//!
//! ## Почему перевод, а не второй разборщик
//!
//! Правило это сформулировал ещё координатор: разбирать один запрос дважды
//! означало бы два способа его понять, и однажды они разойдутся. Поэтому
//! ES-тело переписывается в родной JSON, а дальше идёт та же цепочка — отказ по
//! `noindex`, белый список ключей, потолки, бюджет памяти, страницы курсором.
//!
//! Даром достаётся и то, что иначе пришлось бы делать заново: проверка
//! `noindex` ищет ключ `field`, а в ES-теле имя поля — это ключ объекта, и без
//! перевода она не сработала бы вовсе.
//!
//! ## Свои дополнения — в своём ключе
//!
//! `slots`, `phrases`, `sparse`, `vector`, `translit`, `ids` кладутся в
//! `"svindb"`. Не ради красоты: Elasticsearch однажды заведёт свой ключ с
//! именем `phrases`, и белый список перестанет отличать опечатку от чужого
//! ключа.

/// На каком языке говорит тело.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Dialect {
    /// Родной: `{"k":10,"words":[…]}`.
    Native,
    /// Elasticsearch: `{"query":{…},"size":10}`.
    Es,
}

/// Приметы обоих семейств. Совпасть могут только у тела, которое не имеет
/// смысла ни на одном языке, — и тогда это отказ, а не догадка.
const NATIVE_MARKS: &[&str] = &[
    "k",
    "words",
    "phrases",
    "sparse",
    "vector",
    "translit",
    "max_expansions",
    "max_verify",
    "ids",
];
// `track_total_hits` есть и в native API, поэтому сам по себе не отличает
// диалект. ES остаётся однозначным по `_source`, `stored_fields`, `svindb` или
// форме `query`.
const ES_MARKS: &[&str] = &["_source", "stored_fields", "svindb"];

/// Определяет язык тела.
///
/// Явной таблицей примет, а не догадкой по одному ключу: `sort`, `from`, `size`
/// и `aggs` пишутся у обоих одинаково и приметой быть не могут.
pub fn dialect(v: &serde_json::Value) -> Result<Dialect, String> {
    let Some(obj) = v.as_object() else { return Ok(Dialect::Native) };
    let native = NATIVE_MARKS.iter().find(|k| obj.contains_key(**k));
    let es = ES_MARKS.iter().find(|k| obj.contains_key(**k));
    // `query` есть у обоих, но по-разному: у родного его ветви — `must`,
    // `should`, `must_not` с листьями `{"field":…,"terms":[…]}`, у ES внутри
    // стоит `bool`, `term`, `match` и подобные.
    let es_query = obj
        .get("query")
        .and_then(|q| q.as_object())
        .map(|q| {
            [
                "bool",
                "term",
                "terms",
                "match",
                "match_phrase",
                "range",
                "exists",
                "match_all",
                "nested",
                "prefix",
            ]
            .iter()
            .any(|k| q.contains_key(*k))
        })
        .unwrap_or(false);
    match (native, es.is_some() || es_query) {
        (Some(n), true) => Err(format!(
            "тело написано и на родном языке (ключ {n:?}), и на языке Elasticsearch: \
             выберите один — свои дополнения кладутся в ключ \"svindb\""
        )),
        (Some(_), false) => Ok(Dialect::Native),
        (None, true) => Ok(Dialect::Es),
        // Ни одной приметы: `{"size":10,"sort":[…]}` осмысленно на обоих и
        // значит одно и то же. Родной — потому что он и так разбирает такое.
        (None, false) => Ok(Dialect::Native),
    }
}

/// Переводит тело Elasticsearch в родное.
pub fn translate(v: &serde_json::Value) -> Result<serde_json::Value, String> {
    let mut normalized = v.clone();
    if let Some(query) = normalized.get_mut("query") {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| "дата now раньше эпохи")?
            .as_millis();
        let now = i64::try_from(now).map_err(|_| "дата now вне диапазона i64")?;
        normalize_date_ranges(query, now)?;
    }
    let v = &normalized;
    let obj = v.as_object().ok_or("запрос: ожидается объект")?;
    let mut out = serde_json::Map::new();

    // Совместимость означает либо исполнение, либо точный отказ. HTTP 200 на
    // запрос с проигнорированным фильтром хуже 400: ответ выглядит полным и по
    // нему принимают решения.
    const TOP: &[&str] = &[
        "query",
        "size",
        "from",
        "sort",
        "aggs",
        "search_after",
        "highlight",
        "_source",
        "track_total_hits",
        "svindb",
    ];
    for key in obj.keys() {
        if !TOP.contains(&key.as_str()) {
            return Err(format!(
                "параметр Elasticsearch {key:?} пока не поддерживается; запрос отвергнут, а не исполнен частично"
            ));
        }
    }

    // Размер страницы: `size` у них, `k` у нас. `size` = 0 — только агрегации.
    if let Some(size) = obj.get("size") {
        let size = size.as_u64().ok_or("size: нужно неотрицательное целое число")?;
        out.insert("k".into(), serde_json::json!(size));
        out.insert("size".into(), serde_json::json!(size));
    }
    for k in ["from", "aggs", "search_after", "highlight", "track_total_hits"] {
        if let Some(x) = obj.get(k) {
            out.insert(k.to_string(), x.clone());
        }
    }
    if let Some(sort) = obj.get("sort") {
        out.insert("sort".into(), translate_sort(sort)?);
    }
    // `_source` — это наш `fields`: `false` значит «ничего не отдавать»,
    // список — перечень полей.
    match obj.get("_source") {
        None => {
            out.insert("source".into(), serde_json::json!(true));
        }
        Some(serde_json::Value::Bool(false)) => {
            out.insert("source".into(), serde_json::json!(false));
        }
        Some(serde_json::Value::Bool(true)) => {
            out.insert("source".into(), serde_json::json!(true));
        }
        Some(serde_json::Value::Array(_)) => {
            return Err(
                "_source со списком включает source filtering, который пока не поддерживается; \
                 используйте true/false и svindb.fields для отдельной проекции"
                    .into(),
            )
        }
        Some(_) => return Err("_source: ожидается true или false".into()),
    }

    // Внутренний номер меняется после merge; Elasticsearch-hit без `_id`
    // нельзя связать с документом. Для ES-диалекта идентификатор обязателен.
    out.insert("ids".into(), serde_json::json!(true));

    if let Some(q) = obj.get("query") {
        let (words, must, must_not, filter) = leaf(q, true)?;
        if q.as_object().is_some_and(|q| q.contains_key("match_all")) {
            if q.as_object().is_some_and(|q| q.len() != 1) {
                return Err("match_all нельзя смешивать с другим видом query".into());
            }
            out.insert("match_all".into(), serde_json::json!(true));
        }
        let structured = words
            .iter()
            .chain(must.iter())
            .chain(must_not.iter())
            .any(|clause| clause.get("slots").is_some() || clause.get("bool").is_some());
        if structured {
            let mut tree = serde_json::Map::new();
            if !words.is_empty() {
                tree.insert("should".into(), serde_json::Value::Array(words));
            }
            if !must.is_empty() {
                tree.insert("must".into(), serde_json::Value::Array(must));
            }
            if !must_not.is_empty() {
                tree.insert("must_not".into(), serde_json::Value::Array(must_not));
            }
            if let Some(minimum) = q
                .get("bool")
                .and_then(|b| b.get("minimum_should_match"))
                .and_then(serde_json::Value::as_u64)
            {
                tree.insert("minimum_should_match".into(), serde_json::json!(minimum));
            }
            out.insert("query".into(), serde_json::Value::Object(tree));
        } else {
            if !words.is_empty() {
                out.insert("words".into(), serde_json::Value::Array(words));
            }
            if !must.is_empty() {
                out.insert("must".into(), serde_json::Value::Array(must));
            }
            if !must_not.is_empty() {
                out.insert("must_not".into(), serde_json::Value::Array(must_not));
            }
        }
        if !filter.is_empty() {
            out.insert("filter".into(), serde_json::Value::Array(filter));
        }
        if !structured {
            if let Some(minimum) = q
                .get("bool")
                .and_then(|b| b.get("minimum_should_match"))
                .and_then(serde_json::Value::as_u64)
            {
                out.insert("minimum_should_match".into(), serde_json::json!(minimum));
            }
        }
        if nested_depth(q) > 1 {
            out.insert("source_nested".into(), q.clone());
        }
    }

    // Свои дополнения вливаются последними и как есть.
    if let Some(extra) = obj.get("svindb") {
        let extra = extra.as_object().ok_or("svindb: ожидается объект")?;
        for (k, x) in extra.iter() {
            out.insert(k.clone(), x.clone());
        }
    }
    Ok(serde_json::Value::Object(out))
}

/// Поля строковых ES range-границ: обработчик проверяет их тип по ReadView.
pub fn date_range_fields(value: &serde_json::Value) -> Vec<String> {
    fn collect(value: &serde_json::Value, fields: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(object) => {
                if let Some(ranges) = object.get("range").and_then(serde_json::Value::as_object) {
                    for (field, options) in ranges {
                        let has_dates = ["gte", "gt", "lte", "lt"]
                            .iter()
                            .any(|key| options.get(*key).is_some_and(serde_json::Value::is_string));
                        if has_dates && !fields.contains(field) {
                            fields.push(field.clone());
                        }
                    }
                }
                for value in object.values() {
                    collect(value, fields);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    collect(value, fields);
                }
            }
            _ => {}
        }
    }
    let mut fields = Vec::new();
    if let Some(query) = value.get("query") {
        collect(query, &mut fields);
    }
    fields
}

/// Приводит даты до перевода и source verifier, используя один now на запрос.
fn normalize_date_ranges(query: &mut serde_json::Value, now: i64) -> Result<(), String> {
    match query {
        serde_json::Value::Object(object) => {
            if let Some(ranges) = object.get_mut("range").and_then(serde_json::Value::as_object_mut)
            {
                for (field, options) in ranges {
                    if let Some(options) = options.as_object_mut() {
                        for key in ["gte", "gt", "lte", "lt"] {
                            if let Some(value) = options.get_mut(key) {
                                if let Some(text) = value.as_str() {
                                    let millis = date_bound(text, now).ok_or_else(|| format!(
                                        "range.{field}.{key}: нужна ISO-8601 дата или now с одним смещением ms, s, m, h, d, w"
                                    ))?;
                                    *value = serde_json::json!(millis);
                                }
                            }
                        }
                    }
                }
            }
            for value in object.values_mut() {
                normalize_date_ranges(value, now)?;
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_date_ranges(value, now)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn date_bound(text: &str, now: i64) -> Option<i64> {
    if text == "now" {
        return Some(now);
    }
    if let Some(math) = text.strip_prefix("now") {
        let (sign, expression) = math.split_at_checked(1)?;
        if !matches!(sign, "+" | "-") {
            return None;
        }
        let digits = expression.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return None;
        }
        let amount: i64 = expression[..digits].parse().ok()?;
        let unit = match &expression[digits..] {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            _ => return None,
        };
        let offset = amount.checked_mul(unit)?;
        return if sign == "+" { now.checked_add(offset) } else { now.checked_sub(offset) };
    }
    svindb_core::iso::parse_millis(text)
}

fn nested_depth(query: &serde_json::Value) -> usize {
    match query {
        serde_json::Value::Object(object) => {
            usize::from(object.contains_key("nested"))
                + object.values().map(nested_depth).max().unwrap_or(0)
        }
        serde_json::Value::Array(items) => items.iter().map(nested_depth).max().unwrap_or(0),
        _ => 0,
    }
}

/// Обе формы Elasticsearch sort: строка и `{field:{order}}`. Родная форма
/// `{field:"...",order:"..."}` остаётся внутренней и документированной.
fn translate_sort(value: &serde_json::Value) -> Result<serde_json::Value, String> {
    let list = value.as_array().ok_or("sort: ожидается массив")?;
    let mut out = Vec::with_capacity(list.len());
    for (at, item) in list.iter().enumerate() {
        match item {
            serde_json::Value::String(field) => {
                out.push(serde_json::json!({"field": field, "order": "asc"}));
            }
            serde_json::Value::Object(object)
                if object.contains_key("field") || object.contains_key("order") =>
            {
                for key in object.keys() {
                    if !matches!(key.as_str(), "field" | "order") {
                        return Err(format!("sort[{at}]: ключ {key:?} неизвестен"));
                    }
                }
                out.push(item.clone());
            }
            serde_json::Value::Object(object) if object.len() == 1 => {
                let (field, options) = object.iter().next().expect("один элемент");
                let order = match options {
                    serde_json::Value::String(order) => order.as_str(),
                    serde_json::Value::Object(options) => {
                        for key in options.keys() {
                            if key != "order" && !(key == "missing" && options[key] == "_last") {
                                return Err(format!(
                                    "sort[{at}].{field}: параметр {key:?} пока не поддерживается"
                                ));
                            }
                        }
                        match options.get("order") {
                            None => "asc",
                            Some(value) => value
                                .as_str()
                                .ok_or_else(|| format!("sort[{at}].{field}.order: нужна строка"))?,
                        }
                    }
                    _ => return Err(format!("sort[{at}].{field}: ожидается order")),
                };
                if !matches!(order, "asc" | "desc") {
                    return Err(format!("sort[{at}].{field}: order обязан быть asc или desc"));
                }
                out.push(serde_json::json!({"field": field, "order": order}));
            }
            _ => return Err(format!("sort[{at}]: неизвестная форма")),
        }
    }
    Ok(serde_json::Value::Array(out))
}

/// Разбирает ветвь запроса в четыре родных списка.
#[allow(clippy::type_complexity)]
fn leaf(
    q: &serde_json::Value,
    allow_match_all: bool,
) -> Result<
    (
        Vec<serde_json::Value>,
        Vec<serde_json::Value>,
        Vec<serde_json::Value>,
        Vec<serde_json::Value>,
    ),
    String,
> {
    let obj = q.as_object().ok_or("query: ожидается объект")?;
    if obj.len() != 1 {
        if obj.contains_key("match_all") {
            return Err("match_all нельзя смешивать с другим видом query".into());
        }
        return Err(format!(
            "query обязан содержать ровно один вид условия, получено {}",
            obj.len()
        ));
    }
    let mut words = Vec::new();
    let mut must = Vec::new();
    let mut must_not = Vec::new();
    let mut filter = Vec::new();

    for (verb, body) in obj.iter() {
        match verb.as_str() {
            "match_all" => {
                if !allow_match_all {
                    return Err("match_all внутри bool пока не поддержан".into());
                }
                let body = body.as_object().ok_or("match_all: ожидается объект")?;
                if !body.is_empty() {
                    return Err("match_all: объект обязан быть пустым".into());
                }
            }
            "match" => words.extend(one_field(body, "match", true)?),
            "term" => must.extend(one_field(body, "term", false)?),
            "terms" => must.extend(terms_of(body)?),
            "range" => filter.push(range_of(body)?),
            "exists" => {
                let object = body.as_object().ok_or("exists: ожидается объект")?;
                for key in object.keys() {
                    if key != "field" {
                        return Err(format!("exists: параметр {key:?} не поддерживается"));
                    }
                }
                let f = body.get("field").and_then(|x| x.as_str()).ok_or("exists: нужно field")?;
                must.push(serde_json::json!({
                    "field": f,
                    "terms": [svindb_core::indexer::PRESENCE_TERM],
                    "exists": true,
                    "boost": 0.0
                }));
            }
            "nested" => must.push(nested_of(body)?),
            "bool" => {
                let b = body.as_object().ok_or("bool: ожидается объект")?;
                for key in b.keys() {
                    if !matches!(
                        key.as_str(),
                        "must" | "should" | "must_not" | "filter" | "minimum_should_match"
                    ) {
                        return Err(format!(
                            "bool: параметр {key:?} пока не поддерживается; запрос отвергнут без частичного исполнения"
                        ));
                    }
                }
                for arm in ["must", "should", "must_not"] {
                    let Some(list) = b.get(arm) else { continue };
                    let list = list.as_array().ok_or(format!("bool.{arm}: нужен массив"))?;
                    for x in list.iter() {
                        if x.get("bool").is_some() {
                            // Вложенный bool остаётся одним clause, в том числе
                            // под must_not. Расплющивание списков меняет OR/NOT
                            // и minimum_should_match каждого уровня.
                            let (w, m, n, f) = leaf(x, false)?;
                            if !f.is_empty() {
                                return Err(format!(
                                    "bool.{arm}: range внутри вложенного bool требует локального filter; запрос отвергнут"
                                ));
                            }
                            let minimum = x["bool"]
                                .get("minimum_should_match")
                                .and_then(serde_json::Value::as_u64)
                                .unwrap_or(0);
                            let clause = serde_json::json!({"bool": {
                                "must": m, "should": w, "must_not": n,
                                "minimum_should_match": minimum
                            }});
                            match arm {
                                "must" => must.push(clause),
                                "should" => words.push(clause),
                                _ => must_not.push(clause),
                            }
                            continue;
                        }
                        let (w, m, n, f) = leaf(x, false)?;
                        match arm {
                            "must" => {
                                if !n.is_empty() {
                                    return Err(
                                        "bool.must: внутреннее отрицание не поддерживается".into(),
                                    );
                                }
                                must.extend(w);
                                must.extend(m);
                                filter.extend(f);
                            }
                            "should" => {
                                if !n.is_empty() || !f.is_empty() {
                                    return Err(
                                        "bool.should: range/отрицание пока не переводится без изменения семантики"
                                            .into(),
                                    );
                                }
                                words.extend(w);
                                words.extend(m);
                            }
                            _ => {
                                if !n.is_empty() || !f.is_empty() {
                                    return Err(
                                        "bool.must_not: range/внутреннее отрицание пока не поддерживается"
                                            .into(),
                                    );
                                }
                                must_not.extend(w);
                                must_not.extend(m);
                            }
                        }
                    }
                }
                // `filter` у ES не влияет на вес. У нас вес даёт только
                // словесное условие, а отбор его и так не даёт, — значит
                // отбор ложится точно, а условие по словам внутри `filter`
                // пришлось бы класть в `must`, где оно вес даёт. Отказываем
                // прямо, вместо того чтобы менять порядок выдачи молча.
                if let Some(list) = b.get("filter") {
                    let list = list.as_array().ok_or("bool.filter: нужен массив")?;
                    for x in list.iter() {
                        if x.get("bool").is_some() {
                            return Err(
                                "bool.filter: вложенный bool пока не переводится без изменения группировки"
                                    .into(),
                            );
                        }
                        let (mut w, mut m, mut n, f) = leaf(x, false)?;
                        if !n.is_empty() {
                            return Err(
                                "bool.filter: внутреннее отрицание не поддерживается".into()
                            );
                        }
                        let zero = |clauses: &mut Vec<serde_json::Value>| {
                            for clause in clauses {
                                if let Some(object) = clause.as_object_mut() {
                                    object.insert("boost".into(), serde_json::json!(0.0));
                                }
                            }
                        };
                        zero(&mut w);
                        zero(&mut m);
                        zero(&mut n);
                        // Нулевой boost сохраняет отбор, но не меняет score —
                        // ровно семантика filter у Elasticsearch.
                        must.extend(w);
                        must.extend(m);
                        must_not.extend(n);
                        filter.extend(f);
                    }
                }
                if let Some(m) = b.get("minimum_should_match") {
                    m.as_u64()
                        .ok_or("bool.minimum_should_match: пока поддерживается целое число")?;
                }
            }
            other => {
                return Err(format!(
                    "query: {other:?} пока не переводится; есть match, term, terms, range, \
                     exists, bool, nested, match_all"
                ))
            }
        }
    }
    Ok((words, must, must_not, filter))
}

/// Elasticsearch nested в один или несколько coarse slot-узлов.
///
/// При двух уровнях каждый узел отдельно сужает кандидатов, а точную связь
/// родителя и ребёнка проверяет `source_nested` по lossless source. Поэтому
/// здесь допустимы лишние кандидаты, но недопустим пропуск подходящего.
fn nested_of(body: &serde_json::Value) -> Result<serde_json::Value, String> {
    let object = body.as_object().ok_or("nested: ожидается объект")?;
    for key in object.keys() {
        if !matches!(key.as_str(), "path" | "query" | "score_mode") {
            return Err(format!("nested: ключ {key:?} неизвестен"));
        }
    }
    let path = object
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or("nested.path: нужна строка")?;
    if let Some(mode) = object.get("score_mode") {
        let mode = mode.as_str().ok_or("nested.score_mode: нужна строка")?;
        if !matches!(mode, "none" | "avg") {
            return Err("nested.score_mode: поддерживаются none и avg".into());
        }
    }
    let query = object.get("query").ok_or("nested.query отсутствует")?;
    let query_object = query.as_object().ok_or("nested.query: ожидается объект")?;
    if query_object.len() != 1 {
        return Err("nested.query: нужен ровно один вид условия".into());
    }
    let mut all = Vec::new();
    let mut none = Vec::new();
    let mut children = Vec::new();

    fn one(
        query: &serde_json::Value,
        path: &str,
        target: &mut Vec<serde_json::Value>,
    ) -> Result<(), String> {
        let object = query.as_object().ok_or("nested.query: ожидается объект")?;
        if object.len() != 1 {
            return Err("nested.query: нужен ровно один вид условия".into());
        }
        for (verb, body) in object {
            if verb == "nested" {
                return Err("внутренний nested разбирается родительским узлом".into());
            }
            let value = match verb.as_str() {
                "term" => one_field(body, "nested.term", false)?
                    .into_iter()
                    .next()
                    .ok_or("nested.term пуст")?,
                "terms" => terms_of(body)?.into_iter().next().ok_or("nested.terms пуст")?,
                "match" => {
                    let terms = one_field(body, "nested.match", true)?;
                    for term in terms {
                        check_nested_field(&term, path)?;
                        target.push(term);
                    }
                    continue;
                }
                "range" => serde_json::json!({"range": range_of(body)?}),
                "exists" => {
                    let options = body.as_object().ok_or("nested.exists: ожидается объект")?;
                    for key in options.keys() {
                        if key != "field" {
                            return Err(format!(
                                "nested.exists: параметр {key:?} не поддерживается"
                            ));
                        }
                    }
                    let field = body
                        .get("field")
                        .and_then(serde_json::Value::as_str)
                        .ok_or("nested.exists: нужно field")?;
                    serde_json::json!({"exists": {"field": field}})
                }
                other => return Err(format!("nested.query: {other:?} пока не поддерживается")),
            };
            check_nested_field(&value, path)?;
            target.push(value);
        }
        Ok(())
    }

    if let Some(boolean) = query.get("bool") {
        let boolean = boolean.as_object().ok_or("nested.query.bool: ожидается объект")?;
        for key in boolean.keys() {
            if !matches!(key.as_str(), "must" | "filter" | "must_not") {
                return Err(format!(
                    "nested.query.bool.{key} пока не поддерживается; запрос отвергнут без flatten"
                ));
            }
        }
        for arm in ["must", "filter"] {
            if let Some(items) = boolean.get(arm) {
                for item in
                    items.as_array().ok_or_else(|| format!("nested.bool.{arm}: нужен массив"))?
                {
                    if let Some(nested) = item.get("nested") {
                        if item.as_object().is_none_or(|object| object.len() != 1) {
                            return Err("nested.query: нужен ровно один вид условия".into());
                        }
                        children.push(nested_of(nested)?);
                    } else {
                        one(item, path, &mut all)?;
                    }
                }
            }
        }
        if let Some(items) = boolean.get("must_not") {
            for item in items.as_array().ok_or("nested.bool.must_not: нужен массив")? {
                if let Some(nested) = item.get("nested") {
                    if item.as_object().is_none_or(|object| object.len() != 1) {
                        return Err("nested.query: нужен ровно один вид условия".into());
                    }
                    // Даже coarse-условие, проверяемое по source позднее,
                    // обязано пройти ту же строгую проверку синтаксиса.
                    nested_of(nested)?;
                } else {
                    one(item, path, &mut none)?;
                }
                // Отрицательный дочерний nested не сужает coarse-кандидатов:
                // точный запрет применит source verifier.
            }
        }
    } else {
        if let Some(nested) = query.get("nested") {
            children.push(nested_of(nested)?);
        } else {
            one(query, path, &mut all)?;
        }
    }
    if all.is_empty() && children.is_empty() {
        return Err("nested.query: нужно хотя бы одно положительное условие".into());
    }
    if !all.is_empty() {
        children.insert(
            0,
            serde_json::json!({
                "slots": {
                    "group": path,
                    "all": all,
                    "none": none,
                    "boost": 0.0
                }
            }),
        );
    }
    if children.len() == 1 {
        Ok(children.pop().expect("один nested clause"))
    } else {
        Ok(serde_json::json!({
            "bool": {
                "must": children
            }
        }))
    }
}

fn check_nested_field(value: &serde_json::Value, path: &str) -> Result<(), String> {
    fn field(value: &serde_json::Value) -> Option<&str> {
        if let Some(field) = value.get("field").and_then(serde_json::Value::as_str) {
            return Some(field);
        }
        for key in ["term", "range", "exists"] {
            if let Some(field) = value
                .get(key)
                .and_then(|value| value.get("field"))
                .and_then(serde_json::Value::as_str)
            {
                return Some(field);
            }
        }
        None
    }
    let field = field(value).ok_or("nested: условие не назвало field")?;
    if field == path || field.strip_prefix(path).is_some_and(|tail| tail.starts_with('.')) {
        Ok(())
    } else {
        Err(format!("nested.path {path:?}, но условие ссылается на поле {field:?}"))
    }
}

/// `{"поле":"значение"}` или `{"поле":{"query":"значение"}}`.
fn one_field(
    body: &serde_json::Value,
    what: &str,
    split: bool,
) -> Result<Vec<serde_json::Value>, String> {
    let obj = body.as_object().ok_or(format!("{what}: ожидается объект"))?;
    if obj.len() != 1 {
        return Err(format!("{what}: ожидается ровно одно поле, получено {}", obj.len()));
    }
    let (field, val) = obj.iter().next().ok_or(format!("{what}: пусто"))?;
    let (text, boost) = match val {
        serde_json::Value::String(s) => (s.clone(), None),
        serde_json::Value::Object(options) => {
            let value_key = if split { "query" } else { "value" };
            for key in options.keys() {
                if key != value_key && key != "boost" {
                    return Err(format!("{what}.{field}: параметр {key:?} пока не поддерживается"));
                }
            }
            let value = options
                .get(value_key)
                .ok_or_else(|| format!("{what}.{field}: нужно {value_key}"))?;
            let text = match value {
                serde_json::Value::String(value) => value.clone(),
                other => other.to_string().trim_matches('"').to_string(),
            };
            let boost = options
                .get("boost")
                .map(|value| {
                    value.as_f64().ok_or_else(|| format!("{what}.{field}.boost: нужно число"))
                })
                .transpose()?;
            (text, boost)
        }
        other => (other.to_string().trim_matches('"').to_string(), None),
    };
    if !split {
        let mut clause = serde_json::json!({ "field": field, "terms": [text] });
        if let Some(boost) = boost {
            clause["boost"] = serde_json::json!(boost);
        }
        return Ok(vec![clause]);
    }
    // `match` разбирается на слова, и каждое становится **своим** условием.
    // Несколько слов в одном условии значат «одно из этих написаний», а не
    // «все они», и путать это нельзя.
    Ok(text
        .split_whitespace()
        .map(|word| {
            let mut clause = serde_json::json!({ "field": field, "terms": [word] });
            if let Some(boost) = boost {
                clause["boost"] = serde_json::json!(boost);
            }
            clause
        })
        .collect())
}

fn terms_of(body: &serde_json::Value) -> Result<Vec<serde_json::Value>, String> {
    let obj = body.as_object().ok_or("terms: ожидается объект")?;
    if obj.len() != 1 {
        return Err(format!("terms: ожидается ровно одно поле, получено {}", obj.len()));
    }
    let (field, val) = obj.iter().next().ok_or("terms: пусто")?;
    let list = val.as_array().ok_or(format!("terms.{field}: нужен массив"))?;
    let words: Vec<serde_json::Value> = list
        .iter()
        .map(|x| match x {
            serde_json::Value::String(s) => serde_json::json!(s),
            other => serde_json::json!(other.to_string().trim_matches('"')),
        })
        .collect();
    Ok(vec![serde_json::json!({ "field": field, "terms": words })])
}

/// `{"поле":{"gte":1,"lt":5}}` — в отбор с включительными границами.
fn range_of(body: &serde_json::Value) -> Result<serde_json::Value, String> {
    let obj = body.as_object().ok_or("range: ожидается объект")?;
    if obj.len() != 1 {
        return Err(format!("range: ожидается ровно одно поле, получено {}", obj.len()));
    }
    let (field, bounds) = obj.iter().next().ok_or("range: пусто")?;
    let b = bounds.as_object().ok_or(format!("range.{field}: ожидается объект"))?;
    let mut out = serde_json::Map::new();
    out.insert("field".into(), serde_json::json!(field));
    for (k, v) in b.iter() {
        // Строгая граница переводится в включительную **здесь**, где тип поля
        // ещё не известен, поэтому только для целых: у дробного шаг задаёт сам
        // разбор отбора, который тип знает. Отказ честнее сдвига наугад.
        match k.as_str() {
            "gte" => {
                out.insert("from".into(), v.clone());
            }
            "lte" => {
                out.insert("to".into(), v.clone());
            }
            "gt" => {
                let n = v.as_i64().ok_or(format!("range.{field}.gt: пока только целое"))?;
                out.insert("from".into(), serde_json::json!(n.saturating_add(1)));
            }
            "lt" => {
                let n = v.as_i64().ok_or(format!("range.{field}.lt: пока только целое"))?;
                out.insert("to".into(), serde_json::json!(n.saturating_sub(1)));
            }
            other => return Err(format!("range.{field}: ключ {other:?} неизвестен")),
        }
    }
    Ok(serde_json::Value::Object(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_missing_last_matches_native_sort_policy() {
        let body = serde_json::json!({"query":{"match_all":{}},
            "sort":[{"rank":{"order":"desc","missing":"_last"}}]});
        let native = translate(&body).unwrap();
        assert_eq!(native["sort"][0], serde_json::json!({"field":"rank","order":"desc"}));
        let unsupported = serde_json::json!({"query":{"match_all":{}},
            "sort":[{"rank":{"order":"desc","missing":"_first"}}]});
        assert!(translate(&unsupported).is_err());
    }

    #[test]
    fn date_ranges_use_iso_dates_and_one_now_for_all_bounds() {
        assert_eq!(date_bound("now-15m", 1_000_000), Some(100_000));
        assert_eq!(date_bound("now+2h", 1_000_000), Some(8_200_000));
        assert_eq!(date_bound("2024-01-01T00:00:00Z", 0), Some(1_704_067_200_000));
        for unsupported in
            ["now/d", "now-1M", "now-15m/d", "now-1h+2m", "now-999999999999999999999h"]
        {
            assert!(date_bound(unsupported, 0).is_none(), "{unsupported}");
        }
        let native = translate(&serde_json::json!({"query":{"bool":{"filter":[
            {"range":{"created":{"gte":"now-15m","lte":"now"}}},
            {"range":{"updated":{"gte":"now-15m","lte":"now"}}}
        ]}}}))
        .unwrap();
        assert_eq!(native["filter"][0]["from"], native["filter"][1]["from"]);
        assert_eq!(native["filter"][0]["to"], native["filter"][1]["to"]);
        assert_eq!(
            native["filter"][0]["to"].as_i64().unwrap()
                - native["filter"][0]["from"].as_i64().unwrap(),
            900_000
        );
        let dated = serde_json::json!({"query":{"range":{"created":{"gte":"2024-01-01","lt":"2024-01-02"}}}});
        assert_eq!(date_range_fields(&dated), vec!["created".to_string()]);
        let native = translate(&dated).unwrap();
        assert_eq!(native["filter"][0]["from"], 1_704_067_200_000i64);
        assert_eq!(native["filter"][0]["to"], 1_704_153_599_999i64);
    }

    #[test]
    fn nested_queries_reject_ignored_siblings_and_options() {
        for query in [
            serde_json::json!({"bool":{"must":[{"term":{"items.name":"x"}}]},"ignored":{}}),
            serde_json::json!({"exists":{"field":"items.name","ignored":true}}),
            serde_json::json!({"bool":{"must":[{"nested":{"path":"items.child","query":{"term":{"items.child.name":"x"}}},"ignored":{}}]}}),
            serde_json::json!({"bool":{"must":[{"term":{"items.name":"x"}}],"must_not":[{"nested":{"path":"items.child","query":{"exists":{"field":"items.child.name","ignored":true}}}}]}}),
        ] {
            let body = serde_json::json!({"query":{"nested":{"path":"items","query":query}}});
            assert!(translate(&body).is_err(), "{body}");
        }
        let body = serde_json::json!({"query":{"prefix":{"body":"x"}}});
        assert_eq!(dialect(&body).unwrap(), Dialect::Es);
        assert!(translate(&body).unwrap_err().contains("prefix"));
        assert!(translate(&serde_json::json!({"size":"10"})).is_err());
        assert!(translate(&serde_json::json!({"query":{"nested":{"path":"items","score_mode":7,"query":{"term":{"items.name":"x"}}}}})).is_err());
    }

    fn v(s: &str) -> serde_json::Value {
        serde_json::from_str(s).expect("тело")
    }

    /// Язык решается таблицей примет, а совпадение обоих — отказ.
    #[test]
    fn the_dialect_is_decided_by_marks_and_a_clash_is_refused() {
        assert_eq!(dialect(&v(r#"{"k":10,"words":[]}"#)).unwrap(), Dialect::Native);
        assert_eq!(dialect(&v(r#"{"query":{"bool":{}}}"#)).unwrap(), Dialect::Es);
        assert_eq!(dialect(&v(r#"{"_source":false}"#)).unwrap(), Dialect::Es);
        // Общие ключи приметой не считаются: они значат одно и то же.
        assert_eq!(dialect(&v(r#"{"size":10,"from":5}"#)).unwrap(), Dialect::Native);

        let e = dialect(&v(r#"{"words":[],"query":{"bool":{}}}"#))
            .expect_err("тело на двух языках принято");
        assert!(e.contains("words"), "отказ не назвал примету: {e}");
    }

    /// `range` с `gte`/`lt` становится отбором с включительными границами.
    #[test]
    fn a_range_becomes_an_inclusive_filter() {
        let out = translate(&v(r#"{"query":{"range":{"views":{"gte":10,"lt":20}}}}"#)).unwrap();
        let f = &out["filter"][0];
        assert_eq!(f["field"], "views");
        assert_eq!(f["from"], 10, "gte обязано остаться включительным");
        assert_eq!(f["to"], 19, "lt обязано стать девятнадцатью, а не двадцатью");
    }

    /// `match` из двух слов даёт два условия, а не одно с двумя написаниями.
    #[test]
    fn a_match_of_two_words_becomes_two_conditions() {
        let out = translate(&v(r#"{"query":{"match":{"body":"дом дорога"}}}"#)).unwrap();
        let w = out["words"].as_array().expect("слова");
        assert_eq!(w.len(), 2, "слова слиплись в одно условие: {w:?}");
        assert_eq!(w[0]["terms"][0], "дом");
        assert_eq!(w[1]["terms"][0], "дорога");

        // А `term` не разбирается на слова: он точен по определению.
        let out = translate(&v(r#"{"query":{"term":{"метка":"дом дорога"}}}"#)).unwrap();
        assert_eq!(out["must"][0]["terms"][0], "дом дорога", "term разобран на слова");
    }

    /// `bool` раскладывается по ветвям, а `_source:true` включает exact source.
    #[test]
    fn a_bool_tree_and_source_are_translated() {
        let out = translate(&v(r#"{"query":{"bool":{"must":[{"term":{"a":"x"}}],
                 "must_not":[{"term":{"b":"y"}}],
                 "filter":[{"range":{"n":{"gte":1}}}]}},"_source":true,"size":5}"#))
        .unwrap();
        assert_eq!(out["must"][0]["terms"][0], "x");
        assert_eq!(out["must_not"][0]["terms"][0], "y");
        assert_eq!(out["filter"][0]["from"], 1);
        assert_eq!(out["source"], true);
        assert_eq!(out["k"], 5);
    }

    #[test]
    fn track_total_hits_is_forwarded_without_making_native_and_es_conflict() {
        let out =
            translate(&v(r#"{"query":{"match_all":{}},"track_total_hits":25,"size":3}"#)).unwrap();
        assert_eq!(out["track_total_hits"], 25);
        assert_eq!(dialect(&v(r#"{"k":3,"track_total_hits":true}"#)).unwrap(), Dialect::Native);
    }

    /// Слова внутри `bool.filter` исполняются с нулевым вкладом в score.
    #[test]
    fn words_inside_a_bool_filter_do_not_score() {
        let out = translate(&v(r#"{"query":{"bool":{"filter":[{"term":{"a":"x"}}]}}}"#))
            .expect("term внутри filter");
        assert_eq!(out["must"][0]["terms"][0], "x");
        assert_eq!(out["must"][0]["boost"], 0.0);
    }

    /// Свои дополнения едут рядом и не мешают.
    #[test]
    fn native_extras_ride_alongside() {
        let out = translate(&v(
            r#"{"query":{"match":{"b":"дом"}},"svindb":{"ids":true,"translit":true}}"#,
        ))
        .unwrap();
        assert_eq!(out["ids"], true);
        assert_eq!(out["translit"], true);
        assert_eq!(out["words"][0]["terms"][0], "дом");
    }

    /// `match_all` не должен исчезать при переводе: родному разборщику нужен
    /// явный признак, отличающий полный обход от ошибочно пустого запроса.
    #[test]
    fn match_all_survives_translation() {
        let out = translate(&v(r#"{"query":{"match_all":{}},"size":25,"from":50}"#)).unwrap();
        assert_eq!(out["match_all"], true);
        assert_eq!(out["size"], 25);
        assert_eq!(out["from"], 50);

        let mixed = translate(&v(r#"{"query":{"match_all":{},"term":{"a":"x"}}}"#))
            .expect_err("match_all смешан с другим запросом");
        assert!(mixed.contains("смешивать"), "непонятный отказ: {mixed}");

        let nested =
            translate(&v(r#"{"query":{"bool":{"should":[{"match_all":{}},{"term":{"a":"x"}}]}}}"#))
                .expect_err("вложенный match_all молча потерян");
        assert!(nested.contains("внутри bool"), "непонятный отказ: {nested}");
    }

    /// Неизвестный вид условия называется, а не проглатывается.
    #[test]
    fn an_unknown_leaf_is_named() {
        let e = translate(&v(r#"{"query":{"fuzzy":{"a":"x"}}}"#)).expect_err("fuzzy принят");
        assert!(e.contains("fuzzy"), "отказ не назвал вид: {e}");
    }

    /// Поддерживаемое подмножество не имеет права выбирать первое поле или
    /// первый параметр и молча выбрасывать остальные.
    #[test]
    fn supported_leaves_reject_every_unimplemented_tail() {
        for (body, named) in [
            (r#"{"query":{"term":{"a":"x","b":"y"}}}"#, "одно поле"),
            (r#"{"query":{"match":{"a":{"query":"x","operator":"and"}}}}"#, "operator"),
            (r#"{"query":{"range":{"n":{"gte":1,"time_zone":"+03:00"}}}}"#, "time_zone"),
            (r#"{"query":{"bool":{"must":[],"boost":2}}}"#, "boost"),
            (r#"{"query":{"exists":{"field":"a","boost":2}}}"#, "boost"),
            (r#"{"query":{"bool":{"should":[{"range":{"n":{"gte":1}}}]}}}"#, "bool.should"),
            (r#"{"query":{"bool":{"must_not":[{"range":{"n":{"gte":1}}}]}}}"#, "bool.must_not"),
            (
                r#"{"query":{"bool":{"should":[{"bool":{"filter":[{"range":{"n":{"gte":1}}}]}}]}}}"#,
                "локального filter",
            ),
            (r#"{"query":{"match_all":{}},"_source":["a"]}"#, "source filtering"),
        ] {
            let error = translate(&v(body)).expect_err("неподдержанный хвост проигнорирован");
            assert!(error.contains(named), "отказ {error:?} не назвал {named:?}");
        }

        let term = translate(&v(r#"{"query":{"term":{"a":{"value":"Case","boost":2}}}}"#))
            .expect("стандартная объектная форма term");
        assert_eq!(term["must"][0]["terms"][0], "Case");
        assert_eq!(term["must"][0]["boost"].as_f64(), Some(2.0));
    }

    #[test]
    fn nested_boolean_preserves_groups_and_each_minimum() {
        let translated = translate(&serde_json::json!({"query": {"bool": {
            "should": [
                {"term": {"a": "x"}},
                {"bool": {"must_not": [{"term": {"a": "y"}}]}},
                {"bool": {"should": [{"term": {"a": "z"}}, {"term": {"a": "w"}}],
                          "minimum_should_match": 2}}
            ], "minimum_should_match": 1
        }}}))
        .unwrap();
        let tree = &translated["query"];
        assert_eq!(tree["should"].as_array().unwrap().len(), 3);
        assert_eq!(tree["minimum_should_match"], 1);
        assert_eq!(tree["should"][1]["bool"]["must_not"][0]["terms"][0], "y");
        assert_eq!(tree["should"][2]["bool"]["minimum_should_match"], 2);
        assert!(translated.get("must_not").is_none());
        assert!(translated.get("filter").is_none());
    }
}
