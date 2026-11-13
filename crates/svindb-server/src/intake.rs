//! Разбор ячейки документа на приёме — в одном месте.
//!
//! ## Почему в одном
//!
//! Решение «что делать со значением такого вида в поле такого типа» принималось
//! дважды: `cell_text` для приёма по сети и `text_of` для `svindb index` из
//! NDJSON. Два места, одно правило — и они уже разошлись: офлайновый путь
//! разбирал массив чисел в плотный вектор, сетевой отдавал на нём пустоту.
//! Третьего расхождения быть не должно.
//!
//! ## Почему отказ, а не пустота
//!
//! Прежде **всё** негодное давало пустую строку, а пустая строка значит
//! «значения нет». То есть массив в скалярном поле, объект, текст в числовом
//! поле и число за разрядностью выглядели для индекса ровно как отсутствие
//! значения — и клиент получал `{"added":N}` без единого слова о том, что часть
//! данных не дошла.
//!
//! Молчание здесь дороже отказа. Заметить его можно только по пустой выдаче
//! через недели, и объяснить будет нечем: приём ответил «принято».

use svindb_core::index::Kind;

/// Что вышло из ячейки.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Taken {
    /// Годное значение — так, как его ждёт приём.
    Value(String),
    /// Ключа нет либо `null`. Это **законно** и молчаливо: так JSON пишет
    /// «значения нет», и отказывать на этом значило бы требовать заполнять все
    /// поля схемы.
    Absent,
    /// Значение не того вида: массив или объект там, где ждали скаляр.
    Shape(&'static str),
    /// Вид тот, а прочесть нельзя: текст в числовом поле, число за
    /// разрядностью типа, дробное в целочисленном.
    Unparsable,
    /// То же, но с подсказкой, что писать вместо присланного.
    ///
    /// Отдельно от [`Taken::Unparsable`], потому что «не прочлось» и «не
    /// прочлось, а надо было объявить поле иначе» — разные сообщения, и второе
    /// экономит день.
    Why(&'static str),
}

impl Taken {
    /// Текст для приёма. У всего, кроме годного значения, — пустая строка:
    /// приём принимает срез строк, и отсутствие выражается пустотой.
    #[cfg(test)]
    pub fn text(&self) -> &str {
        match self {
            Taken::Value(s) => s,
            _ => "",
        }
    }

    /// Забирает строку целиком, не копируя.
    ///
    /// Отдельно от [`Taken::text`], потому что разница не стилистическая:
    /// `text().to_string()` — это ещё одно выделение и ещё одно копирование
    /// содержимого **на каждую ячейку каждого документа**, а первая копия тут
    /// же выбрасывается. На ста миллионах документов по пять ячеек это
    /// полмиллиарда лишних пар выделение-освобождение и порядка пятнадцати
    /// гигабайт лишнего копирования; замерено — 128 нс на документ.
    pub fn into_text(self) -> String {
        match self {
            Taken::Value(s) => s,
            _ => String::new(),
        }
    }

    /// Стоит ли о нём сказать. `Absent` — нет: это законный случай.
    pub fn is_loss(&self) -> bool {
        matches!(self, Taken::Shape(_) | Taken::Unparsable | Taken::Why(_))
    }

    pub fn why(&self) -> &'static str {
        match self {
            Taken::Shape(what) => what,
            Taken::Unparsable => "значение не прочлось типом поля",
            Taken::Why(hint) => hint,
            _ => "",
        }
    }
}

/// Разбирает одну ячейку по объявленному типу поля.
pub fn take_cell(v: Option<&serde_json::Value>, kind: Kind) -> Taken {
    let Some(v) = v else { return Taken::Absent };
    if v.is_null() {
        return Taken::Absent;
    }
    // Векторные виды принимают ровно то, что скалярные отвергают, — массив и
    // объект. Их разбор был **только** в офлайновом пути, и сетевой на тех же
    // данных клал пустоту; ради этого расхождения модуль и заведён.
    if let Some(dims) = kind.dims() {
        return numbers(v, dims);
    }
    if kind.dot() {
        return pairs(v);
    }
    match v {
        serde_json::Value::Null => Taken::Absent,
        // Точка приходит и объектом `{"lat":…,"lon":…}`, и массивом
        // `[долгота, широта]` — так её пишет Elasticsearch. Порядок в массиве
        // обратный тому, что в строке, и знать об этом может только здесь, где
        // видно, чем значение пришло: догадываться о нём ниже было бы поздно.
        serde_json::Value::Object(m) if kind == Kind::Geo => match (m.get("lat"), m.get("lon")) {
            (Some(a), Some(o)) => match (a.as_f64(), o.as_f64()) {
                (Some(a), Some(o)) => check(format!("{a},{o}"), kind),
                _ => Taken::Unparsable,
            },
            _ => Taken::Shape("у точки ожидаются lat и lon"),
        },
        serde_json::Value::Array(a) if kind == Kind::Geo => {
            if a.len() != 2 {
                return Taken::Shape("точка массивом — это ровно [долгота, широта]");
            }
            match (a[0].as_f64(), a[1].as_f64()) {
                // Порядок именно такой: у Elasticsearch массив идёт
                // долгота-широта, а строка — широта-долгота. Перепутать их
                // легко, и ответ выйдет правдоподобным.
                (Some(lon), Some(lat)) => check(format!("{lat},{lon}"), kind),
                _ => Taken::Unparsable,
            }
        }
        serde_json::Value::Array(_) => Taken::Shape("массив там, где поле принимает одно значение"),
        serde_json::Value::Object(_) => {
            Taken::Shape("объект там, где поле принимает одно значение")
        }
        serde_json::Value::String(s) => check(s.clone(), kind),
        serde_json::Value::Bool(b) => check(b.to_string(), kind),
        serde_json::Value::Number(n) => {
            // Дробное в целочисленном поле — потеря, а не округление. Округлить
            // молча значило бы записать не то, что прислали, и разойтись с
            // источником навсегда.
            if matches!(kind, Kind::Int32 | Kind::Int64 | Kind::Date) && n.as_i64().is_none() {
                return Taken::Unparsable;
            }
            // Число печатается в строку и разбирается обратно — да, это круг.
            // Обойти его можно было бы, спросив `serde_json` о разрядности
            // напрямую, но тогда рядом с `Kind::parse_value` появилось бы
            // второе правило «влезает ли значение в тип», и разойтись им ничто
            // бы не мешало. Замерено: весь остаток `take_cell` сверх лишней
            // копии — 32 нс на документ, три секунды на ста миллионах. Три
            // секунды не стоят второго правила.
            check(n.to_string(), kind)
        }
    }
}

/// Плотный вектор: массив чисел либо строка вида `1,2,3` / `[1 2 3]`.
///
/// Строковая запись принимается не из снисходительности: её принимал приём и
/// до отчёта — `vector::parse` разбирает и скобки, и пробелы, — и отвергнуть её
/// сейчас значило бы сломать работающую заливку ради стройности.
///
/// Годность проверяется **тем же** `vector::parse`, что и приём, включая
/// размерность: вектор не той длины кладётся пустым и в выдачу не попадает
/// никогда, так что молчание про него — обещание поиска, которого не будет.
fn numbers(v: &serde_json::Value, dims: usize) -> Taken {
    let text = match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(a) => {
            let mut out = String::with_capacity(a.len() * 5);
            for x in a.iter() {
                let Some(n) = x.as_f64() else { return Taken::Unparsable };
                if !out.is_empty() {
                    out.push(',');
                }
                // Целое печатается целым: `1` и `1.0` разбираются одинаково, но
                // записи разной длины, а тысяча измерений — заметная разница.
                if n.fract() == 0.0 && n.abs() < 1e15 {
                    out.push_str(&(n as i64).to_string());
                } else {
                    out.push_str(&n.to_string());
                }
            }
            out
        }
        _ => return Taken::Shape("плотный вектор принимает массив чисел или их запись строкой"),
    };
    if text.trim().is_empty() {
        return Taken::Absent;
    }
    match svindb_core::vector::parse(&text, dims) {
        Some(p) if p.len() == dims => Taken::Value(text),
        _ => Taken::Unparsable,
    }
}

/// Разрежённый вектор: пары «имя — вес».
///
/// Строка проходит как есть: её разберёт тот же разбор, что и объект, и
/// расходиться двум записям незачем.
fn pairs(v: &serde_json::Value) -> Taken {
    match v {
        serde_json::Value::String(s) => Taken::Value(s.clone()),
        serde_json::Value::Object(m) => {
            let mut out = String::with_capacity(m.len() * 12);
            for (k, x) in m.iter() {
                let Some(w) = x.as_f64() else { return Taken::Unparsable };
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push('"');
                out.push_str(&k.replace('"', ""));
                out.push_str("\":");
                out.push_str(&w.to_string());
            }
            Taken::Value(out)
        }
        _ => Taken::Shape("разрежённый вектор принимает объект «имя: вес»"),
    }
}

/// Проверяет, что текст прочтётся тем же разбором, что и на приёме.
///
/// Именно тем же — `Kind::parse_value`, а не своим повторением: разойдись они,
/// и отчёт говорил бы одно, а индекс делал другое.
fn check(text: String, kind: Kind) -> Taken {
    // Двоичное значение проверяется на приёме, а не принимается как есть:
    // мусор, пришедший под видом Base64, лёг бы в индекс молча и обнаружился
    // бы только тогда, когда его попробовали бы прочесть обратно.
    if kind == Kind::Binary {
        return match svindb_core::geo::unb64(&text) {
            Some(_) => Taken::Value(text),
            None => Taken::Unparsable,
        };
    }
    // Точка проверяется тем же правилом, каким потом читается. Приходит она
    // строкой «широта,долгота» — объект и массив переводит в неё разбор
    // документа, который знает, откуда значение взялось.
    if kind == Kind::Geo {
        return match svindb_core::geo::Point::parse(&text) {
            Ok(_) => Taken::Value(text),
            Err(_) => Taken::Unparsable,
        };
    }
    // Дата без пометки единицы принимает только целое. Разбирать ISO здесь
    // было бы соблазнительно, но тогда поле, где уже лежат секунды эпохи,
    // получило бы значения, отличающиеся в тысячу раз, — и узнать об этом было
    // бы неоткуда. Подсказка называет, что писать.
    if kind == Kind::Date && text.trim().parse::<i64>().is_err() {
        return Taken::Why(
            "у поля date значение — целое; для строк ISO-8601 объявите поле как date:iso",
        );
    }
    if !kind.numeric() {
        return Taken::Value(text);
    }
    if text.trim().is_empty() {
        return Taken::Absent;
    }
    match kind.parse_value(&text) {
        Some(_) => Taken::Value(text),
        None => Taken::Unparsable,
    }
}

/// Сколько записей о потерях кладётся в ответ.
///
/// Остальное сворачивается в счётчик. На пачке в двадцать тысяч грязных
/// документов полный перечень был бы больше самого запроса, и читать его
/// никто бы не стал.
pub const REPORT_LIMIT: usize = 32;

/// Что не дошло до индекса.
#[derive(Default)]
pub struct Report {
    /// Сколько значений потеряно всего.
    pub skipped: usize,
    /// Сколько **документов** пострадало.
    ///
    /// Отдельно от `skipped`, потому что это разные числа: два негодных поля
    /// одного документа давали двойку, и по ней нельзя было понять, сколько
    /// записей задето.
    pub skipped_docs: usize,
    /// Идентификаторы пострадавших документов — по одному разу каждый.
    pub bad_ids: Vec<String>,
    /// Первые потери, с именем документа и поля.
    pub first: Vec<String>,
    /// Имя документа, о котором идёт речь сейчас, и было ли о нём уже сказано.
    at_doc: Option<(String, bool)>,
    /// Ключи, которых нет в схеме, — по одному разу каждый.
    pub unknown: Vec<String>,
    /// Сколько всего было незаявленных ключей, считая повторы.
    pub unknown_total: usize,
}

impl Report {
    /// Называет документ, к которому относятся следующие потери.
    ///
    /// По `id`, а не по номеру в пачке: на десяти тысячах сообщений «документ
    /// 4173» не помогает найти виноватую запись — клиенту пришлось бы держать
    /// у себя соответствие номера идентификатору. Документ без `id` называется
    /// номером и **говорит об этом**, чтобы «№3» не путали с идентификатором.
    pub fn at(&mut self, at: usize, id: &str, has_id: bool) {
        let name =
            if has_id && !id.is_empty() { id.to_string() } else { format!("№{at} (без id)") };
        self.at_doc = Some((name, false));
    }

    fn hurt(&mut self) -> String {
        match self.at_doc.as_mut() {
            Some((name, told)) => {
                let name = name.clone();
                if !*told {
                    *told = true;
                    self.skipped_docs += 1;
                    if self.bad_ids.len() < REPORT_LIMIT {
                        self.bad_ids.push(name.clone());
                    }
                }
                name
            }
            None => "документ".to_string(),
        }
    }

    pub fn note(&mut self, _at: usize, field: &str, t: &Taken) {
        self.skipped += 1;
        let why = t.why().to_string();
        let who = self.hurt();
        if self.first.len() < REPORT_LIMIT {
            self.first.push(format!("документ {who:?}, поле {field:?}: {why}"));
        }
    }

    /// Потеря со своим текстом — там, где причину знает не [`Taken`], а тот,
    /// кто спускался по пути документа.
    pub fn note_text(&mut self, _at: usize, field: &str, why: &str) {
        self.skipped += 1;
        let who = self.hurt();
        if self.first.len() < REPORT_LIMIT {
            self.first.push(format!("документ {who:?}, поле {field:?}: {why}"));
        }
    }

    pub fn unknown_key(&mut self, key: &str) {
        self.unknown_total += 1;
        if self.unknown.len() < REPORT_LIMIT && !self.unknown.iter().any(|k| k == key) {
            self.unknown.push(key.to_string());
        }
    }

    pub fn clean(&self) -> bool {
        self.skipped == 0 && self.unknown_total == 0
    }

    /// Приписка к ответу приёма. Пусто, когда всё дошло.
    pub fn suffix(&self, esc: impl Fn(&str) -> String) -> String {
        if self.clean() {
            return String::new();
        }
        let list = |v: &[String]| v.iter().map(|s| esc(s)).collect::<Vec<_>>().join(",");
        format!(
            ",\"skipped\":{},\"skipped_docs\":{},\"bad_ids\":[{}],\"skipped_first\":[{}],\
             \"unknown_fields\":[{}],\"unknown_total\":{}",
            self.skipped,
            self.skipped_docs,
            list(&self.bad_ids),
            list(&self.first),
            list(&self.unknown),
            self.unknown_total
        )
    }

    /// Причина отказа в строгом режиме.
    pub fn refusal(&self) -> String {
        let mut why = String::from("строгий приём: ");
        if self.skipped > 0 {
            why.push_str(&format!("значений не прочлось {}", self.skipped));
            if let Some(f) = self.first.first() {
                why.push_str(&format!(" (первое — {f})"));
            }
        }
        if self.unknown_total > 0 {
            if self.skipped > 0 {
                why.push_str("; ");
            }
            why.push_str(&format!(
                "ключей вне схемы {} ({})",
                self.unknown_total,
                self.unknown.join(", ")
            ));
        }
        why
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scalar_of_the_right_shape_goes_through() {
        assert_eq!(
            take_cell(Some(&serde_json::json!("привет")), Kind::Text),
            Taken::Value("привет".into())
        );
        assert_eq!(take_cell(Some(&serde_json::json!(7)), Kind::Int64), Taken::Value("7".into()));
        assert_eq!(
            take_cell(Some(&serde_json::json!(true)), Kind::Bool),
            Taken::Value("true".into())
        );
        assert_eq!(
            take_cell(Some(&serde_json::json!("1.5")), Kind::Double),
            Taken::Value("1.5".into())
        );
    }

    /// Отсутствие — законно и молчаливо, и это отдельный случай.
    ///
    /// Иначе пришлось бы заполнять все поля схемы, а у широких схем документ
    /// трогает три процента объявленного.
    #[test]
    fn absence_is_lawful_and_silent() {
        assert_eq!(take_cell(None, Kind::Text), Taken::Absent);
        assert_eq!(take_cell(Some(&serde_json::json!(null)), Kind::Int64), Taken::Absent);
        assert_eq!(take_cell(Some(&serde_json::json!("")), Kind::Int64), Taken::Absent);
        assert!(!Taken::Absent.is_loss());
    }

    /// Всё, что прежде молча становилось пустотой, теперь называется потерей.
    #[test]
    fn what_used_to_vanish_is_now_named() {
        let losses = [
            (serde_json::json!(["a", "b"]), Kind::Keyword),
            (serde_json::json!({"a": 1}), Kind::Text),
            (serde_json::json!("не число"), Kind::Int64),
            (serde_json::json!(1.5), Kind::Int64),
            (serde_json::json!("9999999999999"), Kind::Int32),
            (serde_json::json!("nan"), Kind::Double),
        ];
        for (v, kind) in losses {
            let t = take_cell(Some(&v), kind);
            assert!(t.is_loss(), "{v} в {kind:?} обязано быть потерей, а вышло {t:?}");
            assert_eq!(t.text(), "", "потеря отдаёт приёму пустоту");
            assert!(!t.why().is_empty(), "у потери обязана быть причина");
        }
    }

    /// Разбор числа сверяется с тем же `parse_value`, что и приём.
    ///
    /// Заведи здесь своё правило — и отчёт говорил бы одно, а индекс делал
    /// другое: значение, объявленное принятым, не попало бы в колонку.
    #[test]
    fn the_report_agrees_with_the_indexer() {
        for (text, kind) in [
            ("2147483648", Kind::Int32),
            ("9223372036854775808", Kind::Int64),
            ("да", Kind::Bool),
            ("inf", Kind::Float),
        ] {
            let t = take_cell(Some(&serde_json::json!(text)), kind);
            assert_eq!(
                kind.parse_value(text).is_none(),
                t.is_loss(),
                "{text} в {kind:?}: отчёт и приём разошлись"
            );
        }
    }

    /// Перечень ограничен, а счётчик — нет.
    #[test]
    fn the_report_is_bounded_but_the_count_is_not() {
        let mut r = Report::default();
        for i in 0..1000 {
            r.note(i, "цена", &Taken::Unparsable);
            r.unknown_key(&format!("чужое{i}"));
        }
        assert_eq!(r.skipped, 1000);
        assert_eq!(r.first.len(), REPORT_LIMIT);
        assert_eq!(r.unknown_total, 1000);
        assert_eq!(r.unknown.len(), REPORT_LIMIT);
        assert!(!r.clean());
        let s = r.suffix(|x| format!("\"{x}\""));
        assert!(s.contains("\"skipped\":1000"), "{s}");
        assert!(s.contains("\"unknown_total\":1000"), "{s}");
    }

    /// Повторяющийся незаявленный ключ называется один раз.
    #[test]
    fn a_repeated_unknown_key_is_named_once() {
        let mut r = Report::default();
        for _ in 0..50 {
            r.unknown_key("опечатка");
        }
        assert_eq!(r.unknown, vec!["опечатка".to_string()]);
        assert_eq!(r.unknown_total, 50);
    }

    /// Векторные виды разбираются здесь же, а не только в офлайновом пути.
    ///
    /// Прежде сетевой приём звал `cell_text`, который на массиве отдавал
    /// пустоту: один и тот же корпус давал индекс с векторами через `svindb
    /// index` и без них через `_bulk`.
    #[test]
    fn vectors_are_taken_by_both_paths_alike() {
        let dense = take_cell(Some(&serde_json::json!([1.0, -0.5, 2])), Kind::Dense(3));
        assert_eq!(dense, Taken::Value("1,-0.5,2".into()));
        // Строковая запись вектора принималась приёмом и до отчёта.
        assert_eq!(
            take_cell(Some(&serde_json::json!("[1 2 3]")), Kind::Dense(3)),
            Taken::Value("[1 2 3]".into())
        );
        let sparse = take_cell(Some(&serde_json::json!({"кот": 0.5})), Kind::Sparse);
        assert_eq!(sparse, Taken::Value("\"кот\":0.5".into()));
        // А негодное для вектора — названная потеря, не пустота.
        for (v, k) in [
            (serde_json::json!("не массив"), Kind::Dense(3)),
            (serde_json::json!(["текст"]), Kind::Dense(3)),
            // Не та размерность — молчаливая пустота была худшим случаем:
            // документ принят, вектор выброшен, kNN его не найдёт никогда.
            (serde_json::json!([1, 2]), Kind::Dense(3)),
            (serde_json::json!([1, 2, 3, 4]), Kind::Dense(3)),
            (serde_json::json!("1 2"), Kind::Dense(3)),
            (serde_json::json!([1.0, f64::MAX, 3.0]), Kind::Dense(3)),
            (serde_json::json!(7), Kind::Sparse),
            (serde_json::json!({"кот": "много"}), Kind::Sparse),
        ] {
            assert!(take_cell(Some(&v), k).is_loss(), "{v} в {k:?}");
        }
    }

    /// Чистая пачка не приписывает к ответу ничего.
    #[test]
    fn a_clean_batch_says_nothing_extra() {
        let r = Report::default();
        assert!(r.clean());
        assert_eq!(r.suffix(|x| x.to_string()), "");
    }
}
