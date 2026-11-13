//! Разбор и запись JSON — ровно столько, сколько нужно схеме индекса.

use std::fmt::Write as _;

/// Дерево разобранного JSON.
#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    /// Целое. Отдельно от дробного намеренно: номер поля и версия схемы обязаны
    /// пережить круг «запись — чтение» без потерь, а `f64` теряет точность
    /// после девяти квадриллионов.
    Int(i64),
    Num(f64),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
    /// Значение по ключу объекта. Не объект или ключа нет — `None`.
    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Obj(kv) => kv.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Целое. Дробное принимается, только если оно целое по значению: `4.0` —
    /// это четыре, `4.5` — нет.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Json::Int(v) => Some(*v),
            Json::Num(v) if v.fract() == 0.0 && v.abs() < 9.007_199_254_740_992e15 => {
                Some(*v as i64)
            }
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Json::Int(v) => Some(*v as f64),
            Json::Num(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Json::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Пары объекта в порядке записи. Не объект — `None`.
    ///
    /// Нужен разбору настроек: там важно пройти **все** ключи, а не спросить
    /// известные. Настройка с опечаткой, молча пропущенная, — худший вид
    /// поломки в файле, где половина ключей про безопасность: она выглядит
    /// применённой и не применена.
    pub fn as_obj(&self) -> Option<&[(String, Json)]> {
        match self {
            Json::Obj(kv) => Some(kv),
            _ => None,
        }
    }

    pub fn as_arr(&self) -> Option<&[Json]> {
        match self {
            Json::Arr(v) => Some(v),
            _ => None,
        }
    }

    /// Имя вида — для сообщений об ошибке разбора схемы.
    pub fn kind(&self) -> &'static str {
        match self {
            Json::Null => "null",
            Json::Bool(_) => "логическое",
            Json::Int(_) | Json::Num(_) => "число",
            Json::Str(_) => "строка",
            Json::Arr(_) => "массив",
            Json::Obj(_) => "объект",
        }
    }

    /// Запись в одну строку.
    pub fn to_compact(&self) -> String {
        let mut out = String::new();
        write_to(self, &mut out, None, 0);
        out
    }

    /// Запись с отступом в два пробела — схему читают люди.
    pub fn to_pretty(&self) -> String {
        let mut out = String::new();
        write_to(self, &mut out, Some(2), 0);
        out
    }
}

fn write_str(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            // Управляющие — только они и обязаны экранироваться. Всё
            // остальное, включая эмодзи, идёт как есть: файл в UTF-8, и
            // `\u`-последовательности сделали бы схему нечитаемой.
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn write_num(v: f64, out: &mut String) {
    // Бесконечности и `NaN` в JSON выразить нечем, и подменять их нулём значило
    // бы соврать. Схема таких чисел не содержит, но записать их молча нельзя.
    if v.is_finite() {
        let _ = write!(out, "{v}");
    } else {
        out.push_str("null");
    }
}

fn write_to(v: &Json, out: &mut String, indent: Option<usize>, depth: usize) {
    let pad = |out: &mut String, d: usize| {
        if let Some(step) = indent {
            out.push('\n');
            for _ in 0..d * step {
                out.push(' ');
            }
        }
    };
    match v {
        Json::Null => out.push_str("null"),
        Json::Bool(true) => out.push_str("true"),
        Json::Bool(false) => out.push_str("false"),
        Json::Int(n) => {
            let _ = write!(out, "{n}");
        }
        Json::Num(n) => write_num(*n, out),
        Json::Str(s) => write_str(s, out),
        Json::Arr(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push('[');
            for (i, it) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                pad(out, depth + 1);
                write_to(it, out, indent, depth + 1);
            }
            pad(out, depth);
            out.push(']');
        }
        Json::Obj(kv) => {
            if kv.is_empty() {
                out.push_str("{}");
                return;
            }
            out.push('{');
            for (i, (k, val)) in kv.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                pad(out, depth + 1);
                write_str(k, out);
                out.push(':');
                if indent.is_some() {
                    out.push(' ');
                }
                write_to(val, out, indent, depth + 1);
            }
            pad(out, depth);
            out.push('}');
        }
    }
}

/// Насколько глубоко разрешено вкладывать.
///
/// Разбор рекурсивный, и без предела вложенный на миллион уровней массив
/// уронил бы процесс по стеку. Схеме хватает четырёх уровней; сотня оставлена
/// с запасом на любое разумное употребление.
const MAX_DEPTH: usize = 100;

struct Parser<'a> {
    b: &'a [u8],
    at: usize,
}

/// Разбирает JSON целиком. Хвост после значения — ошибка.
pub fn parse(text: &str) -> Result<Json, String> {
    let mut p = Parser { b: text.as_bytes(), at: 0 };
    p.ws();
    let v = p.value(0)?;
    p.ws();
    if p.at != p.b.len() {
        return Err(p.err("после значения остался хвост"));
    }
    Ok(v)
}

impl<'a> Parser<'a> {
    fn err(&self, what: &str) -> String {
        format!("JSON, байт {}: {what}", self.at)
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.at).copied()
    }

    fn ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    fn lit(&mut self, s: &str) -> bool {
        if self.b[self.at..].starts_with(s.as_bytes()) {
            self.at += s.len();
            true
        } else {
            false
        }
    }

    fn value(&mut self, depth: usize) -> Result<Json, String> {
        if depth > MAX_DEPTH {
            return Err(self.err("слишком глубокая вложенность"));
        }
        match self.peek() {
            None => Err(self.err("значение оборвалось")),
            Some(b'n') if self.lit("null") => Ok(Json::Null),
            Some(b't') if self.lit("true") => Ok(Json::Bool(true)),
            Some(b'f') if self.lit("false") => Ok(Json::Bool(false)),
            Some(b'"') => self.string().map(Json::Str),
            Some(b'[') => self.array(depth),
            Some(b'{') => self.object(depth),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.number(),
            Some(c) => Err(self.err(&format!("неожиданный символ {:?}", c as char))),
        }
    }

    fn array(&mut self, depth: usize) -> Result<Json, String> {
        self.at += 1; // [
        let mut out = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.at += 1;
            return Ok(Json::Arr(out));
        }
        loop {
            self.ws();
            out.push(self.value(depth + 1)?);
            self.ws();
            match self.peek() {
                Some(b',') => self.at += 1,
                Some(b']') => {
                    self.at += 1;
                    return Ok(Json::Arr(out));
                }
                _ => return Err(self.err("в массиве ожидались «,» или «]»")),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<Json, String> {
        self.at += 1; // {
        let mut out: Vec<(String, Json)> = Vec::new();
        self.ws();
        if self.peek() == Some(b'}') {
            self.at += 1;
            return Ok(Json::Obj(out));
        }
        loop {
            self.ws();
            if self.peek() != Some(b'"') {
                return Err(self.err("ключом объекта обязана быть строка"));
            }
            let k = self.string()?;
            // Повтор ключа — не «последний побеждает», а отказ. В схеме это
            // означало бы два разных объявления одного поля, и выбор молчком
            // сделал бы формат неповторимым при перезаписи.
            if out.iter().any(|(prev, _)| *prev == k) {
                return Err(self.err(&format!("ключ {k:?} повторяется")));
            }
            self.ws();
            if self.peek() != Some(b':') {
                return Err(self.err("после ключа ожидалось «:»"));
            }
            self.at += 1;
            self.ws();
            let v = self.value(depth + 1)?;
            out.push((k, v));
            self.ws();
            match self.peek() {
                Some(b',') => self.at += 1,
                Some(b'}') => {
                    self.at += 1;
                    return Ok(Json::Obj(out));
                }
                _ => return Err(self.err("в объекте ожидались «,» или «}»")),
            }
        }
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let s = self.b.get(self.at..self.at + 4).ok_or_else(|| self.err("«\\u» оборвался"))?;
        let s = std::str::from_utf8(s).map_err(|_| self.err("«\\u» не шестнадцатеричный"))?;
        let v = u32::from_str_radix(s, 16).map_err(|_| self.err("«\\u» не шестнадцатеричный"))?;
        self.at += 4;
        Ok(v)
    }

    fn string(&mut self) -> Result<String, String> {
        self.at += 1; // "
        let mut out = String::new();
        loop {
            let c = self.peek().ok_or_else(|| self.err("строка не закрыта"))?;
            match c {
                b'"' => {
                    self.at += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.at += 1;
                    let e = self.peek().ok_or_else(|| self.err("экранирование оборвалось"))?;
                    self.at += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hi = self.hex4()?;
                            // Суррогатная пара: старшая половина сама по себе не
                            // символ, и склеить её надо здесь. Иначе эмодзи,
                            // записанное чужим кодировщиком, превратилось бы в
                            // замену — молча.
                            let ch = if (0xD800..0xDC00).contains(&hi) {
                                if !(self.peek() == Some(b'\\')
                                    && self.b.get(self.at + 1) == Some(&b'u'))
                                {
                                    return Err(self.err("старшая половина пары без младшей"));
                                }
                                self.at += 2;
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return Err(self.err("младшая половина пары негодная"));
                                }
                                0x1_0000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                            } else {
                                hi
                            };
                            match char::from_u32(ch) {
                                Some(c) => out.push(c),
                                None => return Err(self.err("«\\u» не даёт символа")),
                            }
                        }
                        _ => return Err(self.err("негодное экранирование")),
                    }
                }
                // Управляющие внутри строки запрещены стандартом, и пропустить
                // их значило бы принять то, что чужой разборщик отвергнет.
                c if c < 0x20 => return Err(self.err("управляющий символ в строке")),
                _ => {
                    // Строка пришла из `&str`, то есть UTF-8 верен по
                    // построению: достаточно перенести символ целиком.
                    let s = &self.b[self.at..];
                    let len = utf8_len(s[0]);
                    let piece = std::str::from_utf8(&s[..len.min(s.len())])
                        .map_err(|_| self.err("негодный UTF-8"))?;
                    out.push_str(piece);
                    self.at += len;
                }
            }
        }
    }

    fn number(&mut self) -> Result<Json, String> {
        let from = self.at;
        if self.peek() == Some(b'-') {
            self.at += 1;
        }
        // Ведущий ноль стандарт запрещает, и принять его нельзя: файл читают и
        // чужие разборщики, а они на `01` откажут. Формат, который наш писатель
        // выдаёт, а чужой читатель не берёт, — это не формат.
        if self.peek() == Some(b'0') {
            self.at += 1;
            if matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                return Err(self.err("у числа ведущий ноль"));
            }
        } else {
            let digits = self.at;
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.at += 1;
            }
            if self.at == digits {
                return Err(self.err("у числа нет ни одной цифры"));
            }
        }
        let mut integral = true;
        // И у дробной части, и у порядка обязана быть хотя бы одна цифра:
        // `1.` и `1e` стандарт не принимает.
        let digits = |p: &mut Parser<'a>, what: &str| -> Result<(), String> {
            let from = p.at;
            while matches!(p.peek(), Some(c) if c.is_ascii_digit()) {
                p.at += 1;
            }
            if p.at == from {
                return Err(p.err(what));
            }
            Ok(())
        };
        if self.peek() == Some(b'.') {
            integral = false;
            self.at += 1;
            digits(self, "у дробной части нет цифр")?;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            integral = false;
            self.at += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.at += 1;
            }
            digits(self, "у порядка нет цифр")?;
        }
        let s =
            std::str::from_utf8(&self.b[from..self.at]).map_err(|_| self.err("негодное число"))?;
        if integral {
            if let Ok(v) = s.parse::<i64>() {
                return Ok(Json::Int(v));
            }
        }
        s.parse::<f64>().map(Json::Num).map_err(|_| {
            self.at = from;
            self.err("негодное число")
        })
    }
}

/// Длина символа UTF-8 по первому байту.
fn utf8_len(b: u8) -> usize {
    match b {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

/// Собрать объект, не выписывая `Json::Obj(vec![...])` руками.
pub fn obj(kv: Vec<(&str, Json)>) -> Json {
    Json::Obj(kv.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

pub fn s(v: &str) -> Json {
    Json::Str(v.to_string())
}

pub fn i(v: i64) -> Json {
    Json::Int(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Круг «запись — чтение» обязан вернуть то же дерево.
    #[test]
    fn a_value_survives_the_round_trip() {
        let v = obj(vec![
            ("format", i(1)),
            ("dead", Json::Null),
            ("live", Json::Bool(true)),
            ("boost", Json::Num(2.5)),
            ("path", s("reactions.emoji")),
            ("ids", Json::Arr(vec![i(0), i(8), i(-3), i(i64::MAX), i(i64::MIN)])),
            ("nested", obj(vec![("a", Json::Arr(vec![]))])),
            ("empty", obj(vec![])),
        ]);
        for text in [v.to_compact(), v.to_pretty()] {
            assert_eq!(parse(&text).expect("разобралось"), v, "текст: {text}");
        }
    }

    /// Порядок ключей сохраняется: схему читают глазами и сравнивают `diff`-ом.
    #[test]
    fn key_order_is_kept() {
        let text = r#"{"z":1,"a":2,"m":3}"#;
        assert_eq!(parse(text).unwrap().to_compact(), text);
    }

    /// Целое не должно проходить через дробное.
    ///
    /// Номер поля живёт вечно и не переиспользуется; проехавший на единицу
    /// номер сцепил бы два разных поля в одно при слиянии.
    #[test]
    fn a_large_integer_keeps_every_digit() {
        let n = 9_007_199_254_740_993i64; // на единицу больше, чем держит f64
        let text = obj(vec![("id", i(n))]).to_compact();
        assert_eq!(text, format!("{{\"id\":{n}}}"));
        assert_eq!(parse(&text).unwrap().get("id").unwrap().as_i64(), Some(n));
    }

    /// Эмодзи переживает и прямую запись, и суррогатную пару чужого кодировщика.
    #[test]
    fn an_emoji_survives_both_spellings() {
        let direct = parse(r#"{"e":"👍"}"#).unwrap();
        let escaped = parse(r#"{"e":"\ud83d\udc4d"}"#).unwrap();
        assert_eq!(direct, escaped);
        assert_eq!(direct.get("e").unwrap().as_str(), Some("👍"));
        // И записывается как есть, а не парой: файл в UTF-8.
        assert_eq!(direct.to_compact(), "{\"e\":\"👍\"}");
    }

    /// Негодное отвергается, а не проглатывается.
    #[test]
    fn broken_input_is_refused() {
        let cases = [
            ("", "пусто"),
            ("{", "объект не закрыт"),
            ("[1,2", "массив не закрыт"),
            ("{\"a\":1,}", "запятая перед закрытием"),
            ("{a:1}", "ключ без кавычек"),
            ("{\"a\" 1}", "нет двоеточия"),
            ("\"abc", "строка не закрыта"),
            ("{\"a\":1}{\"b\":2}", "хвост после значения"),
            ("{\"a\":1,\"a\":2}", "ключ повторяется"),
            ("\"\\ud83d\"", "половина пары"),
            ("\"\\q\"", "негодное экранирование"),
            ("tru", "обрубленное true"),
            ("01", "ведущий ноль"),
            ("-", "минус без цифр"),
            ("[00]", "ведущий ноль в массиве"),
            ("1.", "точка без дробной части"),
        ];
        for (text, why) in cases {
            assert!(parse(text).is_err(), "{why}: {text:?} прошло, а не должно");
        }
    }

    /// Вложенность имеет предел: разбор рекурсивный.
    #[test]
    fn depth_is_bounded() {
        let deep = "[".repeat(MAX_DEPTH + 5) + &"]".repeat(MAX_DEPTH + 5);
        assert!(parse(&deep).is_err(), "слишком глубокое прошло");
        let ok = "[".repeat(10) + &"]".repeat(10);
        assert!(parse(&ok).is_ok(), "десять уровней обязаны разобраться");
    }

    /// Управляющий символ в строке — отказ, как того требует стандарт.
    #[test]
    fn a_control_character_is_refused_but_escaped_is_kept() {
        assert!(parse("\"a\nb\"").is_err());
        let v = parse(r#""a\nb""#).unwrap();
        assert_eq!(v.as_str(), Some("a\nb"));
        assert_eq!(v.to_compact(), r#""a\nb""#);
    }
}
