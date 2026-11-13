//! Разбор текста на слова, которыми реально ищут.

/// Разбор текста на слова.
///
/// Трейт, а не функция, чтобы морфология подключалась статически: разбор идёт
/// по каждому полю каждого документа, и виртуальный вызов на этом пути был бы
/// заметен.
pub trait Analyzer {
    /// Разбирает `text`, складывая слова в `out`. `out` очищается.
    fn tokenize(&self, text: &str, out: &mut Vec<String>);

    /// То же, но в плоский буфер: байты подряд и границы.
    ///
    /// Умолчание проходит через `Vec<String>` — ради тех разборщиков, которым
    /// плоский вид неудобен. Разборщик, который зовут на каждом документе,
    /// обязан переопределить это: в умолчании остаётся ровно то выделение на
    /// каждое слово, ради снятия которого плоский вид и заведён.
    fn tokenize_flat(&self, text: &str, out: &mut Tokens) {
        let mut tmp = Vec::new();
        self.tokenize(text, &mut tmp);
        out.clear();
        for w in tmp.iter() {
            out.push(w);
        }
    }
}

/// Слова поля плоским буфером.
///
/// ## Зачем не `Vec<String>`
///
/// `Vec<String>` требует объекта кучи на **каждое** слово: у документа в
/// полсотни слов это полсотни обращений к распределителю на документ, и они же
/// освобождаются следом. Замер: разбор слов стоил 6.5–7.0 мкс на документ —
/// больше, чем кодирование журнала и разбор JSON вместе взятые.
///
/// Здесь слово не объект, а отрезок общего буфера. Буфер живёт от документа к
/// документу и растёт до размера самого длинного поля, после чего перестаёт
/// обращаться к распределителю вовсе.
///
/// ## Почему границы, а не длины
///
/// Конец слова — это начало следующего, и по концам обе величины получаются
/// вычитанием. Хранить длины значило бы складывать их всякий раз, когда нужен
/// отрезок.
#[derive(Clone, Debug, Default)]
pub struct Tokens {
    bytes: Vec<u8>,
    ends: Vec<u32>,
}

impl Tokens {
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.ends.clear();
    }

    pub fn push(&mut self, w: &str) {
        self.bytes.extend_from_slice(w.as_bytes());
        self.ends.push(self.bytes.len() as u32);
    }

    /// Начинает слово, которое дописывается посимвольно.
    pub fn open(&mut self) -> usize {
        self.bytes.len()
    }

    /// Дописывает символ в открытое слово.
    pub fn put(&mut self, c: char) {
        let mut buf = [0u8; 4];
        self.bytes.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
    }

    /// Закрывает открытое слово. Пустое не запоминается.
    pub fn close(&mut self, from: usize) {
        if self.bytes.len() > from {
            self.ends.push(self.bytes.len() as u32);
        }
    }

    pub fn len(&self) -> usize {
        self.ends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// Байты слова по номеру.
    #[inline(always)]
    pub fn get(&self, i: usize) -> &[u8] {
        let to = self.ends[i] as usize;
        let from = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        &self.bytes[from..to]
    }

    /// Дописывает слова другого буфера в конец этого.
    pub fn append(&mut self, other: &mut Tokens) {
        let base = self.bytes.len() as u32;
        self.bytes.extend_from_slice(&other.bytes);
        self.ends.extend(other.ends.iter().map(|e| e + base));
        other.clear();
    }

    /// Слова по порядку — для проверок и для тех, кому нужен `&str`.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.len()).map(|i| self.get(i))
    }
}

/// Разбор по не-буквам с приведением регистра. Подчёркивание — часть слова.
#[derive(Clone, Copy, Debug, Default)]
pub struct Simple;

impl Analyzer for Simple {
    /// Плоский разбор: ни одного выделения на слово.
    ///
    /// Тот же разбор, что и ниже, слово в слово: по не-буквам, с приведением
    /// регистра посимвольно. Различие только в том, куда ложится результат.
    fn tokenize_flat(&self, text: &str, out: &mut Tokens) {
        out.clear();
        let mut at = out.open();
        let mut open = false;
        for c in text.chars() {
            if is_word_char(c) {
                if !open {
                    at = out.open();
                    open = true;
                }
                // `to_lowercase` посимвольно, а не по строке: немецкая ß даёт
                // две буквы, турецкая İ — две, и склейка через `char` их не
                // теряет.
                for l in c.to_lowercase() {
                    out.put(l);
                }
            } else if open {
                out.close(at);
                open = false;
            }
        }
        if open {
            out.close(at);
        }
    }

    fn tokenize(&self, text: &str, out: &mut Vec<String>) {
        out.clear();
        let mut cur = String::new();
        for c in text.chars() {
            if is_word_char(c) {
                // `to_lowercase` посимвольно, а не по строке: немецкая ß даёт
                // две буквы, турецкая İ — две, и склейка через `char` их не
                // теряет.
                cur.extend(c.to_lowercase());
            } else if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
        }
        if !cur.is_empty() {
            out.push(cur);
        }
    }
}

/// Входит ли символ в слово.
#[inline(always)]
pub fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(text: &str) -> Vec<String> {
        let mut out = Vec::new();
        Simple.tokenize(text, &mut out);
        out
    }

    /// Плоский разбор обязан дать те же слова, что и обычный.
    ///
    /// Не «похожие», а те же: плоский вид заведён ради выделений, а не ради
    /// другого разбора. Разойдись они хоть на одном входе — и сегменты,
    /// собранные разными путями, перестали бы совпадать.
    #[test]
    fn the_flat_split_says_the_same_words() {
        let long = "многобукв ".repeat(40);
        let cases = [
            "Привет, Мир!",
            "Hello   World",
            "снег_и_лёд",
            "ÄPFEL Straße ß İstanbul",
            "",
            "   ",
            "a1 b2_c3 -- d",
            long.as_str(),
        ];
        for text in cases {
            let mut want = Vec::new();
            Simple.tokenize(text, &mut want);
            let mut got = Tokens::default();
            Simple.tokenize_flat(text, &mut got);
            let flat: Vec<String> =
                got.iter().map(|b| String::from_utf8(b.to_vec()).expect("utf-8")).collect();
            assert_eq!(flat, want, "разбор разошёлся на {text:?}");
        }
    }

    #[test]
    fn splits_on_non_letters_and_folds_case() {
        assert_eq!(toks("Привет, Мир!"), vec!["привет", "мир"]);
        assert_eq!(toks("Hello   World"), vec!["hello", "world"]);
    }

    /// То самое подчёркивание: один токен, как у Elasticsearch.
    #[test]
    fn underscore_joins_a_word() {
        assert_eq!(toks("t1_476493"), vec!["t1_476493"]);
        assert_eq!(toks("t0_1 t1_2"), vec!["t0_1", "t1_2"]);
    }

    #[test]
    fn typographic_punctuation_is_a_separator() {
        assert_eq!(toks("фирма «Заря» работает"), vec!["фирма", "заря", "работает"]);
        assert_eq!(toks("тире — и дефис-слово"), vec!["тире", "и", "дефис", "слово"]);
    }

    #[test]
    fn digits_are_part_of_words() {
        assert_eq!(toks("ГОСТ 12345-67"), vec!["гост", "12345", "67"]);
        assert_eq!(toks("wi-fi 5ghz"), vec!["wi", "fi", "5ghz"]);
    }

    #[test]
    fn edges_and_emptiness() {
        assert_eq!(toks(""), Vec::<String>::new());
        assert_eq!(toks("!!!"), Vec::<String>::new());
        assert_eq!(toks("  слово  "), vec!["слово"]);
        assert_eq!(toks("слово"), vec!["слово"]);
    }

    /// Буфер переиспользуется — прошлые слова обязаны исчезнуть.
    #[test]
    fn buffer_is_cleared_between_calls() {
        let mut out = Vec::new();
        Simple.tokenize("первый второй", &mut out);
        Simple.tokenize("третий", &mut out);
        assert_eq!(out, vec!["третий"]);
    }

    /// Астральные плоскости не рвутся посередине символа.
    #[test]
    fn astral_characters_survive() {
        assert_eq!(toks("𝟙𝟚𝟛"), vec!["𝟙𝟚𝟛"]);
        assert_eq!(toks("a😀b"), vec!["a", "b"]);
    }

    /// Регистр сводится и там, где буква раскладывается в две.
    #[test]
    fn case_folding_may_expand() {
        assert_eq!(toks("İstanbul").len(), 1);
        assert_eq!(toks("ÄÖÜ"), vec!["äöü"]);
    }
}
