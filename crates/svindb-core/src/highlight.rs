//! Подсветка: кусок текста вокруг совпадения, с пометками.

use crate::analyze::is_word_char;

/// Пометки вокруг совпадения.
pub struct Tags<'a> {
    pub open: &'a str,
    pub close: &'a str,
}

impl Default for Tags<'_> {
    fn default() -> Self {
        Tags { open: "<em>", close: "</em>" }
    }
}

/// Слово текста: как оно выглядит после разбора и где лежит в исходной строке.
struct Span {
    word: String,
    at: usize,
    to: usize,
}

/// Разбирает текст, запоминая границы слов.
///
/// Границы — в **байтах** исходной строки, потому что резать её потом придётся
/// байтовыми срезами, и перевод из символов в байты на каждом разрезе стоил бы
/// прохода по строке.
fn spans(text: &str) -> Vec<Span> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut start = 0usize;
    for (i, c) in text.char_indices() {
        if is_word_char(c) {
            if cur.is_empty() {
                start = i;
            }
            cur.extend(c.to_lowercase());
        } else if !cur.is_empty() {
            out.push(Span { word: std::mem::take(&mut cur), at: start, to: i });
        }
    }
    if !cur.is_empty() {
        out.push(Span { word: cur, at: start, to: text.len() });
    }
    out
}

/// Позиции слов запроса в тексте — по списку на слово.
///
/// ## Зачем отдельно от [`spans`]
///
/// `out` — по списку на слово `words`, в том же порядке; каждый очищается.
/// Повторённое во фразе слово даёт два одинаковых списка — ровно то, чего ждёт
/// [`crate::phrase::count`].
///
/// `buf` — рабочая строка, переиспользуемая между вызовами. Она здесь ради
/// того же: выделение на слово превратило бы проверку в работу с кучей.
///
/// Возвращает число слов в тексте, то есть длину поля.
///
/// Разбор обязан совпадать с разбором приёма посимвольно, поэтому граница слова
/// берётся из [`crate::analyze::is_word_char`], а регистр сводится посимвольно —
/// как в [`crate::analyze::Simple`], где немецкая ß даёт две буквы.
pub fn locate(text: &str, words: &[String], out: &mut [Vec<u32>], buf: &mut String) -> u32 {
    locate_guarded(text, words, out, buf, None).unwrap_or(0)
}

/// Прерываемый разбор source для фразы без сохранённых positions.
pub fn locate_guarded(
    text: &str,
    words: &[String],
    out: &mut [Vec<u32>],
    buf: &mut String,
    guard: Option<&crate::work::Guard>,
) -> Option<u32> {
    debug_assert_eq!(words.len(), out.len(), "по списку на слово");
    for l in out.iter_mut() {
        l.clear();
    }
    buf.clear();
    let mut at = 0u32;
    let mut tick = 0;
    for c in text.chars() {
        if guard.is_some_and(|g| !g.go(&mut tick)) {
            out.iter_mut().for_each(Vec::clear);
            buf.clear();
            return None;
        }
        if is_word_char(c) {
            buf.extend(c.to_lowercase());
        } else if !buf.is_empty() {
            mark(buf, at, words, out);
            at += 1;
            buf.clear();
        }
    }
    if !buf.is_empty() {
        mark(buf, at, words, out);
        at += 1;
        buf.clear();
    }
    guard.is_none_or(|g| g.check()).then_some(at)
}

/// Отмечает слово текста во всех списках, которым оно принадлежит.
#[inline]
fn mark(word: &str, at: u32, words: &[String], out: &mut [Vec<u32>]) {
    for (i, w) in words.iter().enumerate() {
        if w.as_str() == word {
            out[i].push(at);
        }
    }
}

/// Кусок текста вокруг лучшего совпадения, с пометками.
///
/// `terms` — слова запроса в том же виде, в каком они попали в словарь.
/// `phrase` — если задана, подсвечиваются только её вхождения целиком, а не
/// отдельные слова: спросили фразу — показывать надо фразу.
///
/// `None` — ни одного совпадения в тексте. Это не край: документ мог совпасть
/// по другому полю, и врать пометкой на первом попавшемся слове нельзя.
pub fn snippet(
    text: &str,
    terms: &[String],
    phrase: Option<(&[String], u32)>,
    size: usize,
    tags: &Tags,
) -> Option<String> {
    let sp = spans(text);
    if sp.is_empty() {
        return None;
    }

    // Какие слова текста помечать.
    let mut mark = vec![false; sp.len()];
    let mut any = false;
    match phrase {
        Some((words, slop)) if words.len() >= 2 => {
            // Позиции каждого слова фразы — и то же сопоставление, что в поиске.
            // То же самое намеренно: подсветить надо ровно то, что нашлось, а
            // не то, что похоже.
            let lists: Vec<Vec<u32>> = words
                .iter()
                .map(|w| {
                    sp.iter()
                        .enumerate()
                        .filter(|(_, s)| &s.word == w)
                        .map(|(i, _)| i as u32)
                        .collect()
                })
                .collect();
            let refs: Vec<&[u32]> = lists.iter().map(|l| l.as_slice()).collect();
            if crate::phrase::count(&refs, slop) == 0 {
                return None;
            }
            // Помечаем окна: от вхождения первого слова до последнего.
            for &p0 in &lists[0] {
                let mut last = p0;
                let mut ok = true;
                for l in lists.iter().skip(1) {
                    match l.iter().find(|&&p| p > last) {
                        Some(&p) if p - p0 <= slop + (words.len() as u32 - 1) => last = p,
                        _ => {
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    for m in mark.iter_mut().take(last as usize + 1).skip(p0 as usize) {
                        *m = true;
                    }
                    any = true;
                }
            }
        }
        _ => {
            for (i, s) in sp.iter().enumerate() {
                if terms.iter().any(|t| t == &s.word) {
                    mark[i] = true;
                    any = true;
                }
            }
        }
    }
    if !any {
        return None;
    }

    // Окно вокруг первого помеченного слова: столько же слева и справа.
    let first = mark.iter().position(|&m| m).expect("помеченное есть");
    let centre = (sp[first].at + sp[first].to) / 2;
    let half = size / 2;
    let mut from = centre.saturating_sub(half);
    let mut to = (centre + half).min(text.len());
    // По границам слов, чтобы не резать посреди символа и посреди слова.
    while from > 0 && !text.is_char_boundary(from) {
        from -= 1;
    }
    while to < text.len() && !text.is_char_boundary(to) {
        to += 1;
    }
    if let Some(s) = sp.iter().find(|s| s.at >= from) {
        from = s.at;
    }
    if let Some(s) = sp.iter().rev().find(|s| s.to <= to) {
        to = s.to;
    }
    if from >= to {
        return None;
    }

    // Сборка: куски текста между помеченными словами идут как есть.
    let mut out = String::with_capacity(size + 32);
    if from > 0 {
        out.push('…');
    }
    let mut at = from;
    for (i, s) in sp.iter().enumerate() {
        if s.at < from || s.to > to || !mark[i] {
            continue;
        }
        out.push_str(&text[at..s.at]);
        out.push_str(tags.open);
        out.push_str(&text[s.at..s.to]);
        out.push_str(tags.close);
        at = s.to;
    }
    out.push_str(&text[at..to]);
    if to < text.len() {
        out.push('…');
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> Vec<String> {
        s.split_whitespace().map(|x| x.to_string()).collect()
    }

    /// Разбор подсветки обязан совпадать с разбором приёма посимвольно.
    #[test]
    fn the_split_matches_the_one_used_at_ingest() {
        use crate::analyze::{Analyzer, Simple};
        let cases = [
            "Красный квадрат, синий круг!",
            "ёжик 日本語 emoji 🐖 mixed_case",
            "[[United States|United States of America]]",
            "  ",
            "одно",
            "a1_b2 c-d e.f",
        ];
        let mut want = Vec::new();
        for c in cases {
            Simple.tokenize(c, &mut want);
            let got: Vec<String> = spans(c).into_iter().map(|s| s.word).collect();
            assert_eq!(got, want, "разбор разошёлся на {c:?}");
        }
    }

    /// Проверка фразы обязана видеть ровно те позиции, какие записал приём.
    ///
    /// Разойдись `locate` с разбором приёма хоть на одном символе — фраза
    /// найдётся там, где её нет, или потеряется там, где она есть, и заметить
    /// это будет нечем: обе стороны молчат. Поэтому сверяется не «работает», а
    /// каждая позиция каждого слова против самого разборщика.
    #[test]
    fn located_positions_are_the_ones_ingest_wrote() {
        use crate::analyze::{Analyzer, Simple};
        let cases = [
            "красный квадрат красный круг красный",
            "ёжик 日本語 emoji 🐖 mixed_case ёжик",
            "[[United States|United States of America]]",
            "İstanbul ÄÖÜ ß straße",
            "t1_476493 t1_476493 гост 12345-67",
            "  ",
            "одно",
        ];
        let mut toks = Vec::new();
        let mut buf = String::new();
        for c in cases {
            Simple.tokenize(c, &mut toks);
            // Слова берутся из самого текста, чтобы искалось то, что там есть.
            let words: Vec<String> = {
                let mut w = toks.clone();
                w.sort();
                w.dedup();
                w.push("несуществующее".into());
                w
            };
            let mut out = vec![Vec::new(); words.len()];
            let n = locate(c, &words, &mut out, &mut buf);
            assert_eq!(n as usize, toks.len(), "длина поля разошлась на {c:?}");
            for (i, w) in words.iter().enumerate() {
                let want: Vec<u32> = toks
                    .iter()
                    .enumerate()
                    .filter(|(_, t)| *t == w)
                    .map(|(k, _)| k as u32)
                    .collect();
                assert_eq!(out[i], want, "позиции {w:?} разошлись на {c:?}");
            }
        }
    }

    /// Повторённое во фразе слово обязано дать два одинаковых списка.
    ///
    /// Так устроен договор с `phrase::count`: «а а» — это два списка, а не один.
    #[test]
    fn a_word_repeated_in_the_phrase_gets_its_own_list() {
        let words = t("а а");
        let mut out = vec![Vec::new(); 2];
        let mut buf = String::new();
        locate("а а а", &words, &mut out, &mut buf);
        assert_eq!(out[0], vec![0, 1, 2]);
        assert_eq!(out[1], vec![0, 1, 2]);
        assert_eq!(crate::phrase::count(&[&out[0], &out[1]], 0), 2, "вхождений «а а»");
    }

    /// Буфер и списки переиспользуются — прошлый текст обязан исчезнуть.
    #[test]
    fn nothing_survives_from_the_previous_text() {
        let words = t("красный квадрат");
        let mut out = vec![Vec::new(); 2];
        let mut buf = String::new();
        locate("красный квадрат", &words, &mut out, &mut buf);
        locate("синий круг", &words, &mut out, &mut buf);
        assert!(out[0].is_empty() && out[1].is_empty(), "остались чужие позиции: {out:?}");
        assert!(buf.is_empty(), "в буфере остался хвост: {buf:?}");
    }

    /// Границы указывают на то самое слово, а не на соседнее.
    #[test]
    fn every_span_points_at_its_own_word() {
        let text = "Красный, квадрат! синий_круг 42";
        for s in spans(text) {
            let raw: String = text[s.at..s.to].chars().flat_map(|c| c.to_lowercase()).collect();
            assert_eq!(raw, s.word, "границы указали не на своё слово");
        }
    }

    #[test]
    fn a_word_is_marked_where_it_stands() {
        let text = "жил был красный квадрат и синий круг";
        let out = snippet(text, &t("квадрат"), None, 200, &Tags::default()).expect("кусок");
        assert!(out.contains("<em>квадрат</em>"), "не подсветилось: {out}");
        assert!(out.contains("красный"), "окружение потерялось: {out}");
    }

    /// Спросили фразу — подсвечивается фраза целиком, а не её слова порознь.
    #[test]
    fn a_phrase_is_marked_whole_not_word_by_word() {
        let text = "квадрат отдельно и красный квадрат вместе";
        let words = t("красный квадрат");
        let out = snippet(text, &words, Some((&words, 0)), 200, &Tags::default()).expect("кусок");
        assert!(out.contains("<em>красный</em> <em>квадрат</em>"), "фраза не целиком: {out}");
        // Одинокий «квадрат» в начале помечаться не должен.
        assert!(!out.starts_with("<em>квадрат</em>"), "помечено лишнее: {out}");
    }

    /// Нет совпадения — нет и куска: пометка на первом попавшемся слове была бы
    /// враньём.
    #[test]
    fn no_match_means_no_snippet() {
        let text = "жил был красный квадрат";
        assert!(snippet(text, &t("треугольник"), None, 200, &Tags::default()).is_none());
        let words = t("синий круг");
        assert!(snippet(text, &words, Some((&words, 0)), 200, &Tags::default()).is_none());
        assert!(snippet("", &t("а"), None, 200, &Tags::default()).is_none());
    }

    /// Резать текст можно только по границам символов, иначе строка не строка.
    #[test]
    fn cutting_never_lands_inside_a_character() {
        let text = "ёжик пошёл гулять красный квадрат ёжик вернулся домой очень длинный хвост";
        for size in 1..80 {
            if let Some(out) = snippet(text, &t("квадрат"), None, size, &Tags::default()) {
                assert!(out.contains("квадрат"), "потерялось совпадение при size={size}");
            }
        }
    }
}
