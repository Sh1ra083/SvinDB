//! ## Почему отдельного хранилища здесь нет

pub const WMIN: f64 = 0.001;
/// Наибольший вес. Выше — прижимается: обрезать верх безопаснее, чем растянуть
/// на него шкалу и потерять точность у всех остальных.
pub const WMAX: f64 = 16.0;

/// Кодов на диапазон.
const CODES: u32 = 254;

/// Шаг шкалы в процентах — во сколько раз соседние коды отличаются.
pub const STEP_PERCENT: f64 = 3.9;

/// Таблица разжатия: код в вес. Считается один раз, а не на каждый постинг.
///
/// На каждый — это `exp` и умножение на всякий оценённый документ всякого
/// курсора, то есть десятки тысяч вызовов на запрос. Тем же соображением
/// заведена [`crate::norms::LEVELS`].
static WEIGHTS: std::sync::LazyLock<[f32; 256]> = std::sync::LazyLock::new(|| {
    let mut t = [0f32; 256];
    let ratio = (WMAX / WMIN).ln() / f64::from(CODES - 1);
    for (c, x) in t.iter_mut().enumerate().skip(1) {
        *x = (WMIN * (ratio * f64::from(c as u32 - 1)).exp()) as f32;
    }
    t
});

pub fn encode(w: f64) -> u8 {
    // `NaN` не сравним ни с чем, поэтому проверка идёт «меньше или несравним»
    // явно: обычное `w < WMIN` пропустило бы `NaN` дальше по шкале.
    if w.partial_cmp(&WMIN).is_none_or(|o| o == std::cmp::Ordering::Less) {
        return 0;
    }
    let ratio = (WMAX / WMIN).ln() / f64::from(CODES - 1);
    let c = (w.min(WMAX) / WMIN).ln() / ratio;
    // Округление к ближайшему, а не вниз: шаг здесь относительный, и
    // систематический сдвиг вниз занизил бы каждое слагаемое произведения.
    (c.round() as u32 + 1).min(CODES) as u8
}

/// Код в вес. Ноль даёт ноль.
#[inline(always)]
pub fn decode(code: u8) -> f32 {
    WEIGHTS[code as usize]
}

/// Разбирает разреженный вектор из текста документа.
///
/// Принимает две записи одного и того же:
///
/// - пары через пробел или запятую — `слово:вес слово:вес`;
/// - объект JSON `{"слово": вес, …}` — в том виде, в каком его принимает
///   Elasticsearch.
///
/// `None` — значение не разобралось. Разбор здесь **строгий**, и это не
/// придирчивость: молча принятая половина вектора даёт правдоподобную выдачу по
/// неправильным данным, а отвергнутый документ виден сразу.
///
/// Пустая строка даёт пустой вектор: поле, заполненное не у всех документов, —
/// обычное дело, а не ошибка.
pub fn parse(text: &str) -> Option<Vec<(String, f64)>> {
    let t = text.trim();
    if t.is_empty() {
        return Some(Vec::new());
    }
    let body = match (t.starts_with('{'), t.ends_with('}')) {
        (true, true) => &t[1..t.len() - 1],
        (true, false) | (false, true) => return None,
        (false, false) => t,
    };

    let mut seen: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
    let mut order: Vec<String> = Vec::new();
    let mut chars = body.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace() || *c == ',') {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }
        let mut key = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            let mut closed = false;
            for c in chars.by_ref() {
                if c == '"' {
                    closed = true;
                    break;
                }
                key.push(c);
            }
            if !closed {
                return None;
            }
            while chars.peek().is_some_and(|c| c.is_whitespace()) {
                chars.next();
            }
        } else {
            let mut raw = String::new();
            while let Some(&c) = chars.peek() {
                if c == ',' || c.is_whitespace() {
                    break;
                }
                raw.push(c);
                chars.next();
            }
            let (k, v) = raw.rsplit_once(':')?;
            let w: f64 = v.trim().parse().ok()?;
            if !take(&mut seen, &mut order, k, w) {
                return None;
            }
            continue;
        }
        if chars.next() != Some(':') {
            return None;
        }
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let mut num = String::new();
        while let Some(&c) = chars.peek() {
            if c == ',' || c.is_whitespace() || c == '}' {
                break;
            }
            num.push(c);
            chars.next();
        }
        let w: f64 = num.trim().parse().ok()?;
        if !take(&mut seen, &mut order, &key, w) {
            return None;
        }
    }
    Some(order.into_iter().map(|k| (k.clone(), seen[&k])).collect())
}

/// Кладёт пару, схлопывая повтор наибольшим. `false` — пара не годится.
fn take(
    seen: &mut std::collections::HashMap<String, f64>,
    order: &mut Vec<String>,
    key: &str,
    w: f64,
) -> bool {
    let k = key.trim().trim_matches('"').to_lowercase();
    if k.is_empty() || !w.is_finite() {
        return false;
    }
    match seen.entry(k.clone()) {
        std::collections::hash_map::Entry::Occupied(mut e) => {
            let cur = *e.get();
            e.insert(cur.max(w));
        }
        std::collections::hash_map::Entry::Vacant(e) => {
            e.insert(w);
            order.push(k);
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Разжатие возвращает вес с обещанной относительной точностью.
    ///
    /// Проверяется именно относительная: у логарифмической шкалы абсолютная
    /// ошибка растёт вместе с весом, и мерить её значило бы мерить не то
    /// свойство, ради которого шкала выбрана.
    #[test]
    fn the_codec_keeps_its_relative_accuracy() {
        let mut w = WMIN;
        let mut worst = 0f64;
        while w <= WMAX {
            let back = f64::from(decode(encode(w)));
            let err = (back - w).abs() / w;
            worst = worst.max(err);
            w *= 1.01;
        }
        // Половина шага — то, что даёт округление к ближайшему.
        let allow = STEP_PERCENT / 200.0 * 1.05;
        assert!(
            worst <= allow,
            "худшая ошибка {:.4}% при пороге {:.4}%",
            worst * 100.0,
            allow * 100.0
        );
    }

    /// Шкала монотонна, и края ведут себя как объявлено.
    #[test]
    fn the_scale_only_goes_up() {
        for c in 1..254u8 {
            assert!(
                decode(c) < decode(c + 1),
                "код {c}: {} не меньше {}",
                decode(c),
                decode(c + 1)
            );
        }
        assert_eq!(decode(0), 0.0, "ноль обязан означать отсутствие");
        assert_eq!(encode(0.0), 0);
        assert_eq!(encode(-1.0), 0, "отрицательный вес принят");
        assert_eq!(encode(f64::NAN), 0, "NaN принят");
        assert_eq!(encode(WMIN), 1);
        assert_eq!(encode(WMAX), 254);
        // Выше потолка прижимается, а не переполняется.
        assert_eq!(encode(WMAX * 1000.0), 254);
        // И ни один вес не даёт 255: этот байт занят меткой переполнения
        // частоты, и постинг с ним увёл бы приём в отказ. Проверяется по всей
        // шкале, а не только на краю.
        let mut w = WMIN / 4.0;
        while w < WMAX * 4.0 {
            assert_ne!(encode(w), 255, "вес {w} дал метку переполнения");
            w *= 1.003;
        }
        assert_ne!(encode(f64::INFINITY), 255);
        assert_eq!(encode(WMIN / 2.0), 0);
    }

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-sp-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        d
    }

    /// Выдача совпадает с честным скалярным произведением по исходным весам.
    ///
    /// Не «похожа»: список строится через словарь, списки, отсечение MaxScore и
    /// сведение сегментов — то есть через всё сразу, — и обязан дать ту же
    /// десятку, что даёт прямой перебор по несжатым векторам. Расхождение здесь
    /// означало бы либо потолок ниже настоящего вклада (отсечение выбросило
    /// лишнее), либо ошибку в кодеке, и поймать это иначе нечем.
    #[test]
    fn the_answer_matches_an_honest_dot_product() {
        use crate::index::Kind;
        let dir = tmp("dot");
        let dims: Vec<String> = (0..200).map(|i| format!("d{i}")).collect();
        let mut s = 0x51de_1234u64;
        let mut next = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let docs: Vec<Vec<(usize, f64)>> = (0..2000)
            .map(|_| {
                let n = 5 + (next() % 20) as usize;
                (0..n)
                    .map(|_| ((next() % 200) as usize, (next() % 4000) as f64 / 1000.0 + 0.01))
                    .collect()
            })
            .collect();

        // Три сегмента, чтобы сведение и общая статистика тоже участвовали.
        let mut ix = crate::indexer::Indexer::typed_stored(
            &dir,
            vec!["s".into()],
            vec![1.0],
            vec![Kind::Sparse],
            vec![false],
            700,
        );
        for (i, d) in docs.iter().enumerate() {
            let text: Vec<String> = d.iter().map(|(k, w)| format!("{}:{w}", dims[*k])).collect();
            ix.add(&i.to_string(), &[&text.join(" ")]);
        }
        ix.flush_now();

        let segs = crate::segments::Segments::load(&dir).expect("открытие");
        let mut answers: Vec<Vec<u64>> = Vec::new();
        for round in 0..5 {
            let qn = 3 + round * 2;
            let query: Vec<(usize, f64)> =
                (0..qn).map(|j| ((round * 7 + j * 13) % 200, 0.5 + j as f64 * 0.3)).collect();
            let mut spec = crate::segments::QuerySpec { k: 10, ..Default::default() };
            spec.words = query
                .iter()
                .map(|(k, w)| crate::segments::SpecWord {
                    fid: 0,
                    terms: vec![crate::segments::Term { text: dims[*k].clone(), prefix: false }],
                    given: Some(*w),
                })
                .collect();
            let got: Vec<u64> = segs.search(&spec).hits.iter().map(|h| h.doc).collect();

            // Честное произведение по ТЕМ ЖЕ сжатым весам: кодек здесь не
            // проверяется, он проверен отдельно. Проверяется путь поиска.
            let mut want: Vec<(u32, f64)> = docs
                .iter()
                .enumerate()
                .map(|(i, d)| {
                    let s: f64 = query
                        .iter()
                        .map(|(qk, qw)| {
                            d.iter()
                                .filter(|(dk, _)| dk == qk)
                                .map(|(_, dw)| qw * f64::from(decode(encode(*dw))))
                                .fold(0f64, f64::max)
                        })
                        .sum();
                    (i as u32, s)
                })
                .filter(|x| x.1 > 0.0)
                .collect();
            want.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            let want: Vec<u64> = want.iter().take(10).map(|x| x.0 as u64).collect();
            assert_eq!(got, want, "заход {round}: выдача разошлась с произведением");
            answers.push(want);
        }

        let out = tmp("dot-merged");
        std::fs::create_dir_all(&out).ok();
        let refs: Vec<&crate::index::Index> = segs.list.iter().map(|x| x.as_ref()).collect();
        crate::merge::merge(&refs, &out).expect("слияние");
        drop(segs);
        let merged = crate::index::load(&out).expect("слитый сегмент");
        let one = crate::segments::Segments::from_parts(vec![std::sync::Arc::new(merged)], vec![0]);
        for (round, want) in answers.iter().enumerate() {
            let qn = 3 + round * 2;
            let query: Vec<(usize, f64)> =
                (0..qn).map(|j| ((round * 7 + j * 13) % 200, 0.5 + j as f64 * 0.3)).collect();
            let mut spec = crate::segments::QuerySpec { k: 10, ..Default::default() };
            spec.words = query
                .iter()
                .map(|(k, w)| crate::segments::SpecWord {
                    fid: 0,
                    terms: vec![crate::segments::Term { text: dims[*k].clone(), prefix: false }],
                    given: Some(*w),
                })
                .collect();
            let got: Vec<u64> = one.search(&spec).hits.iter().map(|h| h.doc).collect();
            assert_eq!(&got, want, "заход {round}: слияние изменило выдачу");
        }
        for x in [dir, out] {
            std::fs::remove_dir_all(&x).ok();
        }
    }

    /// Разбор принимает обе записи и разрешает повторы наибольшим.
    #[test]
    fn parsing_takes_both_shapes() {
        let a = parse("кот:1.5 пёс:0.25").expect("пары");
        assert_eq!(a, vec![("кот".to_string(), 1.5), ("пёс".to_string(), 0.25)]);
        let b = parse(r#"{"кот": 1.5, "пёс": 0.25}"#).expect("объект");
        assert_eq!(a, b, "две записи одного вектора разошлись");
        // Повтор — наибольший, а не последний и не сумма.
        assert_eq!(parse("кот:0.5 кот:2.0 кот:1.0").expect("повтор")[0].1, 2.0);
        let dup = parse("Кот:0.5 КОТ:2.0 кОт:1.0").expect("регистр");
        assert_eq!(dup.len(), 1, "разный регистр дал {} измерений: {dup:?}", dup.len());
        assert_eq!(dup[0], ("кот".to_string(), 2.0));
        let m = parse(r#"{"Dog": 0.5, "dog": 1.5, "cat_1": 0.25}"#).expect("модель");
        assert_eq!(m.len(), 2);
        assert_eq!(m[0], ("dog".to_string(), 1.5));
        assert_eq!(parse("").expect("пусто"), vec![]);
        assert!(parse("кот").is_none(), "пара без веса принята");
        assert!(parse("{кот:1").is_none(), "незакрытая скобка принята");
        assert!(parse("кот:1}").is_none(), "лишняя скобка принята");
        // Ключ с пробелом внутри кавычек — законное имя, а не развал вектора.
        let sp = parse(r#"{"два слова": 1.5, "b": 0.5}"#).expect("ключ с пробелом");
        assert_eq!(sp.len(), 2);
        assert_eq!(sp[0], ("два слова".to_string(), 1.5));
        // Незакрытая кавычка — отказ.
        assert!(parse(r#"{"кот: 1.5}"#).is_none(), "незакрытая кавычка принята");
        assert!(parse("кот:inf").is_none(), "бесконечность принята");
        assert!(parse("кот:1e400").is_none(), "переполнение принято");
        assert!(parse("кот:тут").is_none(), "вес не числом принят");
        assert!(parse("кот:nan").is_none(), "NaN принят");
    }

    #[test]
    fn parsing_does_not_go_quadratic() {
        let make = |n: usize| (0..n).map(|i| format!("d{i}:0.5")).collect::<Vec<_>>().join(" ");
        let time = |text: &str, runs: usize| {
            let t = std::time::Instant::now();
            for _ in 0..runs {
                std::hint::black_box(parse(text));
            }
            t.elapsed().as_secs_f64() / runs as f64
        };
        let gap = format!("a:1{}b:2", " ".repeat(200_000));
        let t = std::time::Instant::now();
        assert_eq!(parse(&gap).expect("пробелы").len(), 2);
        assert!(
            t.elapsed().as_millis() < 500,
            "двести тысяч пробелов разбирались {} мс",
            t.elapsed().as_millis()
        );

        let (small, big) = (make(100), make(1600));
        // Прогрев: первый заход платит за раскладку кучи.
        time(&small, 50);
        time(&big, 5);
        let a = time(&small, 200);
        let b = time(&big, 20);
        // Шестнадцатикратный вход при линейном разборе идёт примерно вшестнадцать
        // раз дольше, при квадратичном — в двести пятьдесят шесть. Порог с
        // большим запасом: тест ловит порядок, а не проценты.
        let ratio = b / a.max(1e-12);
        assert!(ratio < 60.0, "рост в {ratio:.1} раза при шестнадцатикратном входе");
    }

    /// Скалярное произведение через кодек близко к точному.
    ///
    /// Это и есть то, ради чего кодек существует, — и проверять надо его, а не
    /// точность отдельного веса: ошибки слагаемых частью гасят друг друга, и
    /// произведение выходит точнее худшего из них.
    #[test]
    fn the_dot_product_survives_the_codec() {
        let mut s = 0x2222_3333u64;
        let mut next = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let mut worst = 0f64;
        for _ in 0..200 {
            let n = 20 + (next() % 60) as usize;
            let (mut exact, mut coded) = (0f64, 0f64);
            for _ in 0..n {
                let q = (next() % 5000) as f64 / 1000.0 + 0.01;
                let d = (next() % 5000) as f64 / 1000.0 + 0.01;
                exact += q * d;
                // Запрос не сжимается — он приходит полным, как и у плотного
                // вектора. Сжат только документ.
                coded += q * f64::from(decode(encode(d)));
            }
            worst = worst.max((coded - exact).abs() / exact);
        }
        assert!(worst <= 0.01, "худшая ошибка произведения {:.3}%", worst * 100.0);
    }
}
