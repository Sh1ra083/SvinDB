//! Шарды: несколько индексов как один, в одном процессе.
//!
//! ## Зачем это вообще нужно
//!
//! **Не ради потолка по номерам.** Общий номер документа — восемь байт, и один
//! индекс держит `2^53 − 1` документов ([`crate::segments::MAX_DOCS`]), то есть
//! девять квадриллионов. Сто миллиардов туда влезают с запасом в девяносто
//! тысяч раз, и шарды для этого не нужны.
//!
//! Нужны они ради трёх других вещей, и каждая упирается в железо, а не в тип:
//!
//! 1. **Записывающий поток один на индекс.** Приём идёт под замком буфера, и
//!    сегменты пишет один писатель. Два шарда — два независимых писателя.
//! 2. **Отображений в памяти конечное число.** Сегмент стоит 15–17 отображений,
//!    а `vm.max_map_count` по умолчанию 65530. Один процесс упирается в это
//!    раньше, чем в номера.
//! 3. **Машина одна.** Сто миллиардов документов — это терабайты, которых на
//!    одном диске не бывает.
//!
//! Шард здесь — это отдельный каталог со своим индексом, своим списком сегментов
//! и своей нумерацией.
//!
//! ## Почему сначала в одном процессе, а не по сети
//!
//! Потому что тогда скоры остаются **точными**, и это не мелочь.
//!
//! Обратная частота обязана считаться по всему, что участвует в запросе. Пока
//! каждый шард считает её по своим `N` и `df`, вес перестаёт быть функцией
//! документа и запроса и становится функцией того, как раскладка распределила
//! документы: один и тот же документ по одному и тому же запросу получает разный
//! вес в зависимости от того, в какой шард он попал. Elasticsearch живёт с этим
//! по умолчанию и предлагает `dfs_query_then_fetch` тем, кому нужна точность, —
//! то есть лишний круг по сети.
//!
//! В одном процессе круга не нужно: [`Segments::stats`] отдаёт `N` и `df` шарда,
//! они складываются, и сложенное отдаётся обратно каждому шарду через
//! [`Segments::search_with_g`]. Скоры получаются те же, что у одного индекса на
//! всём корпусе.
//!
//! ## Раскладка по шардам
//!
//! Хеш идентификатора по остатку от числа шардов. Хеш — [`crate::bloom::hash`],
//! тот же, что у фильтра: FNV-1a с финализатором splitmix64, детерминированный,
//! без сида, одинаковый на всех платформах. Последнее важно: раскладка,
//! зависящая от платформы или от запуска, означает, что документ после
//! перезапуска ищется не в том шарде.
//!
//! Число шардов **не меняется** после создания: смена остатка переставляет почти
//! все документы, и это не настройка, а перезаливка. Так же у Elasticsearch.

use crate::segments::{Outcome, QuerySpec, Segments};

/// Имя каталога шарда по его номеру.
pub fn dir_of(root: &str, shard: usize) -> String {
    format!("{root}/shard{shard:04}")
}

/// В какой шард попадает документ с этим идентификатором.
///
/// Остаток от хеша, а не от чего-то осмысленного вроде даты: осмысленное даёт
/// перекос, а перекос — это шард, который заполнился, пока соседние пусты.
pub fn route(id: &str, shards: usize) -> usize {
    if shards <= 1 {
        return 0;
    }
    slot_owner(slot_of(id), shards)
}

/// Сколько ячеек делит пространство идентификаторов. Постоянная и неизменяемая.
///
/// ## Зачем ячейки, если остаток и так делит поровну
///
/// Остаток `hash % шардов` делит поровну и стоит одно деление — но он **привязан
/// к числу шардов**. Добавь шард, и адрес меняется почти у каждого документа:
/// перебалансировка превращается в переезд всего корпуса, а деление одного
/// шарда надвое — в переезд остальных, которых никто не трогал.
///
/// Ячейка разрывает эту связь. Идентификатор отображается в ячейку **навсегда**,
/// а шардам достаются непрерывные полосы ячеек. Тогда деление шарда — это
/// деление его полосы, и переезжает ровно то, что обязано.
///
/// Число выбрано с запасом и фиксировано: 4096 ячеек на шард дают ровные полосы
/// при любом числе шардов до четырёх тысяч, а менять его нельзя — оно входит в
/// адрес документа так же, как хеш.
pub const SLOTS: u64 = 4096;

/// Больше шардов это пространство маршрутизации представить не может.
///
/// Один непустой шард обязан владеть хотя бы одной ячейкой. Поэтому это не
/// «настройка для стенда», а точная граница текущего on-disk/network формата.
/// Принять 4097 означало бы поднять хотя бы один шард, которому не достанется
/// ни одного документа, и выдавать это за масштабирование.
pub const MAX_SHARDS: usize = SLOTS as usize;

/// Проверяет число шардов до выделения каталогов или сетевого fanout.
pub fn checked_count(shards: usize) -> std::io::Result<usize> {
    let count = shards.max(1);
    if count > MAX_SHARDS {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "запрошено {count} шардов при {SLOTS} устойчивых ячейках маршрутизации; \
                 больше {MAX_SHARDS} непустых шардов этот формат не представляет"
            ),
        ));
    }
    Ok(count)
}

/// Ячейка идентификатора. От числа шардов **не зависит** — в этом весь смысл.
pub fn slot_of(id: &str) -> u64 {
    crate::bloom::hash(id) % SLOTS
}

/// Начало полосы `i`-го из `n` при равном делении.
pub fn slot_lo(i: usize, n: usize) -> u64 {
    if n == 0 {
        return 0;
    }
    (i as u64 * SLOTS) / n as u64
}

/// Чей это слот при равном делении на `shards` полос.
///
/// Полосы непрерывны, а не чередуются: непрерывная полоса делится надвое одним
/// числом, и деление шарда описывается границей, а не списком ячеек.
///
/// ## Почему формула такая, а не `slot * n / SLOTS`
///
/// Полосы заданы началами `lo_i = ⌊i·SLOTS/n⌋`, и владелец ячейки — наибольшее
/// `i`, у которого `lo_i ≤ slot`. Простое `slot·n/SLOTS` этому **не** обратно:
/// при трёх шардах ячейка 1365 начинает вторую полосу, а формула отдаёт её
/// первой. Разойдись здесь на единицу — и один документ по сети и в процессе
/// попал бы в разные шарды, то есть запись и чтение искали бы его в разных
/// местах. Проверено перебором всех ячеек при разном числе шардов.
pub fn slot_owner(slot: u64, shards: usize) -> usize {
    if shards <= 1 {
        return 0;
    }
    let n = shards as u64;
    let at = ((slot + 1) * n - 1) / SLOTS;
    (at as usize).min(shards - 1)
}

/// Попадание с указанием шарда.
///
/// Номер документа **локальный для шарда**, и это осознанно: делать его
/// глобальным значило бы вернуться к одному пространству номеров, то есть к тому
/// самому потолку, ради обхода которого шарды и заведены. Наружу пара выходит
/// парой.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ShardHit {
    pub shard: u32,
    pub doc: u64,
    pub score: f64,
}

/// Итог поиска по всем шардам.
pub struct ShardOutcome {
    pub hits: Vec<ShardHit>,
    pub visited: u64,
    /// Хотя бы один шард упёрся в потолок проверок.
    pub partial: bool,
}

/// Несколько индексов как один.
pub struct Shards {
    pub list: Vec<Segments>,
}

impl Shards {
    /// Открывает `shards` каталогов под общим корнем.
    ///
    /// Отсутствующий каталог — не ошибка: шард, в который ещё ничего не попало,
    /// пуст, и открывать в нём нечего. Ошибка — каталог, который есть и не
    /// читается: это неполный набор, и отвечать по нему значило бы недосчитаться
    /// документов молча.
    pub fn open(root: &str, shards: usize) -> std::io::Result<Shards> {
        let n = checked_count(shards)?;
        let mut list = Vec::with_capacity(n);
        for s in 0..n {
            let dir = dir_of(root, s);
            if !std::path::Path::new(&dir).exists() {
                list.push(Segments::from_parts(Vec::new(), Vec::new()));
                continue;
            }
            list.push(Segments::load(&dir)?);
        }
        Ok(Shards { list })
    }

    pub fn docs(&self) -> u64 {
        self.list.iter().map(|s| s.docs() as u64).sum()
    }

    pub fn segments(&self) -> usize {
        self.list.iter().map(|s| s.list.len()).sum()
    }

    /// Ищет по всем шардам с **точной** общей статистикой.
    ///
    /// Два прохода, и первый дешёвый: он только читает `df` уже раскрытых слов,
    /// не обходя списки. Это тот же `dfs`, что у Elasticsearch, только без круга
    /// по сети — шарды в одном процессе.
    pub fn search(&self, spec: &QuerySpec, threads: usize) -> ShardOutcome {
        // Первый проход: складываем `N` и `df` по всем шардам.
        let mut total = 0usize;
        let mut df: Vec<u64> = Vec::new();
        let mut usable = true;
        for s in self.list.iter() {
            if s.list.is_empty() {
                continue;
            }
            let (n, d) = s.stats(spec);
            total = total.saturating_add(n);
            if d.is_empty() {
                // Раскрытие не сошлось — общей статистики не будет, и каждый
                // шард посчитает по своей. Хуже точностью, но не молча: это
                // видно по отсутствию веса, а не по неверному ответу.
                usable = false;
            } else if df.is_empty() {
                df = d;
            } else if df.len() == d.len() {
                for (a, b) in df.iter_mut().zip(d.iter()) {
                    *a = a.saturating_add(*b);
                }
            } else {
                usable = false;
            }
        }
        let global = (usable && !df.is_empty()).then_some((total, df.as_slice()));

        // Второй проход: поиск с общей статистикой, потом слияние верхушек.
        let mut hits: Vec<ShardHit> = Vec::new();
        let mut visited = 0u64;
        let mut partial = false;
        for (i, s) in self.list.iter().enumerate() {
            if s.list.is_empty() {
                continue;
            }
            let out: Outcome = s.search_with_g(spec, threads, global);
            visited += out.visited;
            partial |= out.partial;
            hits.extend(out.hits.iter().map(|h| ShardHit {
                shard: i as u32,
                doc: h.doc,
                score: h.score,
            }));
        }
        // По убыванию веса, при равенстве — по паре «шард, номер»: иначе порядок
        // зависел бы от того, в каком порядке шарды успели ответить.
        hits.sort_by(|a, b| {
            b.score.total_cmp(&a.score).then(a.shard.cmp(&b.shard)).then(a.doc.cmp(&b.doc))
        });
        hits.truncate(spec.k);
        ShardOutcome { hits, visited, partial }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::Indexer;
    use crate::segments::{SpecWord, Term};

    fn root(name: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-shards-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        d
    }

    /// Наполняет `n` шардов, раскладывая документы по хешу их идентификаторов.
    fn fill(root: &str, shards: usize, docs: usize, per: u32) {
        let mut ixs: Vec<Indexer> = (0..shards)
            .map(|s| Indexer::new(&dir_of(root, s), vec!["body".into()], vec![1.0], per))
            .collect();
        for i in 0..docs {
            let id = format!("d{i}");
            let s = route(&id, shards);
            ixs[s].add(&id, &[&format!("общее слово{} редкое{}", i % 13, i)]);
        }
        for ix in ixs.iter_mut() {
            ix.flush_now();
        }
    }

    fn one(word: &str, k: usize) -> QuerySpec {
        QuerySpec {
            words: vec![SpecWord { fid: 0, terms: vec![Term::parse(word)], given: None }],
            k,
            ..QuerySpec::default()
        }
    }

    /// Раскладка ровная и **воспроизводимая**.
    ///
    /// Воспроизводимость важнее ровности: раскладка, зависящая от запуска или от
    /// платформы, означает, что документ после перезапуска ищется не в том
    /// шарде — то есть теряется.
    #[test]
    fn routing_is_even_and_reproducible() {
        let n = 8;
        let mut counts = vec![0usize; n];
        for i in 0..80_000 {
            counts[route(&format!("d{i}"), n)] += 1;
        }
        let lo = *counts.iter().min().unwrap();
        let hi = *counts.iter().max().unwrap();
        assert!(hi * 100 < lo * 115, "перекос: от {lo} до {hi} на восьми шардах");

        for id in ["d0", "документ", "", "urn:x:1"] {
            assert_eq!(route(id, n), route(id, n), "раскладка не воспроизводится");
        }
        assert_eq!(route("что угодно", 1), 0, "при одном шарде всё в нулевой");
    }

    /// Выдача по шардам совпадает с выдачей по одному индексу — **по весам**.
    ///
    /// Это главное свойство фазы: скоры остаются точными, потому что обратная
    /// частота считается по всем шардам, а не по каждому по отдельности. Сравнить
    /// номера документов нельзя — они локальные для шарда и по построению другие,
    /// — поэтому сверяются веса и число попаданий.
    ///
    /// Проверка не вхолостую: без общей статистики веса разошлись бы, и это
    /// видно — соседний тест снимает её и показывает расхождение.
    #[test]
    fn scores_match_a_single_index() {
        let r = root("same");
        let flat = format!("{r}/flat");
        // Один индекс на всём корпусе.
        let mut ix = Indexer::new(&flat, vec!["body".into()], vec![1.0], 250);
        for i in 0..4000 {
            ix.add(&format!("d{i}"), &[&format!("общее слово{} редкое{}", i % 13, i)]);
        }
        ix.flush_now();
        drop(ix);
        // Тот же корпус, разложенный по четырём шардам.
        fill(&r, 4, 4000, 250);

        let flat = Segments::load(&flat).expect("плоский индекс");
        let sh = Shards::open(&r, 4).expect("шарды");
        assert_eq!(sh.docs(), flat.docs() as u64, "документы потерялись при раскладке");
        assert!(sh.list.iter().all(|s| !s.list.is_empty()), "шард остался пустым");

        for (word, k) in [("общее", 10), ("слово3", 20), ("общее", 100)] {
            let spec = one(word, k);
            let a: Vec<u64> = {
                let mut v: Vec<u64> =
                    flat.search_with(&spec, 4).hits.iter().map(|h| h.score.to_bits()).collect();
                v.sort_unstable();
                v
            };
            let b: Vec<u64> = {
                let mut v: Vec<u64> =
                    sh.search(&spec, 4).hits.iter().map(|h| h.score.to_bits()).collect();
                v.sort_unstable();
                v
            };
            assert_eq!(a.len(), b.len(), "слово {word}, k={k}: разное число попаданий");
            assert_eq!(a, b, "слово {word}, k={k}: веса разошлись — общая частота не сложилась");
        }
        std::fs::remove_dir_all(&r).ok();
    }

    /// Без общей статистики веса расходятся — то есть проверка выше не вхолостую.
    #[test]
    fn without_global_statistics_the_scores_drift() {
        let r = root("drift");
        fill(&r, 4, 4000, 250);
        let sh = Shards::open(&r, 4).expect("шарды");
        let spec = one("общее", 50);

        let with: Vec<u64> = sh.search(&spec, 1).hits.iter().map(|h| h.score.to_bits()).collect();
        // То же, но каждый шард считает частоту по себе.
        let mut without: Vec<ShardHit> = Vec::new();
        for (i, s) in sh.list.iter().enumerate() {
            for h in s.search_with_g(&spec, 1, None).hits.iter() {
                without.push(ShardHit { shard: i as u32, doc: h.doc, score: h.score });
            }
        }
        without.sort_by(|a, b| b.score.total_cmp(&a.score));
        without.truncate(spec.k);
        let without: Vec<u64> = without.iter().map(|h| h.score.to_bits()).collect();
        assert_ne!(
            with, without,
            "без общей статистики веса совпали — значит она ни на что не влияет"
        );
        std::fs::remove_dir_all(&r).ok();
    }

    /// Потолок обходится: у каждого шарда своя нумерация.
    #[test]
    fn each_shard_numbers_its_documents_on_its_own() {
        let r = root("numbering");
        fill(&r, 3, 900, 300);
        let sh = Shards::open(&r, 3).expect("шарды");
        // У каждого непустого шарда есть документ с номером ноль — то есть
        // пространства номеров независимы, а не продолжают друг друга.
        for (i, s) in sh.list.iter().enumerate() {
            if s.list.is_empty() {
                continue;
            }
            assert_eq!(s.base[0], 0, "шард {i}: нумерация не начинается с ноля");
        }
        // Верхушка шире корпуса — значит в неё попадают все шарды.
        //
        // Тридцати попаданий для этого не хватило бы, и не по ошибке: у всех
        // документов здесь один и тот же вес, а при равных весах тай-брейк идёт
        // по паре «шард, номер» — и первые тридцать честно достаются нулевому
        // шарду. Порядок обязан быть определённым, иначе он зависел бы от того,
        // какой шард успел ответить первым.
        let out = sh.search(&one("общее", 1000), 2);
        assert_eq!(out.hits.len(), 900, "нашлись не все документы");
        let mut seen: Vec<u32> = out.hits.iter().map(|h| h.shard).collect();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), 3, "в выдаче не все шарды: {seen:?}");
        // И каждый шард пронумеровал свои документы с ноля: пары «шард, номер»
        // повторяются по номеру, но не по паре.
        let mut pairs: Vec<(u32, u64)> = out.hits.iter().map(|h| (h.shard, h.doc)).collect();
        let before = pairs.len();
        pairs.sort_unstable();
        pairs.dedup();
        assert_eq!(pairs.len(), before, "пары «шард, номер» повторились");
        assert!(
            out.hits.iter().filter(|h| h.doc == 0).count() == 3,
            "номер ноль обязан быть у каждого шарда свой"
        );
        std::fs::remove_dir_all(&r).ok();
    }

    /// Пустой шард не мешает и не считается за поломку.
    #[test]
    fn an_empty_shard_is_not_a_failure() {
        let r = root("empty");
        fill(&r, 2, 400, 200);
        // Третий шард объявлен, но каталога у него нет.
        let sh = Shards::open(&r, 3).expect("шарды");
        assert_eq!(sh.list.len(), 3);
        assert!(sh.list[2].list.is_empty(), "несуществующий шард открылся непустым");
        assert_eq!(sh.docs(), 400);
        assert!(!sh.search(&one("общее", 10), 2).hits.is_empty(), "поиск сломался о пустой шард");
        std::fs::remove_dir_all(&r).ok();
    }
}

#[cfg(test)]
mod slot_tests {
    use super::*;

    /// Владелец ячейки и границы полос — одно и то же, во всех ячейках.
    ///
    /// Проверяется перебором, а не примерами: расхождение здесь бывает ровно на
    /// одну ячейку на границе, и пример его не находит. Первая же попытка на
    /// формуле `slot·n/SLOTS` разошлась при трёх шардах на ячейке 1365.
    #[test]
    fn the_owner_agrees_with_the_edges_everywhere() {
        for n in [1usize, 2, 3, 4, 5, 7, 8, 16, 33, 100, 4096] {
            for slot in 0..SLOTS {
                let owner = slot_owner(slot, n);
                let lo = slot_lo(owner, n);
                let hi = slot_lo(owner + 1, n);
                assert!(
                    slot >= lo && (slot < hi || owner == n - 1),
                    "n={n}, ячейка {slot} отдана шарду {owner} с полосой [{lo}, {hi})"
                );
            }
        }
    }

    /// Полосы покрывают всё пространство и не перекрываются.
    #[test]
    fn the_stripes_tile_the_space() {
        for n in [1usize, 3, 7, 64] {
            let mut edge = 0u64;
            for i in 0..n {
                assert_eq!(slot_lo(i, n), edge, "n={n}: полоса {i} не примыкает к прошлой");
                edge = slot_lo(i + 1, n);
                assert!(edge > slot_lo(i, n), "n={n}: полоса {i} пуста");
            }
            assert_eq!(edge, SLOTS, "n={n}: полосы кончились раньше пространства");
        }
    }

    /// Ячейка документа не зависит от числа шардов. В этом весь смысл ячеек.
    #[test]
    fn a_slot_does_not_move_when_shards_do() {
        for i in 0..500 {
            let id = format!("документ-{i}");
            let was = slot_of(&id);
            assert_eq!(slot_of(&id), was, "ячейка не постоянна сама себе");
        }
        // А адрес — зависит, и только через владельца полосы.
        let id = "документ-7";
        assert_eq!(route(id, 1), 0);
        assert_eq!(route(id, 4), slot_owner(slot_of(id), 4));
    }

    /// Точная граница пространства принимается, первая пустая полоса — нет.
    #[test]
    fn shard_count_stops_at_the_routing_space_not_after_it() {
        assert_eq!(checked_count(0).unwrap(), 1, "ноль означает одиночный индекс");
        assert_eq!(checked_count(MAX_SHARDS).unwrap(), MAX_SHARDS);
        let error = checked_count(MAX_SHARDS + 1).expect_err("пустой 4097-й шард принят");
        let message = error.to_string();
        assert!(
            message.contains("4097") && message.contains("4096") && message.contains("яче"),
            "отказ не объясняет точную границу: {message}"
        );
    }
}
