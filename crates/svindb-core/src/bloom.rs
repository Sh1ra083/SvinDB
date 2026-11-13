//! Фильтр Блума на сегмент: отсечь «здесь такого точно нет».

use crate::mmap::Region;

/// Байт в блоке — ровно строка кеша.
pub const BLOCK_BYTES: usize = 64;
/// Бит в блоке.
pub const BLOCK_BITS: u32 = (BLOCK_BYTES * 8) as u32;
/// Длина заголовка файла.
const HEADER: usize = 16;
/// Метка формата.
const MAGIC: u32 = u32::from_le_bytes(*b"SVBF");

/// Сколько бит на идентификатор отводить по умолчанию.
///
/// Плотность меняется без переиндексации: `svindb bloom <каталог> <бит>`.
/// Это существенно, потому что правильное значение зависит не от корпуса, а от
/// доли изменяющихся документов: на заливке без изменений фильтр — чистая плата.
pub const DEFAULT_BITS: usize = 12;

/// Число хеш-функций при заданной плотности.
pub fn hashes_for(bits_per_item: usize) -> u32 {
    ((bits_per_item as f64 * std::f64::consts::LN_2).round() as u32).clamp(1, 16)
}

/// Хеш идентификатора — считается один раз на идентификатор, не на сегмент.
///
/// Вынесен наружу намеренно. Проверка идёт по всем сегментам подряд, а хеш от
/// сегмента не зависит: считать его пятьдесят раз вместо одного значит отдать
/// обратно половину того, ради чего фильтр заводился.
#[inline]
pub fn hash(id: &str) -> u64 {
    hash64(id.as_bytes())
}

/// Хеш слова для фильтра словаря.
///
/// Поле намеренно не входит в ключ. Такой фильтр отвечает на более широкий
/// вопрос: «есть ли это слово хотя бы в одном поле сегмента». Для запроса,
/// повторяющего одно редкое слово по шестидесяти полям, это одна проверка на
/// сегмент вместо шестидесяти обращений к словарю. Ложное срабатывание лишь
/// оставляет сегмент в работе; отрицательный ответ точен и потому не меняет
/// выдачу.
#[inline]
pub fn term_hash(term: &str) -> u64 {
    hash64(term.as_bytes())
}

/// Хеш идентификатора.
///
/// FNV-1a с финализатором из splitmix64. Сам FNV плохо перемешивает старшие
/// разряды, а нам нужны именно они — по ним выбирается блок. Без финализатора
/// близкие идентификаторы (`док-1000`, `док-1001`) садятся в соседние блоки, и
/// доля ложных срабатываний перестаёт быть равномерной.
#[inline]
pub fn hash64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    mix(h)
}

/// Перемешивает 64 разряда — тот же финализатор, что в `hash64`.
#[inline]
fn mix(mut h: u64) -> u64 {
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

/// Блок и шаг по битам внутри него.
///
/// Блок берётся умножением, а не остатком: остаток это деление, двадцать тактов
/// на каждую проверку. Шаг делается нечётным, чтобы последовательность
/// `x, x+d, x+2d, …` обходила все 512 позиций и `k` бит не наложились друг на
/// друга при неудачном хеше.
#[inline]
fn place(h: u64, blocks: usize) -> (usize, u32, u32) {
    let block = (((h >> 32) as u128 * blocks as u128) >> 32) as usize;
    let g = mix(h ^ 0x9e37_79b9_7f4a_7c15);
    let start = g as u32 & (BLOCK_BITS - 1);
    let step = (g >> 32) as u32 | 1;
    (block, start, step)
}

/// Набор фильтра по одному идентификатору.
///
/// ## Зачем отдельно от [`build`]
///
/// Слияние идентификаторы **не собирает**: список всех строк сегмента — это
/// гигабайты на сегменте в десятки миллионов документов, и нужен он был ровно
/// затем, чтобы отдать его сюда срезом. Фильтр же набирается за один проход и
/// смотреть назад ему не на что.
///
/// Число блоков считается от **обещанного** числа элементов, а не от
/// набранного: оно уходит в заголовок и обязано быть известно до первого бита.
/// Пообещать больше, чем добавишь, безопасно — фильтр станет разреженнее и
/// врать будет реже; пообещать меньше — плотнее и врать чаще, но не солгать в
/// опасную сторону. Отвергнуть существующее он не может ни в одном случае.
pub struct Builder {
    out: Vec<u8>,
    blocks: usize,
    k: u32,
}

impl Builder {
    /// `items` — сколько идентификаторов будет добавлено.
    pub fn new(items: usize, bits_per_item: usize) -> Builder {
        let k = hashes_for(bits_per_item);
        let blocks = blocks_for(items, bits_per_item);
        let mut out = vec![0u8; HEADER + blocks * BLOCK_BYTES];
        out[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        out[4..8].copy_from_slice(&1u32.to_le_bytes());
        out[8..12].copy_from_slice(&(blocks as u32).to_le_bytes());
        out[12..16].copy_from_slice(&k.to_le_bytes());
        Builder { out, blocks, k }
    }

    /// Отмечает идентификатор. Байты, а не строка: слияние читает их прямо из
    /// `ids.bin` и заводить под каждый `String` ему не за чем.
    pub fn add(&mut self, id: &[u8]) {
        self.add_hash(hash64(id));
    }

    /// Добавляет уже посчитанный хеш.
    ///
    /// Нужен словарному фильтру: одно слово встречается в нескольких полях,
    /// но хешировать его заново для каждого поля при сбросе нет причины.
    #[inline]
    pub fn add_hash(&mut self, hash: u64) {
        let (block, start, step) = place(hash, self.blocks);
        let base = HEADER + block * BLOCK_BYTES;
        let mut bit = start;
        for _ in 0..self.k {
            self.out[base + (bit >> 3) as usize] |= 1u8 << (bit & 7);
            bit = bit.wrapping_add(step) & (BLOCK_BITS - 1);
        }
    }

    /// Готовый файл целиком.
    pub fn finish(self) -> Vec<u8> {
        self.out
    }
}

/// Собирает фильтр по идентификаторам. Отдаёт готовый файл целиком.
///
/// Тот же [`Builder`], только со списком на входе: приём идентификаторы и так
/// держит, и разводить ему два кодирования незачем. Общий код здесь не ради
/// краткости — разойдись накопительный путь с потоковым хоть одним битом, и
/// сегменты, собранные приёмом и слиянием, отвечали бы на один идентификатор
/// по-разному.
pub fn build(ids: &[String], bits_per_item: usize) -> Vec<u8> {
    let mut b = Builder::new(ids.len(), bits_per_item);
    for id in ids {
        b.add(id.as_bytes());
    }
    b.finish()
}

/// Сколько блоков нужно на столько идентификаторов.
///
/// Не меньше одного: пустой сегмент тоже должен давать корректный файл, и на все
/// вопросы он честно отвечает «нет».
pub fn blocks_for(items: usize, bits_per_item: usize) -> usize {
    let bits = items.saturating_mul(bits_per_item).max(1);
    bits.div_ceil(BLOCK_BITS as usize).max(1)
}

/// Отображённый фильтр.
pub struct Bloom {
    map: Region,
    blocks: usize,
    k: u32,
}

impl Bloom {
    /// Открывает файл фильтра. `None` — файла нет или он не наш.
    ///
    /// Отсутствие файла — обычное положение дел, а не ошибка: сегменты, собранные
    /// до появления фильтра, продолжают работать, просто без отсечения.
    pub fn open(src: &crate::container::SegmentSource, name: &str) -> Option<Bloom> {
        let map = src.try_open(name)?;
        if map.len_bytes() < HEADER || map.u32_at(0) != MAGIC || map.u32_at(4) != 1 {
            return None;
        }
        let blocks = map.u32_at(8) as usize;
        let k = map.u32_at(12);
        if blocks == 0 || k == 0 || map.len_bytes() < HEADER + blocks * BLOCK_BYTES {
            return None;
        }
        Some(Bloom { map, blocks, k })
    }

    /// `false` — такого идентификатора здесь точно нет.
    #[inline]
    pub fn maybe_has(&self, id: &str) -> bool {
        self.maybe_has_hash(hash(id))
    }

    /// `false` — такого слова нет ни в одном поле сегмента.
    #[inline]
    pub fn maybe_has_term(&self, term: &str) -> bool {
        self.maybe_has_hash(term_hash(term))
    }

    /// То же по готовому хешу — см. [`hash`].
    #[inline]
    pub fn maybe_has_hash(&self, h: u64) -> bool {
        let (block, start, step) = place(h, self.blocks);
        let base = HEADER + block * BLOCK_BYTES;
        let mut bit = start;
        for _ in 0..self.k {
            if self.map.u8_at(base + (bit >> 3) as usize) & (1u8 << (bit & 7)) == 0 {
                return false;
            }
            bit = bit.wrapping_add(step) & (BLOCK_BITS - 1);
        }
        true
    }

    /// Сколько места занимает — для учёта памяти.
    pub fn bytes(&self) -> usize {
        self.map.len_bytes()
    }

    pub fn warm(&self) -> usize {
        self.map.warm()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(n: usize, prefix: &str) -> Vec<String> {
        (0..n).map(|i| format!("{prefix}-{i:08}")).collect()
    }

    fn open_built(name: &str, items: &[String], bits: usize) -> (String, Bloom) {
        let dir = std::env::temp_dir().join(format!("svindb-bloom-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let d = dir.to_string_lossy().to_string();
        std::fs::write(dir.join("f.bloom"), build(items, bits)).unwrap();
        let b = Bloom::open(&crate::container::loose(&d), "f.bloom").unwrap();
        (d, b)
    }

    /// Единственное, чего фильтру нельзя: отвергнуть существующее.
    #[test]
    fn never_denies_what_it_holds() {
        let items = ids(50_000, "док");
        let (dir, b) = open_built("present", &items, DEFAULT_BITS);
        for id in items.iter() {
            assert!(b.maybe_has(id), "фильтр отверг существующий {id}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Для блочного фильтра формула классического неверна: биты ложатся по
    /// блокам неравномерно, и переполненные врут чаще среднего. Тест закрепляет
    /// то, что получилось на самом деле, с запасом на случайность хеша.
    #[test]
    fn false_positive_rate_stays_within_the_measured_bound() {
        let items = ids(100_000, "есть");
        let probes = ids(200_000, "нету");
        for (bits, bound) in [(4usize, 0.30), (8, 0.050), (12, 0.012), (16, 0.005)] {
            let (dir, b) = open_built(&format!("fpr{bits}"), &items, bits);
            let hits = probes.iter().filter(|p| b.maybe_has(p)).count();
            let rate = hits as f64 / probes.len() as f64;
            assert!(
                rate <= bound,
                "{bits} бит на элемент: ложных {:.3}%, ждали не больше {:.3}%",
                rate * 100.0,
                bound * 100.0
            );
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// Поток и список дают **один и тот же** файл, байт в байт.
    ///
    /// Слияние набирает фильтр [`Builder`]-ом по ходу обхода идентификаторов,
    /// приём — [`build`] по готовому списку. Разойдись они, и фильтр слитого
    /// сегмента отвечал бы «здесь такого нет» про существующий документ:
    /// удаление молча вернуло бы «удалено ноль», а документ остался бы в выдаче.
    /// Проверяются и края: пустой набор и единственный элемент — на них
    /// расходится счёт блоков.
    #[test]
    fn the_streaming_builder_matches_the_batch_one() {
        for n in [0usize, 1, 63, 64, 65, 10_000] {
            let items = ids(n, "док");
            for bits in [4usize, 12, 16] {
                let mut b = Builder::new(items.len(), bits);
                for id in items.iter() {
                    b.add(id.as_bytes());
                }
                assert_eq!(b.finish(), build(&items, bits), "{n} элементов при {bits} битах");
            }
        }
    }

    /// Пустой сегмент даёт годный файл, который на всё отвечает «нет».
    #[test]
    fn an_empty_filter_denies_everything() {
        let (dir, b) = open_built("empty", &[], DEFAULT_BITS);
        assert!(!b.maybe_has("что угодно"));
        assert!(!b.maybe_has(""));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Чужой или обрезанный файл не должен открываться молча.
    #[test]
    fn a_foreign_or_truncated_file_is_refused() {
        let dir = std::env::temp_dir().join("svindb-bloom-bad");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();

        let src = crate::container::loose(&dir.to_string_lossy());
        std::fs::write(dir.join("junk.bloom"), vec![7u8; 4096]).unwrap();
        assert!(Bloom::open(&src, "junk.bloom").is_none(), "чужой файл принят за фильтр");

        // Наш заголовок, но тела нет: так выглядит файл, недописанный при сбое.
        let full = build(&ids(1000, "д"), DEFAULT_BITS);
        std::fs::write(dir.join("cut.bloom"), &full[..HEADER + BLOCK_BYTES]).unwrap();
        assert!(Bloom::open(&src, "cut.bloom").is_none(), "обрезанный файл принят за целый");

        // Целый — открывается: иначе проверки выше доказывали бы только то, что
        // фильтр не открывается никогда.
        std::fs::write(dir.join("ok.bloom"), &full).unwrap();
        assert!(Bloom::open(&src, "ok.bloom").is_some(), "целый фильтр не открылся");

        assert!(Bloom::open(&src, "нет.bloom").is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Идентификаторы, идущие подряд, не должны садиться в один блок.
    ///
    /// Это и есть проверка финализатора: без него у близких строк совпадают
    /// старшие разряды, блоки забиваются кучно, и доля ложных взлетает.
    #[test]
    fn neighbouring_ids_spread_across_blocks() {
        let items = ids(10_000, "подряд");
        let blocks = blocks_for(items.len(), DEFAULT_BITS);
        let used: std::collections::HashSet<usize> =
            items.iter().map(|s| place(hash(s), blocks).0).collect();
        // При равномерном разбрасывании занятых блоков должно быть около
        // `blocks·(1 − e^(−n/blocks))`; берём заведомо слабую границу.
        assert!(
            used.len() * 10 > blocks * 6,
            "занято {} блоков из {blocks} — хеш кучкует",
            used.len()
        );
    }
}

#[cfg(test)]
mod measured {
    use super::*;

    /// Печатает долю ложных срабатываний по плотностям.
    #[test]
    fn print_false_positive_rates() {
        let items: Vec<String> = (0..200_000).map(|i| format!("док-{i:08}")).collect();
        let probes: Vec<String> = (0..500_000).map(|i| format!("иной-{i:08}")).collect();
        println!("\nбит/эл  k  байт/эл  ложных, %");
        for bits in [4usize, 6, 8, 10, 12, 16] {
            let f = build(&items, bits);
            let dir = std::env::temp_dir().join("svindb-bloom-measure");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("f.bloom"), &f).unwrap();
            let b =
                Bloom::open(&crate::container::loose(&dir.to_string_lossy()), "f.bloom").unwrap();
            let hits = probes.iter().filter(|p| b.maybe_has(p)).count();
            println!(
                "{bits:>6} {:>2} {:>8.3} {:>10.3}",
                hashes_for(bits),
                f.len() as f64 / items.len() as f64,
                hits as f64 * 100.0 / probes.len() as f64
            );
            std::fs::remove_dir_all(&dir).ok();
        }
    }
}
