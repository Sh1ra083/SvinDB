//! Слот-пространство: массив документа как отрезок ординалов.

use crate::mmap::Region;

/// Документов в блоке. То же число, что у постингов и у колонки.
pub const BLOCK: usize = 128;

/// Байт на блок в указателе: первый ординал и смещение полосы.
pub const AIDX: usize = 8;

/// Номер корневой группы. У неё арность единица у каждого документа.
pub const ROOT: u32 = 0;

/// Как уложена полоса арности.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Kind {
    /// Арность единица у каждого документа: файлов нет, ординал равен номеру.
    #[default]
    Identity,
    /// Полоса на блок из 128 документов.
    Dense,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Identity => "identity",
            Kind::Dense => "dense",
        }
    }

    pub fn parse(s: &str) -> Option<Kind> {
        match s {
            "identity" => Some(Kind::Identity),
            "dense" => Some(Kind::Dense),
            _ => None,
        }
    }
}

/// Собранная полоса арности.
pub struct Built {
    pub kind: Kind,
    pub idx: Vec<u8>,
    pub band: Vec<u8>,
    /// Сколько всего слотов у группы в этом сегменте.
    pub units: usize,
}

/// Влезает ли смещение полосы арности в четыре байта.
///
/// ## Почему проверяется, а не подразумевается
///
/// В `g*.aidx` на каждый блок лежат два тридцатидвухбитных числа: первый ординал
/// блока и смещение блока внутри `g*.arity`. Заворот любого из них не отказывает
/// и не ловится ни одной сверкой размеров: указатель просто начинает читать
/// полосу не с той позиции, и ординалы всей группы съезжают. Значение вложенного
/// поля после этого приписывается чужому элементу массива — молча, на выдаче.
///
/// Проверяются оба числа. Смещение растёт с объёмом полосы, ординал — с числом
/// слотов; на четырёх миллиардах элементов массивов упирается ординал даже там,
/// где полоса далеко от потолка.
///
/// Отказ, а не паника: `panic = "abort"` в выпуске превратил бы `assert!` в
/// убийство узла на приёме.
fn fits_band(len: usize, band: &str, what: &str) -> std::io::Result<()> {
    let ceiling = crate::dict::offset_ceiling();
    if len as u64 <= ceiling {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "{band}: {what} группы перевалили за четыре гигабайта \
             ({len} при потолке {ceiling}) — указатель рядом с полосой тридцатидвухбитный \
             и завернулся бы, а ординалы группы съехали бы на чужие элементы; \
             уменьшите flush_docs у приёма или target_docs у укрупнения"
        ),
    ))
}

/// Кодирует арности документов.
///
/// Вид выбирается по данным: если у каждого документа ровно один слот, полосы
/// нет вовсе. Это не частный случай ради красоты — так выглядит **всякое**
/// поле вне массива, то есть подавляющее большинство.
///
/// `band` — куда ляжет полоса; идёт только в текст отказа по потолку.
pub fn encode(arity: &[u32], band_path: &str) -> std::io::Result<Built> {
    let units: usize = arity.iter().map(|a| *a as usize).sum();
    // Число слотов проверяется и у `Identity`: там оно равно числу документов и
    // до потолка не дотягивается, но правило одно на оба вида, и исключение из
    // него пришлось бы обосновывать заново при каждой правке.
    fits_band(units, band_path, "слоты")?;
    if arity.iter().all(|a| *a == 1) {
        return Ok(Built { kind: Kind::Identity, idx: Vec::new(), band: Vec::new(), units });
    }
    let nblocks = arity.len().div_ceil(BLOCK);
    let mut idx = Vec::with_capacity(nblocks * AIDX);
    let mut band = Vec::with_capacity(nblocks * (1 + BLOCK));
    let mut first = 0u32;
    for chunk in arity.chunks(BLOCK) {
        // Отказ **до** записи усечённого смещения: записанное неотличимо от
        // настоящего, и починить его потом нечем.
        fits_band(band.len(), band_path, "полоса")?;
        idx.extend(first.to_le_bytes());
        idx.extend((band.len() as u32).to_le_bytes());
        // `pack_values` сам пишет байт ширины и добивает хвост до границы
        // байта. Нулевая ширина законна и нужна: блок, где ни у одного
        // документа нет массива, не занимает ни бита данных.
        let mut bp = 0u64;
        crate::bits::pack_values(&mut band, chunk, &mut bp);
        first += chunk.iter().sum::<u32>();
    }
    Ok(Built { kind: Kind::Dense, idx, band, units })
}

/// Отображённая полоса арности группы.
pub enum Slots {
    Identity { docs: usize },
    Dense { idx: Region, band: Region, docs: usize, units: usize },
}

impl Slots {
    /// Открывает полосу группы. `None` — файлы обещаны заголовком, но их нет.
    pub fn open(
        src: &crate::container::SegmentSource,
        gid: u32,
        kind: Kind,
        docs: usize,
        units: usize,
    ) -> Option<Slots> {
        match kind {
            Kind::Identity => Some(Slots::Identity { docs }),
            Kind::Dense => {
                let idx = src.try_open(&format!("g{gid}.aidx"))?;
                let band = src.try_open(&format!("g{gid}.arity"))?;
                if idx.len_bytes() < docs.div_ceil(BLOCK) * AIDX {
                    return None;
                }
                Some(Slots::Dense { idx, band, docs, units })
            }
        }
    }

    pub fn docs(&self) -> usize {
        match self {
            Slots::Identity { docs } => *docs,
            Slots::Dense { docs, .. } => *docs,
        }
    }

    /// Сколько всего слотов у группы.
    pub fn units(&self) -> usize {
        match self {
            Slots::Identity { docs } => *docs,
            Slots::Dense { units, .. } => *units,
        }
    }

    pub fn bytes(&self) -> usize {
        match self {
            Slots::Identity { .. } => 0,
            Slots::Dense { idx, band, .. } => idx.len_bytes() + band.len_bytes(),
        }
    }

    /// Первый ординал документа и число его слотов.
    ///
    /// `(_, 0)` — массива у документа нет. Отличать это от «массив есть, но
    /// пустой» смысла нет: и то и другое означает «ни одного элемента».
    #[inline]
    pub fn range(&self, doc: u32) -> (u32, u32) {
        match self {
            Slots::Identity { docs } => {
                if (doc as usize) < *docs {
                    (doc, 1)
                } else {
                    (0, 0)
                }
            }
            Slots::Dense { idx, band, docs, .. } => {
                if (doc as usize) >= *docs {
                    return (0, 0);
                }
                let b = doc as usize / BLOCK;
                let off = b * AIDX;
                let first = idx.u32_at(off);
                let at = idx.u32_at(off + 4) as usize;
                let n = (*docs - b * BLOCK).min(BLOCK);
                let width = crate::bits::width_of(band.slice(at, 1));
                if width == 0 {
                    // Ни одного слота во всём блоке.
                    return (first, 0);
                }
                let src = band.slice(at + 1, crate::bits::window_len(n, width));
                let slot = doc as usize % BLOCK;
                let mut sum = 0u32;
                for i in 0..slot {
                    sum += crate::bits::value_at(src, width, i);
                }
                (first + sum, crate::bits::value_at(src, width, slot))
            }
        }
    }

    /// Число слотов документа.
    #[inline]
    pub fn arity(&self, doc: u32) -> u32 {
        self.range(doc).1
    }

    /// Номер документа, которому принадлежит ординал.
    ///
    /// Двоичный поиск по первым ординалам блоков, затем проход по блоку.
    /// `None` — ординал за пределами группы.
    pub fn doc_of(&self, unit: u32) -> Option<u32> {
        match self {
            Slots::Identity { docs } => ((unit as usize) < *docs).then_some(unit),
            Slots::Dense { idx, band, docs, units } => {
                if (unit as usize) >= *units {
                    return None;
                }
                let nblocks = docs.div_ceil(BLOCK);
                let (mut lo, mut hi) = (0usize, nblocks);
                while lo + 1 < hi {
                    let mid = (lo + hi) / 2;
                    if idx.u32_at(mid * AIDX) <= unit {
                        lo = mid;
                    } else {
                        hi = mid;
                    }
                }
                let at = idx.u32_at(lo * AIDX + 4) as usize;
                let n = (*docs - lo * BLOCK).min(BLOCK);
                let width = crate::bits::width_of(band.slice(at, 1));
                if width == 0 {
                    return None;
                }
                let src = band.slice(at + 1, crate::bits::window_len(n, width));
                let mut acc = idx.u32_at(lo * AIDX);
                for i in 0..n {
                    let a = crate::bits::value_at(src, width, i);
                    if unit < acc + a {
                        return Some((lo * BLOCK + i) as u32);
                    }
                    acc += a;
                }
                None
            }
        }
    }

    /// Гасит биты документов, у которых зажжён хотя бы один слот.
    ///
    /// **Единственное место во всём движке, где происходит дедупликация.**
    /// Держать её в одном месте обязательно: расползись она по путям обхода —
    /// и в одном из них её однажды забудут, а проявится это дубликатами в
    /// верхушке, то есть потерей мест выдачи.
    ///
    /// `slots` — битовая карта слотов, `out` — карта документов. Обход идёт
    /// слиянием: биты по возрастанию, полоса арности по возрастанию, назад
    /// никто не ходит.
    pub fn fold(&self, slots: &[u64], out: &mut [u64]) -> usize {
        let mut lit = 0usize;
        match self {
            Slots::Identity { docs } => {
                for (w, chunk) in slots.iter().enumerate() {
                    let mut bits = *chunk;
                    while bits != 0 {
                        let b = bits.trailing_zeros() as usize;
                        bits &= bits - 1;
                        let d = w * 64 + b;
                        if d < *docs {
                            let (i, m) = (d / 64, 1u64 << (d % 64));
                            if let Some(slot) = out.get_mut(i) {
                                if *slot & m == 0 {
                                    *slot |= m;
                                    lit += 1;
                                }
                            }
                        }
                    }
                }
            }
            Slots::Dense { .. } => {
                let mut doc = 0u32;
                let (mut from, mut n) = self.range(doc);
                for (w, chunk) in slots.iter().enumerate() {
                    let mut bits = *chunk;
                    while bits != 0 {
                        let b = bits.trailing_zeros() as usize;
                        bits &= bits - 1;
                        let unit = (w * 64 + b) as u32;
                        // Слоты идут по возрастанию, документы тоже: курсор
                        // догоняет, а не ищет заново.
                        while n == 0 || unit >= from + n {
                            if (doc as usize + 1) >= self.docs() {
                                break;
                            }
                            doc += 1;
                            let r = self.range(doc);
                            from = r.0;
                            n = r.1;
                        }
                        if n > 0 && unit >= from && unit < from + n {
                            let (i, m) = (doc as usize / 64, 1u64 << (doc % 64));
                            if let Some(slot) = out.get_mut(i) {
                                if *slot & m == 0 {
                                    *slot |= m;
                                    lit += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        lit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn build(name: &str, arity: &[u32]) -> (String, Built) {
        let dir = std::env::temp_dir().join(format!("svindb-slots-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let b = encode(arity, "проверка/g1.arity").expect("влезло");
        let d = dir.to_string_lossy().to_string();
        if b.kind == Kind::Dense {
            std::fs::File::create(format!("{d}/g1.aidx")).unwrap().write_all(&b.idx).unwrap();
            std::fs::File::create(format!("{d}/g1.arity")).unwrap().write_all(&b.band).unwrap();
        }
        (d, b)
    }

    /// Отрезок документа совпадает с честным пересчётом.
    #[test]
    fn every_range_matches_an_honest_prefix_sum() {
        let mut seed = 12345u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        // Длины вперемешку, включая нули и крупные: блок обязан пережить и то,
        // и другое.
        let arity: Vec<u32> = (0..1000).map(|_| (next() % 6) as u32).collect();
        let (dir, b) = build("ranges", &arity);
        let s = Slots::open(&crate::container::loose(&dir), 1, b.kind, arity.len(), b.units)
            .expect("полоса открылась");
        assert_eq!(s.units(), arity.iter().sum::<u32>() as usize);

        let mut want = 0u32;
        for (d, &a) in arity.iter().enumerate() {
            assert_eq!(s.range(d as u32), (want, a), "документ {d}");
            want += a;
        }
        // За границей сегмента слотов нет.
        assert_eq!(s.range(arity.len() as u32), (0, 0));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Ординал возвращается к своему документу.
    #[test]
    fn every_ordinal_finds_its_document() {
        let arity: Vec<u32> = (0..500).map(|i| (i % 4) as u32).collect();
        let (dir, b) = build("docof", &arity);
        let s = Slots::open(&crate::container::loose(&dir), 1, b.kind, arity.len(), b.units)
            .expect("полоса");
        let mut unit = 0u32;
        for (d, &a) in arity.iter().enumerate() {
            for _ in 0..a {
                assert_eq!(s.doc_of(unit), Some(d as u32), "ординал {unit}");
                unit += 1;
            }
        }
        assert_eq!(s.doc_of(unit), None, "за последним ординалом документа нет");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Свёртка слотов в документы совпадает с честным перебором.
    #[test]
    fn folding_slots_matches_an_honest_walk() {
        let arity: Vec<u32> = (0..300).map(|i| (i % 5) as u32).collect();
        let (dir, b) = build("fold", &arity);
        let s = Slots::open(&crate::container::loose(&dir), 1, b.kind, arity.len(), b.units)
            .expect("полоса");

        // Зажигаем каждый третий слот.
        let mut slots = vec![0u64; b.units.div_ceil(64)];
        let mut want: Vec<bool> = vec![false; arity.len()];
        for u in (0..b.units as u32).step_by(3) {
            slots[u as usize / 64] |= 1u64 << (u % 64);
            want[s.doc_of(u).expect("документ ординала") as usize] = true;
        }
        let mut out = vec![0u64; arity.len().div_ceil(64)];
        let lit = s.fold(&slots, &mut out);
        assert_eq!(lit, want.iter().filter(|x| **x).count(), "число зажжённых документов");
        for (d, w) in want.iter().enumerate() {
            let got = out[d / 64] >> (d % 64) & 1 == 1;
            assert_eq!(got, *w, "документ {d}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// У корневой группы файлов нет, а ординал равен номеру документа.
    #[test]
    fn the_root_group_costs_nothing() {
        let arity = vec![1u32; 777];
        let b = encode(&arity, "проверка/g1.arity").expect("влезло");
        assert_eq!(b.kind, Kind::Identity);
        assert!(b.idx.is_empty() && b.band.is_empty(), "корневая группа завела файлы");
        assert_eq!(b.units, 777);
        let s = Slots::Identity { docs: 777 };
        assert_eq!(s.bytes(), 0);
        for d in 0..777u32 {
            assert_eq!(s.range(d), (d, 1));
            assert_eq!(s.doc_of(d), Some(d));
        }
        assert_eq!(s.range(777), (0, 0));
        assert_eq!(s.doc_of(777), None);
    }

    /// Блок без единого слота не занимает данных и не ломает нумерацию.
    #[test]
    fn an_empty_block_holds_no_data() {
        // Первые 128 документов без массива вовсе, дальше по два.
        let mut arity = vec![0u32; BLOCK];
        arity.extend(vec![2u32; BLOCK]);
        let (dir, b) = build("empty", &arity);
        let s = Slots::open(&crate::container::loose(&dir), 1, b.kind, arity.len(), b.units)
            .expect("полоса");
        for d in 0..BLOCK as u32 {
            assert_eq!(s.range(d), (0, 0), "документ {d} без массива");
        }
        for d in 0..BLOCK as u32 {
            let doc = BLOCK as u32 + d;
            assert_eq!(s.range(doc), (d * 2, 2), "документ {doc}");
        }
        assert_eq!(s.units(), 2 * BLOCK);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Слотов ровно `u32::MAX` — ещё сегмент; на один больше — отказ.
    ///
    /// ## Что ломалось бы без отказа
    ///
    /// Первый ординал блока лежит в `g*.aidx` четырьмя байтами, и накопитель
    /// `first` тоже `u32`. Сумма арностей за потолком заворачивает его на нуль:
    /// в выпуске, где переполнение не проверяется, — молча. Дальше `range`
    /// отдаёт документу отрезок, начинающийся у чужого документа, и значение
    /// вложенного поля приписывается не тому элементу массива. Ни размера, ни
    /// кратности это не меняет: сегмент выглядит целым и врёт.
    ///
    /// Проверяется настоящим вызовом, а не подставным: вход из двух чисел
    /// подводит счётчик вплотную к границе и переступает её, не требуя ни
    /// четырёх гигабайт памяти, ни четырёх миллиардов документов.
    #[test]
    fn a_group_with_more_slots_than_u32_can_number_is_refused() {
        let at = encode(&[u32::MAX], "seg0007/g1.arity").expect("сам u32::MAX ещё влезает");
        assert_eq!(at.units, u32::MAX as usize, "потолок обязан быть включающим");

        let e = encode(&[u32::MAX, 1], "seg0007/g1.arity")
            .err()
            .expect("на один слот за потолком обязан быть отказ, а не завёрнутый ординал");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let msg = e.to_string();
        // Отказ обязан назвать файл, достигнутую величину и выход: без этого
        // оператору нечего уменьшать.
        assert!(msg.contains("seg0007/g1.arity"), "отказ не назвал файл: {msg}");
        assert!(msg.contains("4294967296"), "отказ не назвал достигнутое: {msg}");
        assert!(msg.contains("4294967295"), "отказ не назвал потолок: {msg}");
        assert!(msg.contains("flush_docs"), "отказ не подсказал выход приёму: {msg}");
        assert!(msg.contains("target_docs"), "отказ не подсказал выход укрупнению: {msg}");
    }

    /// Смещение полосы за четырьмя гигабайтами — отказ, а не завёрнутый указатель.
    ///
    /// Полосу такого размера в тесте не собрать: она требует четырёх гигабайт
    /// памяти. Проверяется само правило и его края — те, на которых оно
    /// перестало бы работать при сдвиге на единицу.
    #[test]
    fn the_band_offset_ceiling_is_inclusive_of_u32_max() {
        assert!(fits_band(0, "seg0007/g1.arity", "полоса").is_ok(), "пустая полоса влезает");
        assert!(
            fits_band(u32::MAX as usize, "seg0007/g1.arity", "полоса").is_ok(),
            "сам u32::MAX представим четырьмя байтами и читателем принимается"
        );
        let e = fits_band(u32::MAX as usize + 1, "seg0007/g1.arity", "полоса")
            .expect_err("на байт за потолком обязан быть отказ");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        assert!(e.to_string().contains("полоса"), "отказ не назвал величину: {e}");
    }

    /// Охрана смещения стоит **на пути записи**, а не рядом с ним.
    ///
    /// ## Чем это отличается от двух проверок выше
    ///
    /// Число слотов закрыто настоящим входом: `encode(&[u32::MAX, 1])` подводит
    /// счётчик к границе двумя числами. Смещению полосы так не повезло — оно
    /// требует четырёх гигабайт, — и оставалось проверенным только как предикат:
    /// снятие вызова `fits_band(band.len(), ...)` из `encode` не роняло ничего.
    ///
    /// Здесь потолок опущен до сотни байт. Вход подобран так, чтобы упёрлось
    /// именно **смещение**: по одному слоту в начале каждого блока даёт ширину в
    /// один бит и семнадцать байт на блок, а слотов у всей группы — по одному на
    /// блок, то есть далеко от потолка. Упрись вместо смещения счётчик слотов —
    /// проверка ловила бы уже закрытую охрану, поэтому величина в отказе названа
    /// явно.
    #[test]
    fn the_band_offset_guard_stands_on_the_write_path() {
        let arity: Vec<u32> = (0..40 * BLOCK).map(|i| u32::from(i.is_multiple_of(BLOCK))).collect();

        // При настоящем потолке та же группа обязана собираться.
        let ok = encode(&arity, "seg0007/g1.arity").expect("обычная группа собирается");
        assert_eq!(ok.kind, Kind::Dense);
        assert_eq!(ok.units, 40, "слотов обязано быть по одному на блок");
        assert!(ok.band.len() > 100, "полоса не переросла опущенный потолок: {}", ok.band.len());

        let _low = crate::dict::lower_offset_ceiling(100);
        let e = match encode(&arity, "seg0007/g1.arity") {
            Ok(_) => panic!("охрана не на пути записи: смещение за потолком легло в указатель"),
            Err(e) => e,
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let msg = e.to_string();
        assert!(msg.contains("полоса"), "упёрлось не смещение, а что-то другое: {msg}");
        assert!(msg.contains("seg0007/g1.arity"), "отказ не назвал файл: {msg}");
        assert!(msg.contains("при потолке 100"), "отказ померил не тем потолком: {msg}");
    }

    /// Обычная группа отказа не даёт: правило не должно мешать работе.
    #[test]
    fn an_ordinary_group_is_never_refused() {
        let arity: Vec<u32> = (0..10_000u32).map(|i| i % 5).collect();
        let b = encode(&arity, "seg0007/g1.arity").expect("обычная группа обязана собраться");
        assert_eq!(b.kind, Kind::Dense);
        assert_eq!(b.units, arity.iter().map(|a| *a as usize).sum::<usize>());
    }
}
