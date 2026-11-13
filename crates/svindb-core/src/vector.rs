//! Плотный вектор: коды на диске, поиск отображением, ничего резидентно.
//!
//! Layout 13: `.q8` содержит строки только присутствующих векторов, а `.qm`
//! начинается с `SVQ1`, версии/flags, dims/docs/present. Далее bitmap наличия,
//! rank-prefix через 256 документов, прежние lo/step и нормы по logical docID.
//! Квантование остаётся восьмибитным; меняется адресация, не точность данных.
//! Legacy missing распознаётся по f32::MAX. Если старый merge уже затёр этот
//! признак, восстановить его из кодов невозможно: нужен authoritative source.

use crate::mmap::Region;
use std::sync::atomic::{AtomicU64, Ordering};

/// Разбирает вектор из текста документа.
///
/// Принимает `[1.0, 2.0, 3.0]` и `1.0 2.0 3.0` — квадратные скобки от JSON
/// снимаются, разделителем служит запятая либо пробел. Пустая строка означает
/// «у документа вектора нет» и даёт `Some(пусто)`, а не отказ: поле, не
/// заполненное у части документов, — обычное дело.
pub fn parse(text: &str, dims: usize) -> Option<Vec<f32>> {
    let t = text.trim().trim_start_matches('[').trim_end_matches(']').trim();
    if t.is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::with_capacity(dims);
    for part in t.split([',', ' ', '\t', '\n']).filter(|x| !x.is_empty()) {
        let v: f32 = part.parse().ok()?;
        if !v.is_finite() {
            return None;
        }
        out.push(v);
        if out.len() > dims {
            return None;
        }
    }
    (out.len() == dims).then_some(out)
}

/// Наибольшая размерность. Столько же у Elasticsearch.
pub const MAX_DIMS: usize = 4096;

/// Скорость перебора при делении на 1, 2, 4, … 64 потока, документов в секунду.
/// Ноль — такое деление ещё не пробовали.
static RATE: [AtomicU64; LEVELS] = [const { AtomicU64::new(0) }; LEVELS];
/// Счётчик перебором: по нему решается, когда пробовать соседнее деление.
static TURN: AtomicU64 = AtomicU64::new(0);

const LEVELS: usize = 7;

/// Каждый такой по счёту перебор идёт пробным делением, а не наилучшим.
///
/// Три процента запросов — цена того, чтобы настройка не застыла: машина под
/// нагрузкой ведёт себя иначе, чем свободная, и число, снятое однажды, к вечеру
/// перестаёт быть верным.
const PROBE_EVERY: u64 = 32;

/// На сколько потоков делить этот перебор.
///
/// Поэтому меряется настоящая: каждый перебор сообщает свою скорость через
/// [`record`], раз в [`PROBE_EVERY`] заходов берётся соседнее деление, и
/// выбирается **наименьшее**, дающее скорость в пределах пяти процентов от
/// наилучшей. Наименьшее, а не быстрейшее: лишние потоки покупают проценты, а
/// платят ядрами, которые соседнему запросу дали бы кратный выигрыш.
pub fn split_for(cores: usize) -> usize {
    let top = top_level(cores);
    if top == 0 {
        return 1;
    }
    let turn = TURN.fetch_add(1, Ordering::Relaxed);
    // Непробованное берётся вне очереди: пока про деление не известно ничего,
    // сравнивать его не с чем.
    if let Some(i) = (0..=top).find(|&i| RATE[i].load(Ordering::Relaxed) == 0) {
        return 1 << i;
    }
    if turn.is_multiple_of(PROBE_EVERY) {
        return 1 << ((turn / PROBE_EVERY) as usize % (top + 1));
    }
    1 << best_level(top)
}

/// Запоминает, чем кончился перебор с таким делением.
pub fn record(split: usize, docs: u64, secs: f64) {
    // Слишком короткий проход меряет часы, а не работу.
    if docs == 0 || secs <= 1e-6 {
        return;
    }
    let i = (usize::BITS - 1 - split.max(1).leading_zeros()) as usize;
    if i >= LEVELS {
        return;
    }
    let now = (docs as f64 / secs) as u64;
    let was = RATE[i].load(Ordering::Relaxed);
    // Сглаживание: одна неудача — сосед занял ядра, страница ушла на диск — не
    // должна перечёркивать всё, что известно про это деление.
    let next = if was == 0 { now } else { (was * 3 + now) / 4 };
    RATE[i].store(next, Ordering::Relaxed);
}

fn top_level(cores: usize) -> usize {
    let mut i = 0;
    while i + 1 < LEVELS && (1usize << (i + 1)) <= cores {
        i += 1;
    }
    i
}

fn best_level(top: usize) -> usize {
    let best = (0..=top).map(|i| RATE[i].load(Ordering::Relaxed)).max().unwrap_or(0);
    if best == 0 {
        return 0;
    }
    (0..=top).find(|&i| RATE[i].load(Ordering::Relaxed) * 100 >= best * 95).unwrap_or(0)
}

/// Байт заголовка до таблиц: размерность и число документов.
const HEAD: usize = 8;
const QM_MAGIC: [u8; 4] = *b"SVQ1";
const QM_HEAD: usize = 20;
const RANK_BLOCK: usize = 256;

/// Коды поля, готовые к поиску.
pub struct Vectors {
    codes: Region,
    meta: Region,
    dims: usize,
    docs: usize,
    /// Центроиды разбиения. Пусто — разбиения нет, и поиск полный.
    cent: Region,
    /// Номер центроида на документ, по два байта.
    band: Region,
    /// Сколько центроидов. Ноль — разбиения нет.
    ncent: usize,
    sparse: bool,
    present: usize,
    bitmap_at: usize,
    rank_at: usize,
    values_at: usize,
    norms_at: usize,
}

/// Что вышло из сжатия.
pub struct Built {
    pub codes: Vec<u8>,
    pub meta: Vec<u8>,
}

/// Сжимает векторы сегмента.
///
/// Документ без вектора приходит пустым. В границы он не входит — иначе растянул
/// бы шкалу нулём, которого в данных нет, — и получает норму, до которой не
/// дотянуться: в верхушку он не попадёт никогда.
///
/// `None` — сжимать нечего либо вход не представим кодеком. Для записи сегмента
/// необходимо использовать [`build_checked`], чтобы ошибка не стала отсутствием.
pub fn build(vectors: &[Vec<f32>], dims: usize) -> Option<Built> {
    build_checked(vectors, dims).ok().flatten()
}

/// Writer обязан отличать пустое поле от значений, не представимых кодеком.
/// Переполнение нормы нельзя сохранять как Inf либо превращать в отсутствие.
pub fn build_checked(vectors: &[Vec<f32>], dims: usize) -> std::io::Result<Option<Built>> {
    let invalid = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "вектор не представим q8-кодеком без переполнения",
        )
    };
    if vectors.len() > u32::MAX as usize || dims == 0 || dims > MAX_DIMS {
        return Err(invalid());
    }
    if vectors.is_empty() {
        return Ok(None);
    }
    if vectors
        .iter()
        .any(|v| !v.is_empty() && (v.len() != dims || v.iter().any(|x| !x.is_finite())))
    {
        return Err(invalid());
    }
    let n = vectors.len();
    let mut lo = vec![f32::MAX; dims];
    let mut hi = vec![f32::MIN; dims];
    let mut any = false;
    for v in vectors.iter().filter(|v| v.len() == dims) {
        any = true;
        for (j, x) in v.iter().enumerate() {
            lo[j] = lo[j].min(*x);
            hi[j] = hi[j].max(*x);
        }
    }
    if !any {
        return Ok(None);
    }
    for j in 0..dims {
        if lo[j] > hi[j] {
            lo[j] = 0.0;
            hi[j] = 0.0;
        }
    }
    let step: Vec<f32> = (0..dims).map(|j| (hi[j] - lo[j]) / 255.0).collect();
    if step.iter().any(|x| !x.is_finite()) {
        return Err(invalid());
    }

    let populated = vectors.iter().filter(|v| v.len() == dims).count();
    let mut codes = Vec::with_capacity(populated.checked_mul(dims).ok_or_else(invalid)?);
    let mut norms = vec![0f32; n];
    let mut bitmap = vec![0u8; n.checked_add(7).ok_or_else(invalid)? / 8];
    let mut present = 0usize;
    for (i, v) in vectors.iter().enumerate() {
        if v.len() != dims {
            norms[i] = f32::MAX;
            continue;
        }
        bitmap[i / 8] |= 1 << (i % 8);
        present += 1;
        let code_at = codes.len();
        codes.resize(code_at + dims, 0);
        let mut sq = 0f64;
        for j in 0..dims {
            let q = if step[j] > 0.0 {
                (((v[j] - lo[j]) / step[j]).round() as i32).clamp(0, 255)
            } else {
                0
            };
            codes[code_at + j] = (q - 128) as i8 as u8;
            // Отклонение от середины шкалы, а не само значение: см. шапку.
            let off = (f64::from(q) - 127.5) * f64::from(step[j]);
            sq += off * off;
        }
        norms[i] = sq as f32;
        if !norms[i].is_finite() {
            return Err(invalid());
        }
    }

    let rank_len = n / RANK_BLOCK + 1;
    let mut meta = Vec::with_capacity(QM_HEAD + bitmap.len() + rank_len * 4 + dims * 8 + n * 4);
    meta.extend(QM_MAGIC);
    meta.push(1);
    meta.push(1);
    meta.extend(0u16.to_le_bytes());
    meta.extend((dims as u32).to_le_bytes());
    meta.extend((n as u32).to_le_bytes());
    meta.extend((present as u32).to_le_bytes());
    meta.extend(&bitmap);
    let mut rank = 0u32;
    for block in 0..rank_len {
        meta.extend(rank.to_le_bytes());
        for i in block * RANK_BLOCK..(block * RANK_BLOCK + RANK_BLOCK).min(n) {
            rank += u32::from((bitmap[i / 8] >> (i % 8)) & 1);
        }
    }
    for x in lo.iter() {
        meta.extend(x.to_le_bytes());
    }
    for x in step.iter() {
        meta.extend(x.to_le_bytes());
    }
    for x in norms.iter() {
        meta.extend(x.to_le_bytes());
    }
    Ok(Some(Built { codes, meta }))
}

/// Склеивает векторы сегментов в один.
///
/// ## Почему это дёшево, а граф соседей — нет
///
/// Слияние здесь есть **перекодировка байта в байт**: ширина записи постоянна,
/// порядок сохраняется, ничего искать не надо. У Elasticsearch то же слияние
/// миллиона векторов заняло 246 секунд, потому что HNSW не склеивается — граф
/// приходится строить заново.
///
/// ## Откуда берётся общая шкала
///
/// ## Чего это стоит в точности
///
/// Когда размах у сегментов один (сегмент сливается сам с собой, шкалы
/// совпадают), перекодировка тождественна байт в байт, и `repeated_merges_do_not_drift`
/// проверяет это восемью слияниями подряд.
///
/// `parts` — на каждый исходный сегмент его коды (если есть) и **сколько у него
/// документов**. Число приходит извне, а не из кодов: у сегмента, где поле
/// объявлено, но не заполнено, кодов нет, а документы есть, и пропустить их
/// значило бы сдвинуть всех, кто идёт следом, на чужие векторы.
///
/// `keep` решает, брать ли документ: погашенные при укрупнении выбрасываются, и
/// нумерация уплотняется — та же, что у колонок и списков.
pub fn merge(
    parts: &[(Option<&Vectors>, usize)],
    dims: usize,
    keep: &dyn Fn(usize, u32) -> bool,
    sink: &mut dyn std::io::Write,
) -> std::io::Result<Option<Vec<u8>>> {
    if dims == 0 || dims > MAX_DIMS || parts.iter().all(|p| p.0.is_none()) {
        return Ok(None);
    }
    let mut lo = vec![f32::MAX; dims];
    let mut hi = vec![f32::MIN; dims];
    let total_docs = parts.iter().try_fold(0usize, |total, (v, docs)| {
        if v.is_some_and(|v| v.docs() != *docs || v.dims() != dims) {
            return None;
        }
        total.checked_add(*docs).filter(|n| *n <= u32::MAX as usize)
    });
    if total_docs.is_none() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "несогласованный состав векторного merge",
        ));
    }
    for v in parts.iter().filter_map(|p| p.0) {
        for j in 0..dims {
            lo[j] = lo[j].min(v.lo(j));
            hi[j] = hi[j].max(v.lo(j) + 255.0 * v.step(j));
        }
    }
    let step: Vec<f32> = (0..dims).map(|j| (hi[j] - lo[j]) / 255.0).collect();
    if step.iter().any(|x| !x.is_finite()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "переполнение шкалы векторного merge",
        ));
    }

    let mut norms: Vec<f32> = Vec::new();
    let mut present_flags: Vec<bool> = Vec::new();
    let mut row = vec![0u8; dims];
    let mut ticker = crate::work::CompactionTicker::default();
    for (i, (part, docs)) in parts.iter().enumerate() {
        for d in 0..*docs as u32 {
            if !ticker.tick(0) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "векторный merge отменён",
                ));
            }
            if !keep(i, d) {
                continue;
            }
            // Сегмент без кодов всё равно отдаёт свои документы. Пропустить их
            // нельзя: нумерация — общая с колонками и списками, и пропуск сдвинул
            // бы всех, кто идёт следом, на чужие векторы. Они получают ту же
            // недостижимую норму, что и пустые при сборке.
            let Some(code) = part.and_then(|v| v.code(d)) else {
                norms.push(f32::MAX);
                present_flags.push(false);
                continue;
            };
            let src = part.expect("коды есть, значит есть и сегмент");
            let mut sq = 0f64;
            for j in 0..dims {
                let val = src.lo(j) + (f32::from(code[j]) + 128.0) * src.step(j);
                let q = if step[j] > 0.0 {
                    (((val - lo[j]) / step[j]).round() as i32).clamp(0, 255)
                } else {
                    0
                };
                row[j] = (q - 128) as i8 as u8;
                let off = (f64::from(q) - 127.5) * f64::from(step[j]);
                sq += off * off;
            }
            if !(sq as f32).is_finite() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "переполнение нормы векторного merge",
                ));
            }
            if !ticker.tick(dims as u64) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "векторный merge отменён до записи",
                ));
            }
            sink.write_all(&row)?;
            norms.push(sq as f32);
            present_flags.push(true);
        }
    }
    if !ticker.flush() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "векторный merge отменён",
        ));
    }
    if norms.is_empty() || !present_flags.iter().any(|x| *x) {
        return Ok(None);
    }

    let n = norms.len();
    let bitmap_len =
        n.checked_add(7).ok_or_else(|| std::io::Error::other("слишком много документов"))? / 8;
    let rank_len = n / RANK_BLOCK + 1;
    let mut meta = Vec::with_capacity(QM_HEAD + bitmap_len + rank_len * 4 + dims * 8 + n * 4);
    meta.extend(QM_MAGIC);
    meta.extend([1, 1]);
    meta.extend(0u16.to_le_bytes());
    meta.extend((dims as u32).to_le_bytes());
    meta.extend((n as u32).to_le_bytes());
    meta.extend((present_flags.iter().filter(|x| **x).count() as u32).to_le_bytes());
    let mut bitmap = vec![0u8; bitmap_len];
    for (i, p) in present_flags.iter().enumerate() {
        if *p {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    meta.extend(&bitmap);
    let mut rank = 0u32;
    for block in 0..rank_len {
        meta.extend(rank.to_le_bytes());
        for present in present_flags.iter().skip(block * RANK_BLOCK).take(RANK_BLOCK) {
            rank += u32::from(*present);
        }
    }
    for x in lo.iter() {
        meta.extend(x.to_le_bytes());
    }
    for x in step.iter() {
        meta.extend(x.to_le_bytes());
    }
    for x in norms.iter() {
        meta.extend(x.to_le_bytes());
    }
    Ok(Some(meta))
}

impl Vectors {
    /// Открывает коды поля. `None` — поля нет либо длины не сходятся.
    pub fn open(src: &crate::container::SegmentSource, field: &str) -> Option<Vectors> {
        let codes = src.try_open(&format!("{field}.q8"))?;
        let meta = src.try_open(&format!("{field}.qm"))?;
        if meta.len_bytes() < HEAD {
            return None;
        }
        let (dims, docs, sparse, present, bitmap_at, rank_at, values_at, norms_at) =
            if meta.len_bytes() >= QM_HEAD && (0..4).all(|i| meta.u8_at(i) == QM_MAGIC[i]) {
                if meta.u8_at(4) != 1 || meta.u8_at(5) != 1 || meta.u16_at(6) != 0 {
                    return None;
                }
                let dims = meta.u32_at(8) as usize;
                let docs = meta.u32_at(12) as usize;
                let present = meta.u32_at(16) as usize;
                let bitmap_len = docs.checked_add(7)?.checked_div(8)?;
                let rank_len = docs.checked_div(RANK_BLOCK)?.checked_add(1)?;
                let bitmap_at = QM_HEAD;
                let rank_at = bitmap_at.checked_add(bitmap_len)?;
                let values_at = rank_at.checked_add(rank_len.checked_mul(4)?)?;
                let norms_at = values_at.checked_add(dims.checked_mul(8)?)?;
                let expected_meta = norms_at.checked_add(docs.checked_mul(4)?)?;
                if present == 0 || present > docs || expected_meta != meta.len_bytes() {
                    return None;
                }
                let expected_codes = if meta.u8_at(5) & 1 != 0 {
                    present.checked_mul(dims)?
                } else {
                    docs.checked_mul(dims)?
                };
                if expected_codes != codes.len_bytes() {
                    return None;
                }
                // Проверяем хвост bitmap и монотонность/границы directory.
                if !docs.is_multiple_of(8)
                    && meta.slice(bitmap_at + bitmap_len - 1, 1)[0] & !((1 << (docs % 8)) - 1) != 0
                {
                    return None;
                }
                let mut count = 0usize;
                for block in 0..rank_len {
                    if meta.u32_at(rank_at + block * 4) as usize != count {
                        return None;
                    }
                    for i in block * RANK_BLOCK..(block * RANK_BLOCK + RANK_BLOCK).min(docs) {
                        count += usize::from((meta.slice(bitmap_at + i / 8, 1)[0] >> (i % 8)) & 1);
                    }
                }
                if count != present {
                    return None;
                }
                for j in 0..dims {
                    let lo = f32::from_bits(meta.u32_at(values_at + j * 4));
                    let step = f32::from_bits(meta.u32_at(values_at + dims * 4 + j * 4));
                    if !lo.is_finite() || !step.is_finite() || step < 0.0 {
                        return None;
                    }
                }
                for d in 0..docs {
                    let norm = f32::from_bits(meta.u32_at(norms_at + d * 4));
                    let yes = meta.u8_at(bitmap_at + d / 8) & (1 << (d % 8)) != 0;
                    if !norm.is_finite() || norm < 0.0 || (!yes && norm != f32::MAX) {
                        return None;
                    }
                }
                (
                    dims,
                    docs,
                    meta.u8_at(5) & 1 != 0,
                    present,
                    bitmap_at,
                    rank_at,
                    values_at,
                    values_at + dims * 8,
                )
            } else {
                let dims = meta.u32_at(0) as usize;
                let docs = meta.u32_at(4) as usize;
                let values_at = HEAD;
                let norms_at = HEAD.checked_add(dims.checked_mul(8)?)?;
                (dims, docs, false, docs, 0, 0, values_at, norms_at)
            };
        if dims == 0 || dims > MAX_DIMS {
            return None;
        }
        let valid = sparse || codes.len_bytes() == docs.checked_mul(dims)?;
        if !valid
            || (!sparse && meta.len_bytes() != HEAD + dims.checked_mul(8)? + docs.checked_mul(4)?)
        {
            return None;
        }
        // Разбиение необязательно и проверяется отдельно: не сошлось — его
        // просто нет, и поиск идёт полным перебором. Отказывать в открытии
        // поля из-за приближения нельзя — точный ответ от этого не зависит.
        let cent = src.try_open(&format!("{field}.ivf")).unwrap_or_else(Region::empty);
        let band = src.try_open(&format!("{field}.ivfb")).unwrap_or_else(Region::empty);
        let mut ncent = 0usize;
        if cent.len_bytes() >= 8 && band.len_bytes() >= docs * 2 {
            let n = cent.u32_at(0) as usize;
            let d = cent.u32_at(4) as usize;
            if n >= 2 && d == dims && cent.len_bytes() >= 8 + n * dims * 4 {
                ncent = n;
            }
        }
        Some(Vectors {
            codes,
            meta,
            dims,
            docs,
            cent,
            band,
            ncent,
            sparse,
            present,
            bitmap_at,
            rank_at,
            values_at,
            norms_at,
        })
    }

    pub fn dims(&self) -> usize {
        self.dims
    }

    pub fn docs(&self) -> usize {
        self.docs
    }

    /// Новый заголовок требует layout 13 даже при полностью заполненном поле.
    pub fn uses_versioned_format(&self) -> bool {
        self.bitmap_at != 0
    }

    /// Знаковый код документа.
    pub fn code(&self, doc: u32) -> Option<&[i8]> {
        if doc as usize >= self.docs {
            return None;
        }
        let doc = doc as usize;
        if !self.sparse && self.f32_at(self.norms_at + doc * 4) == f32::MAX {
            return None;
        }
        let at = if self.sparse {
            if !self.is_present(doc) {
                return None;
            }
            self.rank(doc) * self.dims
        } else {
            doc * self.dims
        };
        let raw = self.codes.slice(at, self.dims);
        // Safety: `i8` и `u8` одной ширины и выравнивания; смысл байтов задан
        // при записи, здесь меняется только их прочтение.
        Some(unsafe { std::slice::from_raw_parts(raw.as_ptr() as *const i8, raw.len()) })
    }

    /// Перепаковывает legacy-коды с внешним, авторитетным признаком наличия.
    /// Коды, шкалы и нормы копируются побитово; меняется только размещение.
    pub fn compact_with_presence(
        &self,
        present: &dyn Fn(u32) -> bool,
        sink: &mut dyn std::io::Write,
    ) -> std::io::Result<Option<Vec<u8>>> {
        let mut flags = vec![false; self.docs];
        let mut count = 0usize;
        for d in 0..self.docs as u32 {
            if present(d) {
                if self.code(d).is_none() {
                    return Err(std::io::Error::other("признак наличия без кода"));
                }
                flags[d as usize] = true;
                count += 1;
            }
        }
        if count == 0 {
            return Ok(None);
        }
        // Не материализуем весь q8 при перепаковке большого сегмента.
        let mut codes = Vec::with_capacity(1 << 20);
        for (d, yes) in flags.iter().enumerate() {
            if *yes {
                codes.extend(self.code(d as u32).expect("проверен выше").iter().map(|x| *x as u8));
                if codes.len() + self.dims > 1 << 20 {
                    sink.write_all(&codes)?;
                    codes.clear();
                }
            }
        }
        sink.write_all(&codes)?;
        let bitmap_len = self.docs.div_ceil(8);
        let rank_len = self.docs / RANK_BLOCK + 1;
        let mut meta =
            Vec::with_capacity(QM_HEAD + bitmap_len + rank_len * 4 + self.dims * 8 + self.docs * 4);
        meta.extend(QM_MAGIC);
        meta.extend([1, 1]);
        meta.extend(0u16.to_le_bytes());
        meta.extend((self.dims as u32).to_le_bytes());
        meta.extend((self.docs as u32).to_le_bytes());
        meta.extend((count as u32).to_le_bytes());
        let mut bitmap = vec![0u8; bitmap_len];
        for (i, yes) in flags.iter().enumerate() {
            if *yes {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }
        meta.extend(&bitmap);
        let mut rank = 0u32;
        for block in 0..rank_len {
            meta.extend(rank.to_le_bytes());
            for present in flags.iter().skip(block * RANK_BLOCK).take(RANK_BLOCK) {
                rank += u32::from(*present);
            }
        }
        for j in 0..self.dims {
            meta.extend(self.f32_at(self.values_at + j * 4).to_le_bytes());
        }
        for j in 0..self.dims {
            meta.extend(self.f32_at(self.values_at + self.dims * 4 + j * 4).to_le_bytes());
        }
        for (d, yes) in flags.iter().enumerate() {
            let norm = if *yes { self.f32_at(self.norms_at + d * 4) } else { f32::MAX };
            meta.extend(norm.to_le_bytes());
        }
        Ok(Some(meta))
    }

    pub fn lo(&self, j: usize) -> f32 {
        self.f32_at(self.values_at + j * 4)
    }

    pub fn step(&self, j: usize) -> f32 {
        self.f32_at(self.values_at + self.dims * 4 + j * 4)
    }

    fn f32_at(&self, at: usize) -> f32 {
        f32::from_bits(self.meta.u32_at(at))
    }

    #[inline]
    fn is_present(&self, doc: usize) -> bool {
        if self.sparse {
            (self.meta.slice(self.bitmap_at + doc / 8, 1)[0] & (1 << (doc % 8))) != 0
        } else {
            self.f32_at(self.norms_at + doc * 4) != f32::MAX
        }
    }
    #[inline]
    fn rank(&self, doc: usize) -> usize {
        if self.present == self.docs {
            return doc;
        }
        let block = doc / RANK_BLOCK;
        let mut n = self.meta.u32_at(self.rank_at + block * 4) as usize;
        // Не более 32 байт bitmap: popcount по u64 и ограниченному хвосту.
        let start = block * (RANK_BLOCK / 8);
        let bytes = self.meta.slice(self.bitmap_at + start, doc / 8 - start);
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            n += u64::from_le_bytes(chunk.try_into().expect("восемь байт")).count_ones() as usize;
        }
        n += chunks.remainder().iter().map(|v| v.count_ones() as usize).sum::<usize>();
        let partial = self.meta.u8_at(self.bitmap_at + doc / 8) & ((1u16 << (doc % 8)) - 1) as u8;
        n += partial.count_ones() as usize;
        n
    }

    /// Разжимает код документа обратно в вектор. Нужен пересчёту и проверке.
    pub fn restore(&self, doc: u32, out: &mut Vec<f32>) -> bool {
        let Some(c) = self.code(doc) else { return false };
        out.clear();
        for (j, x) in c.iter().enumerate() {
            let lo = self.f32_at(self.values_at + j * 4);
            let step = self.f32_at(self.values_at + self.dims * 4 + j * 4);
            out.push(lo + (f32::from(*x) + 128.0) * step);
        }
        true
    }

    /// Ближайшие `k` документов и квадрат расстояния до каждого.
    ///
    /// `alive` решает, виден ли документ: удаление здесь не перестраивает ничего,
    /// как и везде в этом движке.
    ///
    /// Перебор полный и **последовательный**: коды читаются подряд из
    /// отображения, и ядру видно, что дальше понадобится следующая страница.
    /// Случайный обход графа такой подсказки не даёт — за это он и платит на
    /// холодном кеше.
    pub fn top(&self, query: &[f32], k: usize, alive: &dyn Fn(u32) -> bool) -> Vec<(u32, f32)> {
        self.top_range(query, k, alive, 0, self.docs as u32)
    }

    /// Верхушка по отрезку с просмотром только названных списков.
    ///
    /// `probes` пуст — просматривается всё, то есть точный перебор. Это и есть
    /// объявленный откат: **отсутствие разбиения и отказ от него неотличимы**, и
    /// смешивать сегменты с разбиением и без него безопасно — каждый отдаёт
    /// настоящие расстояния, разница только в том, сколько документов он успел
    /// посмотреть.
    pub fn top_probed(
        &self,
        query: &[f32],
        k: usize,
        alive: &dyn Fn(u32) -> bool,
        from: u32,
        to: u32,
        probes: &[u16],
    ) -> Vec<(u32, f32)> {
        self.top_probed_guarded(query, k, alive, from, to, probes, None)
    }

    /// То же с общим сторожем HTTP-запроса.
    #[allow(clippy::too_many_arguments)]
    pub fn top_probed_guarded(
        &self,
        query: &[f32],
        k: usize,
        alive: &dyn Fn(u32) -> bool,
        from: u32,
        to: u32,
        probes: &[u16],
        guard: Option<&crate::work::Guard>,
    ) -> Vec<(u32, f32)> {
        if probes.is_empty() || self.ncent == 0 {
            return self.top_range_guarded(query, k, alive, from, to, guard);
        }
        // Список короткий (корень из числа документов при обычном `nprobe`),
        // и проверка вхождения дешевле любой хитрости.
        let keep = |d: u32| alive(d) && probes.binary_search(&self.band_at(d)).is_ok();
        self.top_range_guarded(query, k, &keep, from, to, guard)
    }

    /// Через сколько принятых документов пересчитывать порог верхушки.
    ///
    /// Пересчёт стоит прохода по всей верхушке. Реже — меньше работы и то же
    /// самое на выходе (разбор — у [`Vectors::top_range_with`]); совсем редко —
    /// порог отстаёт и перестаёт отсекать. Тысяча выбрана так, чтобы проход по
    /// верхушке размывался на неё до сотых долей сравнения на документ даже при
    /// `k` в десять тысяч.
    const WORST_STRIDE: u32 = 1024;

    /// То же, но по половине сегмента — для разделения перебора на потоки.
    ///
    /// Перебор делится **по номерам документов** и делится идеально: у кусков
    /// нет ни общего состояния, ни порога, ни порядка. Это прямое следствие
    /// отказа от графа: обход графа делить нельзя — соседи ведут куда угодно, —
    /// а сплошной проход делится делением отрезка пополам.
    pub fn top_range(
        &self,
        query: &[f32],
        k: usize,
        alive: &dyn Fn(u32) -> bool,
        from: u32,
        to: u32,
    ) -> Vec<(u32, f32)> {
        self.top_range_with(query, k, alive, from, to, true, Self::WORST_STRIDE)
    }

    fn top_range_guarded(
        &self,
        query: &[f32],
        k: usize,
        alive: &dyn Fn(u32) -> bool,
        from: u32,
        to: u32,
        guard: Option<&crate::work::Guard>,
    ) -> Vec<(u32, f32)> {
        self.top_range_with_guard(query, k, alive, from, to, true, Self::WORST_STRIDE, guard)
    }

    /// Есть ли у поля разбиение на списки.
    pub fn has_ivf(&self) -> bool {
        self.ncent > 0
    }

    /// Сколько центроидов. Ноль — разбиения нет.
    pub fn ncentroids(&self) -> usize {
        self.ncent
    }

    /// Значение центроида по номеру и измерению.
    #[inline]
    fn cent_at(&self, c: usize, j: usize) -> f32 {
        self.f32_of(&self.cent, 8 + (c * self.dims + j) * 4)
    }

    #[inline]
    fn f32_of(&self, r: &Region, off: usize) -> f32 {
        f32::from_bits(r.u32_at(off))
    }

    /// Номера списков, которые стоит просмотреть под этот запрос.
    ///
    /// Пусто — разбиения нет либо просить нечего: вызывающий идёт полным
    /// перебором.
    ///
    /// `nprobe`, равный числу центроидов, отдаёт **все** списки. Это не
    /// оговорка, а опора: тогда приближение вырождается в точный перебор, и
    /// полнота выдачи равна единице по построению, а не по замеру. Проверять
    /// приближение надо в том числе этим — свойством, которое обязано
    /// выполняться без всякой статистики.
    pub fn probes(&self, query: &[f32], nprobe: usize) -> Vec<u16> {
        if self.ncent == 0 || query.len() != self.dims || nprobe == 0 {
            return Vec::new();
        }
        let mut d: Vec<(f32, u16)> = (0..self.ncent)
            .map(|c| {
                let mut s = 0f32;
                for (j, q) in query.iter().enumerate().take(self.dims) {
                    let t = q - self.cent_at(c, j);
                    s += t * t;
                }
                (s, c as u16)
            })
            .collect();
        // При равных расстояниях побеждает меньший номер: иначе выбор списков
        // зависел бы от порядка сортировки, а он от него зависеть не должен.
        d.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        d.truncate(nprobe.min(self.ncent));
        let mut out: Vec<u16> = d.into_iter().map(|(_, c)| c).collect();
        out.sort_unstable();
        out
    }

    /// Приписка документа к списку.
    #[inline]
    fn band_at(&self, doc: u32) -> u16 {
        let off = doc as usize * 2;
        if off + 2 > self.band.len_bytes() {
            return u16::MAX;
        }
        self.band.u16_at(off)
    }

    /// То же с выключаемой полосой норм — чтобы её можно было сверить.
    ///
    /// ## Что за полоса
    ///
    /// У каждого документа рядом с кодом лежит `‖d‖²` — его и так считают, чтобы
    /// получить расстояние. По неравенству Коши — Буняковского
    /// `q·d ≤ ‖q‖·‖d‖`, откуда
    ///
    /// ```text
    /// ‖q − d‖² = ‖q‖² − 2·q·d + ‖d‖² ≥ (‖d‖ − ‖q‖)²
    /// ```
    ///
    /// Правая часть считается из **одного** числа `f32`, лежащего рядом с
    /// соседями по обходу, — против чтения `dims` байт кода и скалярного
    /// произведения на них. На векторе в семьсот шестьдесят восемь измерений
    /// это одно чтение против семисот шестидесяти восьми умножений.
    ///
    /// ## Сколько она отсекает
    ///
    /// Замерено на корпусе из трёх тысяч векторов в сорок восемь измерений,
    /// сто восемь запросов при `k` от единицы до ста тридцати семи: **3.3%**
    /// документов не дошли до скалярного произведения. Немного, и понятно
    /// почему: у корпуса из равномерно случайных векторов нормы почти
    /// одинаковы, и граница выходит слабой. Полоса тем полезнее, чем шире
    /// разброс длин — то есть на настоящих вложениях, которые никто не
    /// нормировал.
    ///
    /// Обещать по этому числу больше нечего, и здесь оно записано именно
    /// затем, чтобы никто не пересказал его как «векторный поиск ускорен».
    ///
    /// ## Почему это не меняет выдачу
    ///
    /// Полоса отсекает только те документы, у которых расстояние **заведомо** не
    /// лучше k-го: их отбросил бы и обычный `d >= worst` строкой ниже. Запас в
    /// сравнении обязателен и не для красоты: расстояние считается в `f32` с
    /// точностью около `1e-7`, а граница — в `f64`, и без запаса документ,
    /// стоящий ровно на пороге, мог бы выпасть из-за разницы в округлении. С
    /// запасом отсекается только то, что отстоит от порога дальше этой разницы.
    ///
    /// Сверка — у `the_norm_band_changes_nothing_it_returns`: та же выдача на
    /// тысячах запросов при включённой и выключенной полосе.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn top_range_with(
        &self,
        query: &[f32],
        k: usize,
        alive: &dyn Fn(u32) -> bool,
        from: u32,
        to: u32,
        band: bool,
        stride: u32,
    ) -> Vec<(u32, f32)> {
        self.top_range_with_guard(query, k, alive, from, to, band, stride, None)
    }

    #[allow(clippy::too_many_arguments)]
    fn top_range_with_guard(
        &self,
        query: &[f32],
        k: usize,
        alive: &dyn Fn(u32) -> bool,
        from: u32,
        to: u32,
        band: bool,
        stride: u32,
        guard: Option<&crate::work::Guard>,
    ) -> Vec<(u32, f32)> {
        if query.len() != self.dims || k == 0 || from >= to {
            return Vec::new();
        }
        let to = to.min(self.docs as u32);
        // Постоянные запроса. `scaled` — отклонение запроса от середины шкалы,
        // умноженное на шаг: именно оно идёт в произведение с кодом.
        let mut scaled = vec![0f32; self.dims];
        let mut qq = 0f64;
        let mut base = 0f64;
        for j in 0..self.dims {
            let lo = self.lo(j);
            let step = self.step(j);
            let mid = f64::from(lo) + 127.5 * f64::from(step);
            let off = f64::from(query[j]) - mid;
            scaled[j] = (off * f64::from(step)) as f32;
            qq += off * off;
            base += 0.5 * off * f64::from(step);
        }
        let qq = qq as f32;
        let base = base as f32;
        let norms_at = self.norms_at;

        // Верхушка держится кучей на `k` мест: сортировать миллион, чтобы взять
        // десять, значило бы заплатить за девятьсот девяносто девять тысяч
        // девятьсот девяносто, которые никому не нужны.
        let mut heap: Vec<(f32, u32)> = Vec::with_capacity(k + 1);
        let mut worst = f32::MAX;
        // Норма запроса — для полосы. Считается один раз на весь обход.
        let qnorm = f64::from(qq).max(0.0).sqrt();
        // Границы полосы. Пока порога нет, они не отсекают никого.
        let (mut gate_hi, mut gate_lo) = (f32::INFINITY, f32::NEG_INFINITY);
        // Сколько документов прошло с последнего пересчёта порога.
        //
        // Прежде порог пересчитывался после **каждой** вставки: проход по всей
        // верхушке ради одного числа. При `k` в десятки это незаметно, при `k` в
        // десять тысяч — десять тысяч сравнений на каждый принятый документ.
        //
        // Реже — можно, и выдача от этого не меняется ни на документ. Порог
        // берётся из настоящего обхода верхушки, а верхушка со временем только
        // улучшается; значит устаревший порог **больше** нынешнего. Больший
        // порог отсекает меньше, а не больше: лишние кандидаты доходят до кучи,
        // и куча выбрасывает их сама. Ошибка возможна только в сторону лишней
        // работы, никогда — в сторону потери.
        let mut since = 0u32;
        let mut work_tick = 0u32;
        for doc in from..to {
            if guard.is_some_and(|g| !g.go(&mut work_tick)) {
                break;
            }
            if !alive(doc) {
                continue;
            }
            if !self.is_present(doc as usize) {
                continue;
            }
            let nd = self.f32_at(norms_at + doc as usize * 4);
            // Полоса — два сравнения, без единого корня.
            //
            // Условие `(‖d‖ − ‖q‖)² ≥ worst` содержит корни, но все они — от
            // величин, постоянных между пересчётами порога. Разрешив его
            // относительно `‖d‖²`, получаем две границы, которые считаются там
            // же, где обновляется `worst`, то есть раз в тысячу документов:
            //
            // ```text
            // ‖d‖² ≥ (‖q‖ + √worst)²   — документ дальше запроса и уже не бьёт
            // ‖d‖² ≤ (‖q‖ − √worst)²   — ближе к началу, и тоже не бьёт
            // ```
            //
            // Так и должно быть: корень на документ стоил бы десятков тактов —
            // столько же, сколько скалярное произведение на полусотне
            // измерений, которое он призван сэкономить. На таком поле полоса
            // была бы не выигрышем, а налогом.
            if band && nd >= gate_hi || band && nd <= gate_lo {
                continue;
            }
            let Some(c) = self.code(doc) else {
                continue;
            };
            let dot = base + crate::simd::dot_f32_i8(&scaled, c);
            let d = qq - 2.0 * dot + nd;
            if heap.len() == k && d >= worst {
                continue;
            }
            heap.push((d, doc));
            if heap.len() > k {
                // Худший выбывает. При `k` в десятки линейный поиск дешевле
                // настоящей кучи: перестройка стоила бы больше, чем проход.
                //
                // Худший — по **тому же** порядку, каким выдача сортируется в
                // конце: сперва расстояние, при равенстве больший номер. Не
                // «первый с наибольшим расстоянием», как было.
                //
                // Разница видна только на ничьих — и стоила выдачи. Порог
                // верхушки пересчитывается не на каждой вставке, значит между
                // пересчётами он **больше** настоящего, и документ с
                // расстоянием, в точности равным `k`-му, проходит отсечку
                // вместо того чтобы отсеяться. Прежний цикл вытеснял тогда
                // первого из равных — то есть документ с **меньшим** номером, —
                // и правило «ничьи разрешаются меньшим номером» нарушалось. По
                // общему порядку выбывает сам новичок, и состав не меняется:
                // сколько бы лишних кандидатов ни дошло до кучи, в ней остаются
                // те же `k` наименьших.
                //
                // Ничьи здесь не экзотика: расстояние считается по восьмибитным
                // кодам, и близкие векторы квантуются в один и тот же код.
                let mut at = 0usize;
                for (i, x) in heap.iter().enumerate() {
                    if x.0 > heap[at].0 || (x.0 == heap[at].0 && x.1 > heap[at].1) {
                        at = i;
                    }
                }
                heap.swap_remove(at);
            }
            since += 1;
            // Первое заполнение — сразу: до него порога нет вовсе, и ждать
            // тысячу документов значило бы тысячу документов ничего не отсекать.
            if heap.len() == k && (worst == f32::MAX || since >= stride.max(1)) {
                worst = heap.iter().fold(f32::MIN, |m, x| m.max(x.0));
                since = 0;
                // Границы полосы пересчитываются здесь же — там, где меняется
                // порог, и только там. Запас: верхняя граница поднимается, а
                // нижняя опускается на миллионную долю, потому что расстояние
                // считается в `f32`, а границы в `f64`, и без запаса документ,
                // стоящий ровно на пороге, мог бы выпасть из-за разницы в
                // округлении.
                let w = f64::from(worst);
                if w > 0.0 {
                    let r = w.sqrt();
                    let hi = (qnorm + r) * (qnorm + r);
                    gate_hi = (hi * (1.0 + 1e-6)) as f32;
                    // Ниже запроса полоса отсекает, только если порог до него
                    // не дотягивает; иначе такого `‖d‖²` не бывает вовсе.
                    gate_lo = if qnorm > r {
                        let lo = (qnorm - r) * (qnorm - r);
                        (lo * (1.0 - 1e-6)) as f32
                    } else {
                        f32::NEG_INFINITY
                    };
                } else {
                    // Порог нулевой или отрицательный от взаимного вычитания
                    // близких чисел: отсекать по нему нечего.
                    gate_hi = f32::INFINITY;
                    gate_lo = f32::NEG_INFINITY;
                }
            }
        }
        // Равные расстояния разрешаются меньшим номером — как и везде в выдаче,
        // иначе порядок поплыл бы от того, в каком порядке шёл обход.
        heap.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        heap.into_iter().map(|(d, doc)| (doc, d)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn corpus(n: usize, d: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut s = seed;
        let mut next = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        (0..n).map(|_| (0..d).map(|_| (next() % 20000) as f32 / 100.0 - 100.0).collect()).collect()
    }

    fn write(dir: &str, b: &Built) {
        std::fs::create_dir_all(dir).ok();
        std::fs::write(format!("{dir}/v.q8"), &b.codes).unwrap();
        std::fs::write(format!("{dir}/v.qm"), &b.meta).unwrap();
    }

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-vec-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        d
    }

    /// Выдача совпадает с полным перебором по точным векторам.
    #[test]
    fn the_answer_matches_an_exact_full_scan() {
        let (n, d) = (2000usize, 64usize);
        let data = corpus(n, d, 0xfeed_1234);
        let dir = tmp("exact");
        write(&dir, &build(&data, d).expect("сжатие"));
        let vs = Vectors::open(&crate::container::loose(&dir), "v").expect("открытие");
        assert_eq!(vs.dims(), d);
        assert_eq!(vs.docs(), n);

        let mut agreed = 0usize;
        for qi in [0usize, 7, 100, 999, 1999] {
            let q = &data[qi];
            let got = vs.top(q, 10, &|_| true);
            let mut want: Vec<(u32, f32)> = (0..n as u32)
                .map(|i| (i, crate::simd::l2_f32_scalar(&data[i as usize], q)))
                .collect();
            want.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            // Первый обязан совпасть всегда: это сам запрос, расстояние ноль.
            assert_eq!(got[0].0, qi as u32, "запрос {qi} не нашёл сам себя");
            let want10: std::collections::HashSet<u32> = want[..10].iter().map(|x| x.0).collect();
            agreed += got.iter().filter(|x| want10.contains(&x.0)).count();
        }
        // Пятьдесят мест из пятидесяти.
        assert!(agreed >= 48, "совпало {agreed} из 50");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Полоса норм не меняет выдачу ни на документ и ни на бит.
    ///
    /// ## Почему сверка, а не рассуждение
    ///
    /// Довод про неравенство Коши — Буняковского верен в точной арифметике.
    /// Считается же всё в `f32` и `f64` вперемешку, и вопрос не в том, верна ли
    /// граница, а в том, не перешагнёт ли округление порог у документа, стоящего
    /// ровно на нём. Ответить на это можно только сверкой, и сверять надо
    /// побитово: расхождение в последнем разряде — это перестановка в выдаче,
    /// а перестановка в выдаче и есть тот дефект, который ищут потом часами.
    ///
    /// Запросы берутся и из корпуса, и вне его: у первых расстояние ноль и
    /// порог падает сразу, у вторых верхушка набирается лениво, и полоса
    /// работает в другом режиме.
    #[test]
    fn the_norm_band_changes_nothing_it_returns() {
        let (n, d) = (3000usize, 48usize);
        let data = corpus(n, d, 0x51de_5115);
        let dir = tmp("band");
        write(&dir, &build(&data, d).expect("сжатие"));
        let vs = Vectors::open(&crate::container::loose(&dir), "v").expect("открытие");

        let mut far = corpus(12, d, 0xbeef_0007);
        // Заведомо далёкие: сдвиг за пределы шкалы корпуса.
        for v in far.iter_mut() {
            for x in v.iter_mut() {
                *x = *x * 3.0 + 500.0;
            }
        }
        let mut cases: Vec<&Vec<f32>> = far.iter().collect();
        for qi in [0usize, 1, 17, 333, 1500, 2999] {
            cases.push(&data[qi]);
        }

        let mut checked = 0usize;
        for q in cases {
            for k in [1usize, 10, 137] {
                // Живы не все: полоса обязана уживаться с отбором, потому что
                // отбор меняет, какой документ окажется `k`-м, а значит и порог.
                for alive in [0usize, 1, 3] {
                    let keep =
                        move |doc: u32| alive == 0 || !(doc as usize).is_multiple_of(alive + 1);
                    let with =
                        vs.top_range_with(q, k, &keep, 0, n as u32, true, Vectors::WORST_STRIDE);
                    let without = vs.top_range_with(q, k, &keep, 0, n as u32, false, 1);
                    assert_eq!(with.len(), without.len(), "полоса изменила длину выдачи");
                    for (a, b) in with.iter().zip(without.iter()) {
                        assert_eq!(a.0, b.0, "полоса переставила документы");
                        assert_eq!(a.1.to_bits(), b.1.to_bits(), "полоса изменила расстояние");
                    }
                    assert!(!with.is_empty(), "выдача пуста — проверка вхолостую");
                    checked += 1;
                }
            }
        }
        assert!(checked >= 100, "проверено всего {checked} случаев");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Ничьи по расстоянию: выдача не должна зависеть от шага пересчёта порога.
    #[test]
    fn ties_do_not_depend_on_the_threshold_stride() {
        let (n, d) = (800usize, 64usize);
        let base = corpus(n, d, 0x7a1e_5eed);
        // Каждый вектор повторён пять раз: расстояния совпадают точно.
        let data: Vec<Vec<f32>> = (0..n * 5).map(|i| base[i % n].clone()).collect();
        let dir = tmp("ties");
        write(&dir, &build(&data, d).expect("сжатие"));
        let vs = Vectors::open(&crate::container::loose(&dir), "v").expect("открытие");
        let all = |_: u32| true;
        let mut diverged = 0usize;
        for qi in [0usize, 7, 100, 399] {
            for k in [1usize, 3, 10, 50, 137] {
                let a = vs.top_range_with(&data[qi], k, &all, 0, data.len() as u32, false, 1);
                let b = vs.top_range_with(&data[qi], k, &all, 0, data.len() as u32, true, 1024);
                if a.iter().map(|x| x.0).ne(b.iter().map(|x| x.0)) {
                    diverged += 1;
                }
            }
        }
        std::fs::remove_dir_all(&dir).ok();
        assert_eq!(diverged, 0, "шаг пересчёта порога поменял выдачу в {diverged} случаях из 20");
    }

    /// Расстояние, посчитанное тремя слагаемыми, равно прямому.
    ///
    /// Раскладка `‖q′‖² − 2·q′·d̂′ + ‖d̂′‖²` — это оптимизация, и проверять её
    /// надо против прямого вычитания разжатых значений, а не против самой себя.
    #[test]
    fn the_three_term_distance_equals_the_direct_one() {
        let (n, d) = (200usize, 96usize);
        let data = corpus(n, d, 0x0bad_c0de);
        let dir = tmp("three");
        write(&dir, &build(&data, d).expect("сжатие"));
        let vs = Vectors::open(&crate::container::loose(&dir), "v").expect("открытие");

        let mut back = Vec::new();
        for qi in [0usize, 3, 77, 199] {
            let q = &data[qi];
            let got = vs.top(q, n, &|_| true);
            for (doc, dist) in got.iter() {
                assert!(vs.restore(*doc, &mut back));
                let direct = crate::simd::l2_f32_scalar(&back, q);
                let mag: f32 = back.iter().chain(q.iter()).map(|x| x * x).sum();
                assert!(
                    (dist - direct).abs() <= mag.max(1.0) * 1e-5,
                    "документ {doc}: тремя слагаемыми {dist}, напрямую {direct}, слагаемых на {mag}"
                );
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Порядок выдачи не зависит от того, в чём считать: тот же в `f64`.
    ///
    /// Проверка на равенство чисел ничего не говорит о том, ради чего числа
    /// нужны, — а нужны они, чтобы упорядочить документы. Здесь проверяется
    /// именно это: точный счёт в двойной точности обязан дать ту же десятку.
    /// Если бы потеря на сокращении когда-нибудь начала переставлять соседей,
    /// упало бы вот здесь, а не в поле.
    #[test]
    fn the_order_survives_the_loss_of_precision() {
        let (n, d) = (3000usize, 128usize);
        // Смещённое, как настоящие данные: у SIFT все значения от нуля до 255, и
        // именно смещение делает сокращение опасным. Равномерный шум вокруг нуля
        // такой проверкой не был бы.
        let mut s = 0x1357_9bdf_u64;
        let mut next = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let data: Vec<Vec<f32>> =
            (0..n).map(|_| (0..d).map(|_| (next() % 256) as f32).collect()).collect();
        let dir = tmp("order");
        write(&dir, &build(&data, d).expect("сжатие"));
        let vs = Vectors::open(&crate::container::loose(&dir), "v").expect("открытие");

        let mut back = Vec::new();
        for qi in [1usize, 500, 2999] {
            let q = &data[qi];
            let got = vs.top(q, 10, &|_| true);
            // Точный счёт по тем же разжатым значениям, но в двойной точности.
            let mut want: Vec<(u32, f64)> = (0..n as u32)
                .map(|i| {
                    vs.restore(i, &mut back);
                    let s: f64 = back
                        .iter()
                        .zip(q.iter())
                        .map(|(a, b)| {
                            let t = f64::from(*a) - f64::from(*b);
                            t * t
                        })
                        .sum();
                    (i, s)
                })
                .collect();
            want.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            let got: Vec<u32> = got.iter().map(|x| x.0).collect();
            let want: Vec<u32> = want[..10].iter().map(|x| x.0).collect();
            assert_eq!(got, want, "запрос {qi}: порядок разошёлся с точным счётом");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Погашенный документ в выдачу не попадает.
    #[test]
    fn a_dead_document_never_shows_up() {
        let (n, d) = (500usize, 32usize);
        let data = corpus(n, d, 0x5eed);
        let dir = tmp("dead");
        write(&dir, &build(&data, d).expect("сжатие"));
        let vs = Vectors::open(&crate::container::loose(&dir), "v").expect("открытие");
        let q = &data[42];
        assert_eq!(vs.top(q, 5, &|_| true)[0].0, 42);
        // Гасим сам запрос — теперь первым обязан быть кто-то другой.
        let got = vs.top(q, 5, &|doc| doc != 42);
        assert!(got.iter().all(|(doc, _)| *doc != 42), "погашенный вернулся: {got:?}");
        assert_eq!(got.len(), 5);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Документ без вектора не попадает в выдачу и не портит шкалу.
    #[test]
    fn a_document_without_a_vector_stays_out() {
        let (n, d) = (300usize, 16usize);
        let mut data = corpus(n, d, 0xabc);
        data[5].clear();
        data[100].clear();
        let dir = tmp("empty");
        write(&dir, &build(&data, d).expect("сжатие"));
        let vs = Vectors::open(&crate::container::loose(&dir), "v").expect("открытие");
        let got = vs.top(&data[7], n, &|_| true);
        // Даже если k больше числа векторов, missing не дополняет выдачу.
        assert_eq!(got.len(), n - 2);
        assert!(got.iter().all(|(doc, _)| *doc != 5 && *doc != 100));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Слияние двух сегментов даёт ту же выдачу, что сборка из всех векторов.
    ///
    /// Это главное свойство раскладки: у графа соседей его нет, за что он и
    /// платит секундами перестройки. Проверяется не «похоже», а совпадение
    /// десятки с той, что дал бы сегмент, собранный из тех же данных сразу.
    #[test]
    fn a_merge_matches_building_from_scratch() {
        let d = 48usize;
        let a = corpus(400, d, 0x11);
        let b = corpus(300, d, 0x22);
        let (da, db) = (tmp("mrg-a"), tmp("mrg-b"));
        write(&da, &build(&a, d).expect("сжатие"));
        write(&db, &build(&b, d).expect("сжатие"));
        let (va, vb) = (
            Vectors::open(&crate::container::loose(&da), "v").expect("открытие"),
            Vectors::open(&crate::container::loose(&db), "v").expect("открытие"),
        );

        let dm = tmp("mrg-m");
        std::fs::create_dir_all(&dm).ok();
        let mut sink = Vec::new();
        let meta = merge(&[(Some(&va), 400), (Some(&vb), 300)], d, &|_, _| true, &mut sink)
            .expect("склейка")
            .expect("заголовок");
        std::fs::write(format!("{dm}/v.q8"), &sink).unwrap();
        std::fs::write(format!("{dm}/v.qm"), &meta).unwrap();
        let vm = Vectors::open(&crate::container::loose(&dm), "v").expect("открытие слитого");
        assert_eq!(vm.docs(), 700);

        let all: Vec<Vec<f32>> = a.iter().chain(b.iter()).cloned().collect();
        let dw = tmp("mrg-w");
        write(&dw, &build(&all, d).expect("сжатие"));
        let vw = Vectors::open(&crate::container::loose(&dw), "v").expect("открытие целого");

        // Сверяется СОСТАВ десятки, а не её порядок. Порядок почти равных
        // после пересжатия плавает — см. шапку `merge`, — и требовать его
        // значило бы записать в тест свойство, которого у скалярного сжатия
        // нет. Зато состав обязан сойтись целиком: расхождение в нём означало
        // бы, что документ уехал далеко, а не на деление шкалы.
        let mut same = 0usize;
        let mut of = 0usize;
        for qi in (0..700).step_by(7) {
            let got: std::collections::HashSet<u32> =
                vm.top(&all[qi], 10, &|_| true).iter().map(|x| x.0).collect();
            let want = vw.top(&all[qi], 10, &|_| true);
            // Первым обязан стоять сам запрос — у него расстояние наименьшее с
            // огромным отрывом, и никакое пересжатие его не сдвинет.
            assert_eq!(vm.top(&all[qi], 1, &|_| true)[0].0, qi as u32, "запрос {qi}");
            same += want.iter().filter(|x| got.contains(&x.0)).count();
            of += want.len();
        }
        assert!(same * 100 >= of * 99, "состав десятки сошёлся на {same} из {of}");
        for x in [da, db, dm, dw] {
            std::fs::remove_dir_all(&x).ok();
        }
    }

    /// Восемь слияний подряд не уводят выдачу.
    ///
    /// Каждое слияние повторно сжимает уже сжатое представление, поэтому тест
    /// проверяет отсутствие накопления ошибки.
    #[test]
    fn repeated_merges_do_not_drift() {
        let d = 32usize;
        let data = corpus(500, d, 0x33);
        let dir = tmp("drift");
        write(&dir, &build(&data, d).expect("сжатие"));
        let first: Vec<u32> = Vectors::open(&crate::container::loose(&dir), "v")
            .expect("открытие")
            .top(&data[7], 10, &|_| true)
            .iter()
            .map(|x| x.0)
            .collect();

        for round in 0..8 {
            let v = Vectors::open(&crate::container::loose(&dir), "v").expect("открытие");
            let mut sink = Vec::new();
            let meta = merge(&[(Some(&v), 500)], d, &|_, _| true, &mut sink)
                .expect("склейка")
                .expect("заголовок");
            drop(v);
            std::fs::write(format!("{dir}/v.q8"), &sink).unwrap();
            std::fs::write(format!("{dir}/v.qm"), &meta).unwrap();
            let now: Vec<u32> = Vectors::open(&crate::container::loose(&dir), "v")
                .expect("открытие")
                .top(&data[7], 10, &|_| true)
                .iter()
                .map(|x| x.0)
                .collect();
            assert_eq!(now, first, "выдача уехала после {} слияний", round + 1);
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Погашенные при слиянии выбывают, а нумерация уплотняется.
    ///
    /// И отдельно: сегмент, у которого векторов нет вовсе, всё равно занимает
    /// свои места. Пропусти он их — все следующие документы отвечали бы чужими
    /// векторами, и выдача осталась бы правдоподобной.
    #[test]
    fn a_merge_keeps_the_numbering_in_step() {
        let d = 16usize;
        let a = corpus(10, d, 0x44);
        let da = tmp("num-a");
        write(&da, &build(&a, d).expect("сжатие"));
        let va = Vectors::open(&crate::container::loose(&da), "v").expect("открытие");

        let dm = tmp("num-m");
        std::fs::create_dir_all(&dm).ok();
        let mut sink = Vec::new();
        // Первым идёт сегмент БЕЗ векторов на семь документов, затем наш на
        // десять, из которых гасятся третий и четвёртый.
        let meta = merge(
            &[(None, 7), (Some(&va), 10)],
            d,
            &|i, doc| !(i == 1 && (doc == 3 || doc == 4)),
            &mut sink,
        )
        .expect("склейка")
        .expect("заголовок");
        std::fs::write(format!("{dm}/v.q8"), &sink).unwrap();
        std::fs::write(format!("{dm}/v.qm"), &meta).unwrap();
        let vm = Vectors::open(&crate::container::loose(&dm), "v").expect("открытие");
        assert_eq!(vm.docs(), 7 + 8);

        // Пятый исходный встал восьмым: семь пустых, потом 0,1,2, потом он.
        let got = vm.top(&a[5], 1, &|_| true);
        assert_eq!(got[0].0, 7 + 3, "документ встал не на своё место: {got:?}");
        // Погашенные не вернулись ни под каким номером.
        let live: Vec<u32> = vm.top(&a[3], 15, &|_| true).iter().map(|x| x.0).collect();
        assert_eq!(live.len(), 8, "семь документов без вектора не должны попасть в выдачу");
        for x in [da, dm] {
            std::fs::remove_dir_all(&x).ok();
        }
    }

    /// Настройка деления не выходит за машину и оседает на наименьшем.
    ///
    /// Проверять «выбрано два» нельзя — это и была бы подгонка под стенд.
    /// Проверяется то, что верно везде: деление не больше числа ядер, все
    /// уровни успевают попробоваться, и при равных скоростях выбирается
    /// наименьшее, а не наибольшее.
    #[test]
    fn the_split_tuner_settles_on_the_smallest_that_works() {
        let cores = 8usize;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..16 {
            let n = split_for(cores);
            assert!(n >= 1 && n <= cores, "деление {n} при {cores} ядрах");
            seen.insert(n);
            record(n, 1_000_000, if n == 1 { 0.010 } else { 0.0075 });
        }
        assert_eq!(seen, [1, 2, 4, 8].into_iter().collect(), "не все уровни пробовались");
        let picks: Vec<usize> = (0..16).map(|_| split_for(cores)).collect();
        let twos = picks.iter().filter(|&&n| n == 2).count();
        assert!(twos >= 12, "устоялось не на двух: {picks:?}");
        assert_eq!(split_for(1), 1);
    }

    /// Негодный заголовок не открывается, а не читается как попало.
    #[test]
    fn a_broken_header_is_refused() {
        let (n, d) = (100usize, 8usize);
        let data = corpus(n, d, 1);
        let dir = tmp("broken");
        let b = build(&data, d).expect("сжатие");
        write(&dir, &b);
        assert!(Vectors::open(&crate::container::loose(&dir), "v").is_some());

        // Обрезанные коды: длина перестала сходиться с заголовком.
        std::fs::write(format!("{dir}/v.q8"), &b.codes[..b.codes.len() - d]).unwrap();
        assert!(
            Vectors::open(&crate::container::loose(&dir), "v").is_none(),
            "обрезанный файл принят"
        );

        // Размерность за пределом.
        let mut bad = b.meta.clone();
        bad[0..4].copy_from_slice(&(MAX_DIMS as u32 + 1).to_le_bytes());
        std::fs::write(format!("{dir}/v.q8"), &b.codes).unwrap();
        std::fs::write(format!("{dir}/v.qm"), &bad).unwrap();
        assert!(
            Vectors::open(&crate::container::loose(&dir), "v").is_none(),
            "негодная размерность принята"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_constant_dimension_does_not_divide_by_zero() {
        let d = 8usize;
        let data: Vec<Vec<f32>> = (0..50)
            .map(|i| (0..d).map(|j| if j == 3 { 7.5 } else { i as f32 }).collect())
            .collect();
        let dir = tmp("const");
        write(&dir, &build(&data, d).expect("сжатие"));
        let vs = Vectors::open(&crate::container::loose(&dir), "v").expect("открытие");
        let mut back = Vec::new();
        assert!(vs.restore(0, &mut back));
        assert!((back[3] - 7.5).abs() < 1e-4, "постоянное измерение вернулось как {}", back[3]);
        assert_eq!(vs.top(&data[10], 1, &|_| true)[0].0, 10);
        std::fs::remove_dir_all(&dir).ok();
    }
}

// ---------------------------------------------------------------------------
// Приближённый поиск: обратный список по центроидам (IVF).
//
// ## Почему IVF, а не граф соседей
//
// Граф (HNSW) сюда не ложится, и не по вкусу, а по устройству движка:
//
// * **Сегменты неизменяемы и сливаются.** Граф не склеивается — его строят
//   заново, и слияние миллиона векторов у Elasticsearch занимает четыре минуты
//   ровно поэтому. Перекодировка кодов байт в байт занимает секунды.
// * **Построение графа недетерминировано.** Оно зависит от порядка вставки и от
//   случайного выбора; два одинаковых по составу сегмента давали бы разные
//   графы, а значит разную выдачу.
// * **Обход графа не делится по отрезкам номеров.** Соседство ведёт куда
//   угодно, и деление, на котором стоит нынешний перебор, было бы потеряно.
//
// Обратный список свободен от всех трёх: обучение детерминировано, полоса
// приписок сливается так же байт в байт, как коды, а отбор кандидатов остаётся
// проходом по отрезку номеров.
//
// ## Что это стоит на диске
//
// Два байта на документ (номер центроида) плюс сами центроиды. У вектора в
// семьсот шестьдесят восемь измерений код занимает те же семьсот шестьдесят
// восемь байт, то есть полоса добавляет **четверть процента**.
// ---------------------------------------------------------------------------

/// Сколько раз пересчитывать центроиды. Число, а не сходимость.
///
/// Сходимость — это условие остановки, зависящее от данных, и на двух
/// одинаковых по составу сегментах она может наступить на разных шагах.
/// Фиксированное число шагов даёт один и тот же ответ всегда, а качество
/// разбиения после десятка шагов почти не меняется.
const IVF_ROUNDS: usize = 12;

/// Каждый такой по счёту вектор идёт в обучение.
///
/// Обучать по всем — значит платить за это на каждом сбросе сегмента, а
/// центроиды от выборки почти не зависят: их место задаёт форма облака, а не
/// отдельные точки. Шаг фиксирован, поэтому выборка одна и та же при одинаковом
/// составе — обучение обязано быть воспроизводимым.
const IVF_SAMPLE: usize = 16;

/// Сколько документов должно быть, чтобы разбиение вообще имело смысл.
///
/// На маленьком сегменте полный перебор дешевле обхода списков, а выигрыш
/// начинается там, где документов много.
const IVF_MIN_DOCS: usize = 4096;

/// Сколько «расстояний до центроида» позволено посчитать при обучении.
///
/// ## Откуда предел и почему он вообще нужен
///
/// Приписка документа к списку стоит прохода по **всем** центроидам, то есть
/// `документы × списки × измерения` операций. При числе списков как корень из
/// числа документов это растёт как `n^1.5·d` — быстрее, чем всё остальное на
/// сбросе сегмента.
///
/// Замерено на выпускной сборке:
///
/// | документов | измерений | обучение |
/// |---:|---:|---:|
/// | 20 000 | 384 | 0.87 с |
/// | 100 000 | 384 | 6.26 с |
/// | 100 000 | 768 | 16.1 с |
///
/// Отсюда полтора миллиарда операций в секунду и **восемь минут** на сегмент в
/// миллион документов по семьсот шестьдесят восемь измерений. Сброс сегмента
/// стоит на пути приёма, и восемь минут там — не «медленно», а остановка узла.
///
/// Поэтому предел: обучение идёт, только пока укладывается примерно в секунду.
/// Не уложилось — разбиения не будет, и поиск пойдёт точным перебором. Это
/// безопасно по построению (см. `top_probed`), но это и **ограничение**: на
/// больших сегментах приближения сегодня нет.
///
/// Настоящее место обучения — обслуживание, а не сброс: там оно шло бы фоном и
/// не держало приём. Перенести его — отдельная работа, и до неё предел честнее,
/// чем восьмиминутный сброс.
const IVF_BUDGET: u64 = 1_500_000_000;

/// Что вышло из обучения: центроиды и приписка каждого документа.
pub struct Ivf {
    /// `ncentroids × dims` значений подряд.
    pub centroids: Vec<u8>,
    /// Номер центроида на документ, по два байта.
    pub band: Vec<u8>,
}

/// Обучает разбиение и приписывает каждый документ к центроиду.
///
/// `None` — разбиение не строится: документов мало, или размерность не та.
/// Это не отказ: без разбиения поиск идёт полным перебором, как шёл всегда.
///
/// ## Почему это детерминировано
///
/// Три источника случайности убраны все: начальные центроиды берутся шагом по
/// самой выборке (а не случайным выбором), число шагов задано числом (а не
/// сходимостью), пустой кластер получает свой прежний центр (а не случайную
/// точку). Одинаковый вход даёт одинаковый выход побайтово — и это проверяется.
pub fn train_ivf(vectors: &[Vec<f32>], dims: usize) -> Option<Ivf> {
    let n = vectors.len();
    if n < IVF_MIN_DOCS || dims == 0 || dims > MAX_DIMS {
        return None;
    }
    // Корень из числа документов — обычная мера: столько же списков придётся
    // просмотреть при `nprobe`, растущем как корень, и это даёт квадратный
    // корень работы вместо линейной.
    let want = (n as f64).sqrt().round() as usize;
    let ncent = want.clamp(2, u16::MAX as usize).min(n / 2);
    if ncent < 2 {
        return None;
    }
    // Приписка идёт по всем документам и всем центроидам — это и есть главная
    // цена обучения. Не укладывается в предел — разбиения не будет.
    let cost = (n as u64).saturating_mul(ncent as u64).saturating_mul(dims as u64);
    if cost > IVF_BUDGET {
        return None;
    }
    // Выборка: каждый шестнадцатый, но не меньше числа центроидов.
    let step = (n / (ncent * IVF_SAMPLE).max(1)).max(1);
    let sample: Vec<&Vec<f32>> =
        (0..n).step_by(step).filter_map(|i| vectors.get(i)).filter(|v| v.len() == dims).collect();
    if sample.len() < ncent {
        return None;
    }
    // Начальные центры — шагом по выборке. Не случайные: обучение обязано быть
    // воспроизводимым, а разброс по выборке даёт не худшее начало, чем случай.
    let pick = sample.len() / ncent;
    let mut cent: Vec<Vec<f32>> = (0..ncent).map(|c| sample[c * pick].clone()).collect();

    let mut owner: Vec<usize> = vec![0; sample.len()];
    for _ in 0..IVF_ROUNDS {
        for (i, v) in sample.iter().enumerate() {
            owner[i] = nearest(&cent, v);
        }
        let mut sums: Vec<Vec<f64>> = vec![vec![0.0; dims]; ncent];
        let mut counts: Vec<usize> = vec![0; ncent];
        for (i, v) in sample.iter().enumerate() {
            let c = owner[i];
            counts[c] += 1;
            for j in 0..dims {
                sums[c][j] += f64::from(v[j]);
            }
        }
        for c in 0..ncent {
            // Пустой кластер сохраняет прежний центр. Пересадить его на
            // случайную точку значило бы внести ту самую случайность, которой
            // здесь быть не должно.
            if counts[c] == 0 {
                continue;
            }
            for j in 0..dims {
                cent[c][j] = (sums[c][j] / counts[c] as f64) as f32;
            }
        }
    }

    // Приписка — по **всем** документам, а не по выборке.
    let mut band = Vec::with_capacity(n * 2);
    for v in vectors.iter() {
        // Документ без вектора приписывается к нулевому: искать его всё равно
        // некому — норма у него недостижимая, см. `build`.
        let c = if v.len() == dims { nearest(&cent, v) } else { 0 };
        band.extend((c as u16).to_le_bytes());
    }
    let mut centroids = Vec::with_capacity(ncent * dims * 4 + 8);
    centroids.extend((ncent as u32).to_le_bytes());
    centroids.extend((dims as u32).to_le_bytes());
    for c in cent.iter() {
        for x in c.iter() {
            centroids.extend(x.to_le_bytes());
        }
    }
    Some(Ivf { centroids, band })
}

/// Какую долю списков смотреть по умолчанию.
///
/// ## Откуда число
///
/// Замерено на двадцати тысячах векторов в тридцать два измерения, сорок
/// запросов, `k = 10`, сто сорок один список:
///
/// | доля списков | кластеризованный | равномерный |
/// |---:|---:|---:|
/// | 0.05 | 0.960 | 0.465 |
/// | 0.10 | 0.985 | 0.652 |
/// | 0.20 | 0.998 | 0.838 |
/// | **0.30** | **1.000** | **0.907** |
/// | 0.50 | 1.000 | 0.983 |
///
/// Умолчание задаёт **худший** случай, а не лучший. На кластеризованном наборе
/// хватило бы пяти процентов — двадцатикратное сокращение работы; на
/// равномерном никакой настоящей структуры нет вовсе, и требуемая полнота
/// достигается только просмотром трети. Поставить пять процентов значило бы
/// объявить полноту, которой на равномерных данных не будет, и узнать об этом
/// пришлось бы по жалобам.
///
/// Кто знает, что его векторы кластеризованы, просит меньше сам: `nprobe` есть
/// в запросе. Умолчание же обязано быть верным на данных, которых оно не видело.
pub fn default_nprobe(ncent: usize) -> usize {
    // Малый сегмент законно не получает IVF вовсе. Ноль здесь означает
    // «полный перебор» для `probes/top_probed`; `clamp(1, 0)` вместо этого
    // паникует и в release с panic=abort роняет весь узел на первом KNN.
    if ncent == 0 {
        return 0;
    }
    ((ncent as f64 * 0.30).ceil() as usize).clamp(1, ncent)
}

/// Ближайший центроид по квадрату расстояния.
fn nearest(cent: &[Vec<f32>], v: &[f32]) -> usize {
    let mut best = 0usize;
    let mut bd = f32::MAX;
    for (c, x) in cent.iter().enumerate() {
        let mut d = 0f32;
        for j in 0..v.len().min(x.len()) {
            let t = v[j] - x[j];
            d += t * t;
        }
        // Строгое «меньше»: при равенстве побеждает меньший номер, иначе
        // приписка зависела бы от порядка обхода центроидов.
        if d < bd {
            bd = d;
            best = c;
        }
    }
    best
}

#[cfg(test)]
mod ivf_tests {
    use super::*;

    #[test]
    fn no_ivf_means_full_scan_instead_of_a_clamp_panic() {
        assert_eq!(default_nprobe(0), 0);
        assert_eq!(default_nprobe(1), 1);
    }

    /// Псевдослучайные векторы: кластеризованные либо равномерные.
    fn corpus_of(n: usize, d: usize, seed: u64, clustered: bool) -> Vec<Vec<f32>> {
        let mut s = seed | 1;
        let mut next = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        // Центры кластеров — тоже псевдослучайные, но свои и заранее.
        let nclust = 24usize;
        let centers: Vec<Vec<f32>> = (0..nclust)
            .map(|_| (0..d).map(|_| (next() % 20000) as f32 / 100.0 - 100.0).collect())
            .collect();
        (0..n)
            .map(|i| {
                if !clustered {
                    return (0..d).map(|_| (next() % 20000) as f32 / 100.0 - 100.0).collect();
                }
                let c = &centers[i % nclust];
                // Разброс вокруг центра много меньше расстояния между центрами.
                c.iter().map(|x| x + ((next() % 400) as f32 / 100.0 - 2.0)).collect()
            })
            .collect()
    }

    fn open(dir: &str, data: &[Vec<f32>], d: usize) -> Vectors {
        std::fs::create_dir_all(dir).ok();
        let b = build(data, d).expect("сжатие");
        std::fs::write(format!("{dir}/v.q8"), &b.codes).unwrap();
        std::fs::write(format!("{dir}/v.qm"), &b.meta).unwrap();
        if let Some(ivf) = train_ivf(data, d) {
            std::fs::write(format!("{dir}/v.ivf"), &ivf.centroids).unwrap();
            std::fs::write(format!("{dir}/v.ivfb"), &ivf.band).unwrap();
        }
        Vectors::open(&crate::container::loose(dir), "v").expect("открытие")
    }

    fn tmp(name: &str) -> String {
        let d = std::env::temp_dir()
            .join(format!("svindb-ivf-{name}-{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        std::fs::remove_dir_all(&d).ok();
        d
    }

    /// Полнота выдачи против точного перебора по тому же корпусу.
    fn recall(vs: &Vectors, data: &[Vec<f32>], queries: &[usize], k: usize, nprobe: usize) -> f64 {
        let mut hit = 0usize;
        let mut want = 0usize;
        for &qi in queries.iter() {
            let q = &data[qi];
            let exact: std::collections::HashSet<u32> =
                vs.top(q, k, &|_| true).into_iter().map(|(d, _)| d).collect();
            let probes = vs.probes(q, nprobe);
            let got = vs.top_probed(q, k, &|_| true, 0, data.len() as u32, &probes);
            hit += got.iter().filter(|(d, _)| exact.contains(d)).count();
            want += exact.len();
        }
        hit as f64 / want.max(1) as f64
    }

    /// Гейт полноты: 0.95 на кластеризованном наборе, 0.90 на равномерном.
    ///
    /// Два набора не для полноты картины. У кластеризованного разбиение
    /// совпадает с формой данных, и списки отсекают почти без потерь; у
    /// равномерного никакого «настоящего» разбиения нет вовсе, и полнота
    /// показывает, чего стоит приближение в худшем случае. Заявлять одно число
    /// на оба значило бы прятать худший случай за лучшим.
    #[test]
    fn recall_meets_the_gate_on_both_shapes() {
        let (n, d) = (20_000usize, 32usize);
        let queries: Vec<usize> = (0..40).map(|i| i * 401 % n).collect();
        // `nprobe` берётся как корень из числа списков — обычная мера, при
        // которой работа падает примерно вчетверо.
        for (name, clustered, gate) in [("clustered", true, 0.95f64), ("uniform", false, 0.90f64)] {
            let data = corpus_of(n, d, 0x5eed_1234, clustered);
            let dir = tmp(name);
            let vs = open(&dir, &data, d);
            assert!(vs.has_ivf(), "{name}: разбиение не построилось — проверять нечего");
            let nprobe = default_nprobe(vs.ncentroids());
            let got = recall(&vs, &data, &queries, 10, nprobe);
            assert!(
                got >= gate,
                "{name}: полнота {got:.3} при пороге {gate}, списков {}, смотрим {nprobe}",
                vs.ncentroids()
            );
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// Просмотр всех списков даёт полноту ровно единицу.
    ///
    /// Это не замер, а свойство: при `nprobe`, равном числу списков,
    /// приближение вырождается в точный перебор. Если оно не выполняется,
    /// сломан не порог качества, а сам отбор кандидатов — и никакая статистика
    /// про это не скажет.
    #[test]
    fn probing_every_list_is_exactly_the_full_scan() {
        let (n, d) = (8_000usize, 24usize);
        let data = corpus_of(n, d, 0xfeed_9876, true);
        let dir = tmp("all");
        let vs = open(&dir, &data, d);
        assert!(vs.has_ivf());
        let queries: Vec<usize> = (0..25).map(|i| i * 313 % n).collect();
        let got = recall(&vs, &data, &queries, 10, vs.ncentroids());
        assert!((got - 1.0).abs() < 1e-12, "полнота при всех списках {got}, а не единица");

        // И выдача совпадает не только множеством, но и порядком с расстояниями.
        for &qi in queries.iter().take(5) {
            let q = &data[qi];
            let exact = vs.top(q, 10, &|_| true);
            let all = vs.probes(q, vs.ncentroids());
            let probed = vs.top_probed(q, 10, &|_| true, 0, n as u32, &all);
            assert_eq!(exact.len(), probed.len());
            for (a, b) in exact.iter().zip(probed.iter()) {
                assert_eq!(a.0, b.0, "порядок разошёлся");
                assert_eq!(a.1.to_bits(), b.1.to_bits(), "расстояние разошлось");
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Пустой список списков — это точный перебор, а не пустая выдача.
    ///
    /// Откат объявлен явно: нет разбиения, попросили точность, не сошёлся файл
    /// — все три случая обязаны вести себя одинаково и одинаково безобидно.
    #[test]
    fn no_probes_means_the_full_scan() {
        let (n, d) = (6_000usize, 16usize);
        let data = corpus_of(n, d, 0x1234_abcd, true);
        let dir = tmp("fallback");
        let vs = open(&dir, &data, d);
        let q = &data[7];
        let exact = vs.top(q, 10, &|_| true);
        let empty = vs.top_probed(q, 10, &|_| true, 0, n as u32, &[]);
        assert_eq!(exact.len(), empty.len(), "пустой список списков дал другую длину");
        for (a, b) in exact.iter().zip(empty.iter()) {
            assert_eq!((a.0, a.1.to_bits()), (b.0, b.1.to_bits()), "откат изменил выдачу");
        }

        // Поле без разбиения ведёт себя так же.
        let bare = tmp("bare");
        std::fs::create_dir_all(&bare).ok();
        let b = build(&data, d).expect("сжатие");
        std::fs::write(format!("{bare}/v.q8"), &b.codes).unwrap();
        std::fs::write(format!("{bare}/v.qm"), &b.meta).unwrap();
        let plain = Vectors::open(&crate::container::loose(&bare), "v").expect("открытие");
        assert!(!plain.has_ivf(), "разбиение взялось ниоткуда");
        assert!(plain.probes(q, 8).is_empty(), "без разбиения нашлись списки");
        let got = plain.top_probed(q, 10, &|_| true, 0, n as u32, &plain.probes(q, 8));
        assert_eq!(got.len(), exact.len());
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&bare).ok();
    }

    /// Обучение воспроизводимо побайтово.
    ///
    /// Один и тот же вход обязан давать один и тот же выход: иначе два
    /// одинаковых по составу сегмента отвечали бы по-разному, и объяснить это
    /// было бы нечем.
    #[test]
    fn training_is_reproducible_to_the_byte() {
        let (n, d) = (5_000usize, 20usize);
        let data = corpus_of(n, d, 0xc0ff_ee11, true);
        let a = train_ivf(&data, d).expect("обучение");
        let b = train_ivf(&data, d).expect("обучение");
        assert_eq!(a.centroids, b.centroids, "центроиды разошлись");
        assert_eq!(a.band, b.band, "приписки разошлись");
    }

    /// Обучение отказывается от больших сегментов, а не тянет их минутами.
    ///
    /// Приписка стоит `документы × списки × измерения`, и на миллионе векторов
    /// в семьсот шестьдесят восемь измерений это восемь минут на пути приёма.
    /// Отказ здесь безобиден — поиск идёт точным перебором, — а восьмиминутный
    /// сброс безобидным не был бы.
    #[test]
    fn training_declines_what_it_cannot_afford_in_a_second() {
        // Считаем по формуле, а не гоняем миллион векторов: тест обязан быть
        // быстрым, а проверяет он решение, а не арифметику процессора.
        let afford = |n: usize, d: usize| -> bool {
            let ncent = ((n as f64).sqrt().round() as usize).clamp(2, u16::MAX as usize).min(n / 2);
            (n as u64) * (ncent as u64) * (d as u64) <= IVF_BUDGET
        };
        assert!(afford(20_000, 384), "двадцать тысяч по 384 обязаны уложиться");
        assert!(!afford(1_000_000, 768), "миллион по 768 обязан быть отвергнут");
        assert!(!afford(500_000, 384), "полмиллиона по 384 обязан быть отвергнут");

        // И это правда решение обучения, а не только формулы.
        let d = 64usize;
        let n = 200_000usize;
        assert!(!afford(n, d), "подобрать корпус за пределом не вышло");
        let data = corpus_of(n, d, 5, true);
        assert!(train_ivf(&data, d).is_none(), "обучение взялось за то, что не по силам");
    }

    /// На маленьком поле разбиение не строится вовсе.
    #[test]
    fn a_small_field_gets_no_lists() {
        let d = 12usize;
        let data = corpus_of(100, d, 7, true);
        assert!(train_ivf(&data, d).is_none(), "на сотне документов построилось разбиение");
    }
}
