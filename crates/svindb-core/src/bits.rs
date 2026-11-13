//! Побитовая упаковка блока постингов — обе стороны в одном файле.

/// Документов в блоке. Столько же в индексаторе и в курсоре — формат общий.
pub const BLOCK: usize = 128;

/// Упаковывает дельты в конец `out`, продвигая счётчик записанных байт `bp`.
///
/// `bp` — не длина `out`: буфер сбрасывается на диск порциями, а смещения
/// блоков считаются от начала файла.
pub fn pack(out: &mut Vec<u8>, vals: &[u32], bp: &mut u64) {
    pack_width(out, vals, bp, 1)
}

/// Упаковка, допускающая нулевую ширину.
pub fn pack_values(out: &mut Vec<u8>, vals: &[u32], bp: &mut u64) {
    pack_width(out, vals, bp, 0)
}

fn pack_width(out: &mut Vec<u8>, vals: &[u32], bp: &mut u64, min: u32) {
    let mx = vals.iter().copied().max().unwrap_or(0);
    let width = (32 - mx.leading_zeros()).max(min);
    out.push(width as u8);
    *bp += 1;

    let mut acc: u64 = 0;
    let mut bits = 0u32;
    for &v in vals {
        // После внутреннего цикла `bits` всегда меньше восьми, а `width` не
        // больше тридцати двух, поэтому сдвиг укладывается в 39 бит и в `u64`.
        acc |= (v as u64) << bits;
        bits += width;
        while bits >= 8 {
            out.push((acc & 0xff) as u8);
            acc >>= 8;
            bits -= 8;
            *bp += 1;
        }
    }
    if bits > 0 {
        out.push((acc & 0xff) as u8);
        *bp += 1;
    }
}

/// Сколько байт занимают `n` значений ширины `width` вместе с байтом заголовка.
#[inline(always)]
pub fn packed_len(n: usize, width: u32) -> usize {
    1 + (n * width as usize).div_ceil(8)
}

/// Ширина блока — первый байт.
///
/// Пустой срез даёт единицу, а не ноль: так читатель на обрезанном файле
/// продвигается и упирается в конец списка, а не зацикливается на месте.
#[inline(always)]
pub fn width_of(header: &[u8]) -> u32 {
    header.first().copied().unwrap_or(1) as u32
}

/// Сколько байт запрашивать под блок из `n` значений ширины `width`.
///
/// С запасом в восемь байт: распаковка читает окно в `u64` вокруг каждого
/// значения, и последнее значение блока заходит за его границу.
#[inline(always)]
pub fn window_len(n: usize, width: u32) -> usize {
    (n * width as usize).div_ceil(8) + 8
}

/// Распаковывает `n` дельт ширины `width` и превращает их в номера документов,
/// прибавляя каждую к предыдущему номеру. `base` — последний номер прошлого
/// блока, ноль для первого.
///
/// `src` — данные **без** байта ширины. Короткий `src` не ошибка: хвост файла
/// дочитывается нулями, и лишние значения всё равно отсекаются по числу
/// постингов в списке.
/// Распаковка значений как есть, без накопления.
///
/// Отдельно от [`unpack_deltas`], потому что накопление там не украшение: номера
/// документов хранятся приращениями и без суммы бессмысленны. Частоты, наоборот,
/// самостоятельны, и суммировать их значило бы получить чушь.
///
/// Нулевая ширина — обычное дело, а не край: у подавляющего большинства блоков
/// каждое слово встречается в документе по разу, и весь блок частот занимает
/// **ноль байт**. Ради этого хранится `tf−1`, а не `tf`.
pub fn unpack_values(src: &[u8], n: usize, width: u32, out: &mut [u32]) {
    if width == 0 {
        out[..n].fill(0);
        return;
    }
    let mask: u64 = if width >= 64 { u64::MAX } else { (1u64 << width) - 1 };
    let mut bit = 0usize;
    for slot in out.iter_mut().take(n) {
        let byte = bit >> 3;
        let shift = (bit & 7) as u32;
        let w: u64 = if byte + 8 <= src.len() {
            let mut b = [0u8; 8];
            b.copy_from_slice(&src[byte..byte + 8]);
            u64::from_le_bytes(b)
        } else {
            let mut w = 0u64;
            for j in 0..8usize {
                if let Some(&b) = src.get(byte + j) {
                    w |= (b as u64) << (8 * j);
                }
            }
            w
        };
        *slot = ((w >> shift) & mask) as u32;
        bit += width as usize;
    }
}

/// Одно значение по номеру, без распаковки блока.
#[inline(always)]
pub fn value_at(src: &[u8], width: u32, i: usize) -> u32 {
    if width == 0 {
        return 0;
    }
    let bit = i * width as usize;
    let byte = bit >> 3;
    let shift = (bit & 7) as u32;
    let mask: u64 = if width >= 64 { u64::MAX } else { (1u64 << width) - 1 };
    let w: u64 = if byte + 8 <= src.len() {
        let mut b = [0u8; 8];
        b.copy_from_slice(&src[byte..byte + 8]);
        u64::from_le_bytes(b)
    } else {
        let mut w = 0u64;
        for j in 0..8usize {
            if let Some(&b) = src.get(byte + j) {
                w |= (b as u64) << (8 * j);
            }
        }
        w
    };
    ((w >> shift) & mask) as u32
}

pub fn unpack_deltas(src: &[u8], n: usize, width: u32, base: u32, out: &mut [u32]) {
    let mask: u64 = if width >= 64 { u64::MAX } else { (1u64 << width) - 1 };
    let mut d = base;
    let mut bit = 0usize;
    for slot in out.iter_mut().take(n) {
        let byte = bit >> 3;
        let shift = (bit & 7) as u32;
        // Быстрый путь: восемь байт целиком лежат в срезе. Медленный нужен
        // только у самого конца файла, где срез обрезан по его длине.
        let w: u64 = if byte + 8 <= src.len() {
            let mut b = [0u8; 8];
            b.copy_from_slice(&src[byte..byte + 8]);
            u64::from_le_bytes(b)
        } else {
            let mut w = 0u64;
            for j in 0..8usize {
                if let Some(&b) = src.get(byte + j) {
                    w |= (b as u64) << (8 * j);
                }
            }
            w
        };
        d = d.wrapping_add(((w >> shift) & mask) as u32);
        *slot = d;
        bit += width as usize;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Круговой прогон: что упаковали, то и распаковали.
    fn roundtrip(docs: &[u32], base: u32) {
        let deltas: Vec<u32> = docs
            .iter()
            .scan(base, |prev, &d| {
                let delta = d - *prev;
                *prev = d;
                Some(delta)
            })
            .collect();

        let mut bytes = Vec::new();
        let mut bp = 0u64;
        pack(&mut bytes, &deltas, &mut bp);
        assert_eq!(bp as usize, bytes.len(), "счётчик байт разошёлся с буфером");

        let width = width_of(&bytes);
        assert_eq!(packed_len(deltas.len(), width), bytes.len());

        let mut out = vec![0u32; deltas.len()];
        unpack_deltas(&bytes[1..], deltas.len(), width, base, &mut out);
        assert_eq!(out, docs, "ширина {width}, база {base}");
    }

    #[test]
    fn packs_and_unpacks_dense_list() {
        let docs: Vec<u32> = (0..BLOCK as u32).collect();
        roundtrip(&docs, 0);
    }

    #[test]
    fn packs_and_unpacks_sparse_list() {
        let docs: Vec<u32> = (0..BLOCK as u32).map(|i| i * 100_003).collect();
        roundtrip(&docs, 0);
    }

    /// Блок в середине списка отсчитывается от последнего номера прошлого.
    #[test]
    fn continues_from_previous_block() {
        let docs: Vec<u32> = (0..BLOCK as u32).map(|i| 1_000_000 + i * 7).collect();
        roundtrip(&docs, 999_999);
    }

    /// Все ширины от одной до тридцати двух бит.
    #[test]
    fn every_width_survives() {
        for width in 1..=32u32 {
            let step = if width >= 32 { u32::MAX / 2 } else { (1u32 << width) - 1 };
            let docs: Vec<u32> = (0..17u32).map(|i| i.wrapping_mul(step)).collect();
            let mut sorted = docs.clone();
            sorted.sort_unstable();
            sorted.dedup();
            roundtrip(&sorted, 0);
        }
    }

    /// Неполный блок — обычное дело: последний блок списка почти всегда такой.
    #[test]
    fn partial_block() {
        for n in 1..=BLOCK {
            let docs: Vec<u32> = (0..n as u32).map(|i| i * 3 + 1).collect();
            roundtrip(&docs, 0);
        }
    }

    /// Нулевые дельты (повтор документа) не должны давать нулевую ширину.
    #[test]
    fn zero_deltas_keep_width_one() {
        let mut bytes = Vec::new();
        let mut bp = 0u64;
        pack(&mut bytes, &[0, 0, 0], &mut bp);
        assert_eq!(width_of(&bytes), 1);
        let mut out = [1u32; 3];
        unpack_deltas(&bytes[1..], 3, 1, 42, &mut out);
        assert_eq!(out, [42, 42, 42]);
    }

    /// Обрезанный хвост дочитывается нулями, а не роняет процесс.
    #[test]
    fn truncated_tail_reads_as_zeros() {
        let docs: Vec<u32> = (0..BLOCK as u32).map(|i| i * 9 + 1).collect();
        let deltas: Vec<u32> = docs
            .iter()
            .scan(0u32, |p, &d| {
                let x = d - *p;
                *p = d;
                Some(x)
            })
            .collect();
        let mut bytes = Vec::new();
        let mut bp = 0u64;
        pack(&mut bytes, &deltas, &mut bp);
        let width = width_of(&bytes);

        // Медленный путь обязан дать то же, что быстрый: срез ровно по данным,
        // без восьмибайтового запаса.
        let mut fast = vec![0u32; BLOCK];
        let mut padded = bytes[1..].to_vec();
        padded.extend([0u8; 8]);
        unpack_deltas(&padded, BLOCK, width, 0, &mut fast);
        let mut slow = vec![0u32; BLOCK];
        unpack_deltas(&bytes[1..], BLOCK, width, 0, &mut slow);
        assert_eq!(fast, slow);
        assert_eq!(slow, docs);
    }

    /// Окно запрашивается с запасом ровно в восемь байт.
    #[test]
    fn window_covers_last_value() {
        for width in 1..=32u32 {
            let need = window_len(BLOCK, width);
            assert!(need >= packed_len(BLOCK, width) - 1 + 8);
        }
    }
}
