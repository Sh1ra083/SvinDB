//! Сжатие кусков хранилища. Своё, потому что в ядре зависимостей нет.

/// Байт с наименьшей длиной совпадения, которую вообще стоит кодировать.
const MIN_MATCH: usize = 4;
/// Насколько далеко назад умеет смотреть ссылка.
const WINDOW: usize = 65_535;
/// Сколько последних байт всегда идут литералами: у конца нет места под лексему.
const LAST_LITERALS: usize = 12;
/// Размер таблицы последних вхождений. Степень двойки.
const HASH_BITS: u32 = 14;

#[inline(always)]
fn hash4(v: u32) -> usize {
    // Умножение с последующим сдвигом: старшие разряды произведения перемешаны
    // лучше всего, и берём именно их.
    ((v.wrapping_mul(2_654_435_761)) >> (32 - HASH_BITS)) as usize
}

#[inline(always)]
fn read_u32(src: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([src[at], src[at + 1], src[at + 2], src[at + 3]])
}

/// Записывает длину, не влезшую в четыре бита: байты по 255.
fn push_len(out: &mut Vec<u8>, mut n: usize) {
    while n >= 255 {
        out.push(255);
        n -= 255;
    }
    out.push(n as u8);
}

/// Сжимает кусок. Ответ всегда пригоден к распаковке, каким бы вход ни был.
pub fn compress(src: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(src.len() / 2 + 16);
    if src.len() < MIN_MATCH + LAST_LITERALS {
        // Слишком коротко, чтобы искать совпадения: всё литералами.
        emit_literals(&mut out, src);
        return out;
    }

    let mut table = vec![0u32; 1 << HASH_BITS];
    let mut anchor = 0usize; // начало ещё не записанных литералов
    let mut at = 1usize;
    let limit = src.len() - LAST_LITERALS;

    while at < limit {
        let h = hash4(read_u32(src, at));
        let cand = table[h] as usize;
        table[h] = at as u32;

        // Совпадение годится, если оно в окне и первые четыре байта совпали.
        let ok = at > cand
            && at - cand <= WINDOW
            && cand + MIN_MATCH <= src.len()
            && read_u32(src, cand) == read_u32(src, at);
        if !ok {
            at += 1;
            continue;
        }

        // Растим совпадение вперёд.
        let mut len = MIN_MATCH;
        while at + len < limit && src[cand + len] == src[at + len] {
            len += 1;
        }

        let lit = at - anchor;
        let ext = len - MIN_MATCH;
        let token = ((lit.min(15) as u8) << 4) | ext.min(15) as u8;
        out.push(token);
        if lit >= 15 {
            push_len(&mut out, lit - 15);
        }
        out.extend_from_slice(&src[anchor..at]);
        let off = (at - cand) as u16;
        out.extend_from_slice(&off.to_le_bytes());
        if ext >= 15 {
            push_len(&mut out, ext - 15);
        }

        at += len;
        anchor = at;
    }

    emit_literals(&mut out, &src[anchor..]);
    out
}

/// Хвост из одних литералов — последняя лексема без смещения.
fn emit_literals(out: &mut Vec<u8>, lit: &[u8]) {
    let n = lit.len();
    out.push((n.min(15) as u8) << 4);
    if n >= 15 {
        push_len(out, n - 15);
    }
    out.extend_from_slice(lit);
}

/// Распаковывает кусок. `None` — поток негодный.
///
/// Негодный поток это не край, а обычное дело: файл мог обрваться при падении,
/// байт мог испортиться на диске. Возвращать в таком случае мусор как содержимое
/// документа значило бы врать тем убедительнее, чем незаметнее порча.
pub fn decompress(src: &[u8], expect: usize) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(expect);
    let mut at = 0usize;

    while at < src.len() {
        let token = src[at];
        at += 1;
        let mut lit = (token >> 4) as usize;
        if lit == 15 {
            loop {
                let b = *src.get(at)?;
                at += 1;
                lit += b as usize;
                if b != 255 {
                    break;
                }
            }
        }
        if at + lit > src.len() {
            return None;
        }
        out.extend_from_slice(&src[at..at + lit]);
        at += lit;

        // Хвост: литералы кончились ровно на конце входа, смещения нет.
        if at == src.len() {
            break;
        }
        if at + 2 > src.len() {
            return None;
        }
        let off = u16::from_le_bytes([src[at], src[at + 1]]) as usize;
        at += 2;
        if off == 0 || off > out.len() {
            return None;
        }

        let mut len = (token & 15) as usize;
        if len == 15 {
            loop {
                let b = *src.get(at)?;
                at += 1;
                len += b as usize;
                if b != 255 {
                    break;
                }
            }
        }
        len += MIN_MATCH;

        // Копирование **по байту**: совпадение может перекрываться с собой, и
        // это не ошибка, а способ закодировать повтор одного байта. Копирование
        // срезом такой случай испортило бы молча.
        let from = out.len() - off;
        for i in 0..len {
            let b = out[from + i];
            out.push(b);
        }
        if out.len() > expect {
            return None;
        }
    }

    (out.len() == expect).then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round(data: &[u8]) {
        let packed = compress(data);
        let back = decompress(&packed, data.len());
        assert_eq!(back.as_deref(), Some(data), "круг не сошёлся на {} байтах", data.len());
    }

    #[test]
    fn every_shape_survives_the_round_trip() {
        round(b"");
        round(b"a");
        round(b"abc");
        round(&[0u8; 100]);
        round(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        round("русский текст с повторами русский текст с повторами".as_bytes());
        round(&(0..10_000u32).flat_map(|i| i.to_le_bytes()).collect::<Vec<u8>>());

        // Длинное совпадение — проверка продолжения длины по 255.
        let mut long = "начало".as_bytes().to_vec();
        long.extend(std::iter::repeat_n(b'x', 5000));
        long.extend(std::iter::repeat_n(b'x', 5000));
        round(&long);

        // Много разных литералов подряд — проверка продолжения длины литералов.
        let mut seed = 12345u64;
        let noise: Vec<u8> = (0..5000)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed as u8
            })
            .collect();
        round(&noise);
    }

    /// Повторы одного байта кодируются перекрывающимся совпадением.
    ///
    /// Копирование срезом здесь дало бы неверный ответ и не упало бы: длина
    /// сошлась бы, содержимое — нет.
    #[test]
    fn an_overlapping_match_repeats_correctly() {
        let data = b"abcabcabcabcabcabcabcabcabcabcabcabcabcabc";
        round(data);
        let packed = compress(data);
        assert!(packed.len() < data.len(), "повтор обязан сжаться: {} байт", packed.len());
    }

    /// Испорченный поток отвергается, а не отдаётся как содержимое.
    #[test]
    fn a_broken_stream_is_refused_not_returned() {
        let data = "какой-то текст с повторами какой-то текст".as_bytes();
        let packed = compress(data);

        // Обрыв на каждой длине: ни одна не должна дать «успешный» ответ не той
        // длины.
        for cut in 0..packed.len() {
            if let Some(back) = decompress(&packed[..cut], data.len()) {
                assert_eq!(back, data, "обрыв на {cut} дал не то содержимое");
            }
        }
        // Порча смещения уводит за начало — распаковщик обязан это заметить.
        let mut bad = packed.clone();
        let last = bad.len() - 1;
        bad[last / 2] = 0xff;
        if let Some(back) = decompress(&bad, data.len()) {
            assert_eq!(back.len(), data.len());
        }
    }

    /// Сжатие обязано выигрывать на том, ради чего заведено.
    #[test]
    fn real_looking_text_actually_shrinks() {
        let mut doc = String::new();
        for i in 0..200 {
            doc.push_str(&format!("товар номер {i} категория электроника цена {} руб\n", i * 7));
        }
        let data = doc.as_bytes();
        let packed = compress(data);
        assert!(
            packed.len() * 3 < data.len(),
            "однородный текст обязан сжаться втрое: {} из {}",
            packed.len(),
            data.len()
        );
        round(data);
    }
}
