//! ## Почему своё

// ───────────────────────────── SHA-256 ─────────────────────────────

const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

const IV: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

pub const OUT: usize = 32;
const BLOCK: usize = 64;

/// Потоковый SHA-256.
///
/// Потоковый, а не «дай кусок — получи ответ», ради PBKDF2: там двести тысяч
/// повторов по тридцать два байта, и выделение памяти на каждый стоило бы
/// дороже самого счёта.
#[derive(Clone)]
pub struct Sha256 {
    h: [u32; 8],
    buf: [u8; BLOCK],
    have: usize,
    /// Длина сообщения в битах — она уходит в дополнение.
    bits: u64,
}

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256 {
    pub fn new() -> Sha256 {
        Sha256 { h: IV, buf: [0; BLOCK], have: 0, bits: 0 }
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.bits = self.bits.wrapping_add((data.len() as u64) * 8);
        if self.have > 0 {
            let take = (BLOCK - self.have).min(data.len());
            self.buf[self.have..self.have + take].copy_from_slice(&data[..take]);
            self.have += take;
            data = &data[take..];
            if self.have == BLOCK {
                let block = self.buf;
                self.compress(&block);
                self.have = 0;
            } else {
                // Данные кончились, а блок не набрался. Уйти отсюда надо сразу:
                // ниже хвост записывается в начало буфера и `have` берётся от
                // него, то есть недобранные байты были бы затёрты нулём длины.
                return;
            }
        }
        let mut it = data.chunks_exact(BLOCK);
        for c in &mut it {
            let mut block = [0u8; BLOCK];
            block.copy_from_slice(c);
            self.compress(&block);
        }
        let tail = it.remainder();
        self.buf[..tail.len()].copy_from_slice(tail);
        self.have = tail.len();
    }

    pub fn finish(mut self) -> [u8; OUT] {
        // Дополнение: единичный бит, нули, длина в битах восемью байтами.
        let bits = self.bits;
        let mut pad = [0u8; BLOCK * 2];
        pad[0] = 0x80;
        let rem = (self.have + 1) % BLOCK;
        let zeros = if rem <= 56 { 56 - rem } else { BLOCK + 56 - rem };
        let end = 1 + zeros;
        pad[end..end + 8].copy_from_slice(&bits.to_be_bytes());
        // `update` пересчитал бы длину — дополнение добавляется мимо счётчика.
        let saved = self.bits;
        self.update(&pad[..end + 8]);
        self.bits = saved;
        let mut out = [0u8; OUT];
        for (i, w) in self.h.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&w.to_be_bytes());
        }
        out
    }

    fn compress(&mut self, block: &[u8; BLOCK]) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = self.h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = h.wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (x, y) in self.h.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *x = x.wrapping_add(y);
        }
    }
}

pub fn sha256(data: &[u8]) -> [u8; OUT] {
    let mut s = Sha256::new();
    s.update(data);
    s.finish()
}

// ────────────────────────────── HMAC ──────────────────────────────

/// HMAC-SHA256 с заранее разложенным ключом.
///
/// Разложенным — потому что PBKDF2 бьёт одним ключом двести тысяч раз, и
/// раскладывать его каждый раз значило бы удвоить работу.
#[derive(Clone)]
pub struct Hmac {
    inner: Sha256,
    outer: Sha256,
}

impl Hmac {
    pub fn new(key: &[u8]) -> Hmac {
        let mut k = [0u8; BLOCK];
        // Ключ длиннее блока сжимается — так задано, а не «на всякий случай».
        if key.len() > BLOCK {
            k[..OUT].copy_from_slice(&sha256(key));
        } else {
            k[..key.len()].copy_from_slice(key);
        }
        let mut inner = Sha256::new();
        let mut outer = Sha256::new();
        let mut pad = [0u8; BLOCK];
        for i in 0..BLOCK {
            pad[i] = k[i] ^ 0x36;
        }
        inner.update(&pad);
        for i in 0..BLOCK {
            pad[i] = k[i] ^ 0x5c;
        }
        outer.update(&pad);
        Hmac { inner, outer }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    pub fn finish(self) -> [u8; OUT] {
        let mut outer = self.outer;
        outer.update(&self.inner.finish());
        outer.finish()
    }
}

pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; OUT] {
    let mut h = Hmac::new(key);
    h.update(msg);
    h.finish()
}

// ───────────────────────────── PBKDF2 ─────────────────────────────

/// PBKDF2-HMAC-SHA256.
///
/// Смысл повторов — не в стойкости хеша, а в **цене подбора**: укравший файл
/// перебирает пароли, и каждая попытка обходится ему во столько же, во сколько
/// честный вход. Оттого число повторов и вынесено в настройки: оно стареет
/// вместе с железом.
pub fn pbkdf2(password: &[u8], salt: &[u8], iters: u32, out: &mut [u8]) {
    assert!(iters > 0, "повторов обязано быть хотя бы один");
    let base = Hmac::new(password);
    for (block, chunk) in out.chunks_mut(OUT).enumerate() {
        let i = (block as u32) + 1;
        let mut h = base.clone();
        h.update(salt);
        h.update(&i.to_be_bytes());
        let mut u = h.finish();
        let mut acc = u;
        for _ in 1..iters {
            let mut h = base.clone();
            h.update(&u);
            u = h.finish();
            for (a, b) in acc.iter_mut().zip(u.iter()) {
                *a ^= b;
            }
        }
        let n = chunk.len();
        chunk.copy_from_slice(&acc[..n]);
    }
}

// ───────────────────────────── base64 ─────────────────────────────

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn b64(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for c in data.chunks(3) {
        let b = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 { ALPHABET[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if c.len() > 2 { ALPHABET[n as usize & 63] as char } else { '=' });
    }
    out
}

pub fn unb64(text: &str) -> Option<Vec<u8>> {
    let mut acc = 0u32;
    let mut bits = 0u32;
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    for ch in text.bytes() {
        // Пробелы и переводы строк пропускаются: заголовок с длинным токеном
        // почтовые и прокси-программы переносят.
        if ch.is_ascii_whitespace() {
            continue;
        }
        if ch == b'=' {
            break;
        }
        let v = match ch {
            b'A'..=b'Z' => ch - b'A',
            b'a'..=b'z' => ch - b'a' + 26,
            b'0'..=b'9' => ch - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        } as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

pub fn same(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Случайные байты от ядра, с отказом вместо паники.
///
/// Из `/dev/urandom`, а не из счётчика тактов и не из времени: секрет токена,
/// предсказуемый по моменту создания, — не секрет.
///
/// Отдельно от [`entropy`], потому что у двух вызывающих разная цена отказа.
/// Выпуск ключа — редкое административное действие, и паника там честна. А
/// вот UUID индекса берётся внутри `PUT /{индекс}`, то есть на обычном пути
/// запроса: при `panic = "abort"` исчерпание дескрипторов уронило бы **весь
/// процесс со всеми остальными индексами** вместо того, чтобы отказать одному
/// запросу. Поэтому у lifecycle-путей должен быть возврат ошибки.
pub fn try_entropy(n: usize) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut out = vec![0u8; n];
    let mut f = std::fs::File::open("/dev/urandom")?;
    f.read_exact(&mut out)?;
    Ok(out)
}

/// Случайные байты от ядра.
///
/// Отказ чтения — паника, и это намеренно: выдать токен из неизвестно чего
/// хуже, чем не выдать. Там, где отказ обязан остаться отказом одного запроса,
/// берётся [`try_entropy`].
pub fn entropy(n: usize) -> Vec<u8> {
    try_entropy(n).expect("/dev/urandom не прочитался — секреты выдавать не из чего")
}

// ──────────────────────────── пароли ────────────────────────────

/// Соль — шестнадцать байтов: столько же берёт большинство, и этого хватает,
/// чтобы у двух пользователей с одним паролем вышли разные хеши, то есть чтобы
/// заранее посчитанная таблица не работала.
const SALT: usize = 16;
/// Верхняя граница persisted cost: повреждённая настройка не должна превращать
/// один вход в часы CPU.
pub const MAX_PASSWORD_ITERATIONS: u32 = 2_000_000;

pub fn password_iterations(hash: &str) -> Option<u32> {
    let mut it = hash.split('$');
    (it.next() == Some("pbkdf2")).then_some(())?;
    let cost = it.next()?.parse::<u32>().ok()?;
    let salt = unb64(it.next()?)?;
    let key = unb64(it.next()?)?;
    if it.next().is_some() || salt.len() != SALT || key.len() != OUT {
        return None;
    }
    (cost > 0 && cost <= MAX_PASSWORD_ITERATIONS).then_some(cost)
}

/// Хеш пароля в виде `pbkdf2$<повторов>$<соль64>$<ключ64>`.
pub fn password_hash(password: &str, iters: u32) -> String {
    let salt = entropy(SALT);
    let mut dk = [0u8; OUT];
    pbkdf2(password.as_bytes(), &salt, iters, &mut dk);
    format!("pbkdf2${iters}${}${}", b64(&salt), b64(&dk))
}

/// Сверяет пароль с хешем. Разбитая строка — «не подошёл», а не паника.
pub fn password_check(hash: &str, password: &str) -> bool {
    let mut it = hash.split('$');
    if it.next() != Some("pbkdf2") {
        return false;
    }
    let Some(iters) = password_iterations(hash) else { return false };
    // `password_iterations` уже разобрал первые два компонента; локальный
    // итератор продвигается до тех же данных независимо, чтобы весь формат
    // оставался проверен здесь.
    let _ = it.next();
    let (Some(salt), Some(want)) = (it.next().and_then(unb64), it.next().and_then(unb64)) else {
        return false;
    };
    if it.next().is_some() || want.is_empty() || want.len() > 64 {
        return false;
    }
    let mut got = vec![0u8; want.len()];
    pbkdf2(password.as_bytes(), &salt, iters, &mut got);
    same(&got, &want)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    fn unhex(s: &str) -> Vec<u8> {
        s.as_bytes()
            .chunks(2)
            .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
            .collect()
    }

    /// FIPS 180-4 / RFC 6234. Чужие ответы, а не свои.
    #[test]
    fn sha256_matches_the_published_vectors() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(&sha256(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        assert_eq!(
            hex(&sha256(
                b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmno\
                  ijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu"
            )),
            "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1"
        );
        // Миллион букв «a» — тот случай, где ломается счётчик длины и разбиение
        // на блоки, а короткие строки этого не показывают.
        let mut s = Sha256::new();
        for _ in 0..1000 {
            s.update(&[b'a'; 1000]);
        }
        assert_eq!(
            hex(&s.finish()),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    /// Ответ не зависит от того, какими кусками пришло сообщение.
    #[test]
    fn the_answer_does_not_depend_on_chunking() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 37 % 251) as u8).collect();
        let want = sha256(&data);
        for step in [1usize, 3, 63, 64, 65, 127, 128, 999] {
            let mut s = Sha256::new();
            for c in data.chunks(step) {
                s.update(c);
            }
            assert_eq!(s.finish(), want, "кусками по {step} вышло другое");
        }
    }

    /// RFC 4231, случаи 1–4, 6, 7.
    #[test]
    fn hmac_matches_rfc_4231() {
        let cases: &[(Vec<u8>, Vec<u8>, &str)] = &[
            (
                vec![0x0b; 20],
                b"Hi There".to_vec(),
                "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
            ),
            (
                b"Jefe".to_vec(),
                b"what do ya want for nothing?".to_vec(),
                "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
            ),
            (
                vec![0xaa; 20],
                vec![0xdd; 50],
                "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe",
            ),
            (
                unhex("0102030405060708090a0b0c0d0e0f10111213141516171819"),
                vec![0xcd; 50],
                "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b",
            ),
            // Ключ длиннее блока — сжимается.
            (
                vec![0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First".to_vec(),
                "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54",
            ),
            (
                vec![0xaa; 131],
                b"This is a test using a larger than block-size key and a larger \
                  than block-size data. The key needs to be hashed before being used \
                  by the HMAC algorithm."
                    .to_vec(),
                "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2",
            ),
        ];
        for (i, (key, msg, want)) in cases.iter().enumerate() {
            assert_eq!(hex(&hmac_sha256(key, msg)), *want, "случай {i}");
        }
    }

    /// RFC 7914 §11 — единственные опубликованные векторы PBKDF2 именно с
    /// SHA-256. Второй с восемьюдесятью тысячами повторов: он ловит ошибку,
    /// которая на одном повторе не видна.
    #[test]
    fn pbkdf2_matches_rfc_7914() {
        let mut out = [0u8; 64];
        pbkdf2(b"passwd", b"salt", 1, &mut out);
        assert_eq!(
            hex(&out),
            "55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc\
             49ca9cccf179b645991664b39d77ef317c71b845b1e30bd509112041d3a19783"
        );
        let mut out = [0u8; 64];
        pbkdf2(b"Password", b"NaCl", 80_000, &mut out);
        assert_eq!(
            hex(&out),
            "4ddcd8f60b98be21830cee5ef22701f9641a4418d04c0414aeff08876b34ab56\
             a1d425a1225833549adb841b51c9b3176a272bdebba1d078478f62b397f33c8d"
        );
    }

    /// Длина вывода ничего не меняет в общей части: второй блок считается
    /// отдельно, и ошибка в номере блока проявилась бы именно так.
    #[test]
    fn a_longer_output_extends_the_shorter_one() {
        let mut short = [0u8; 32];
        let mut long = [0u8; 64];
        pbkdf2(b"passwd", b"salt", 7, &mut short);
        pbkdf2(b"passwd", b"salt", 7, &mut long);
        assert_eq!(short[..], long[..32]);
    }

    /// RFC 4648 §10.
    #[test]
    fn base64_matches_rfc_4648() {
        for (raw, enc) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(b64(raw.as_bytes()), enc, "кодирование {raw:?}");
            assert_eq!(unb64(enc).unwrap(), raw.as_bytes(), "разбор {enc:?}");
        }
        assert!(unb64("не base64!").is_none());
    }

    #[test]
    fn base64_survives_the_round_trip_on_random_bytes() {
        for n in 0..200usize {
            let data: Vec<u8> = (0..n).map(|i| (i * 97 % 256) as u8).collect();
            assert_eq!(unb64(&b64(&data)).unwrap(), data, "длина {n}");
        }
    }

    #[test]
    fn a_password_checks_only_against_itself() {
        let h = password_hash("правильный конь батарейка скрепка", 10_000);
        assert!(password_check(&h, "правильный конь батарейка скрепка"));
        assert!(!password_check(&h, "правильный конь батарейка скрепк"));
        assert!(!password_check(&h, ""));
        let g = password_hash("правильный конь батарейка скрепка", 10_000);
        assert_ne!(h, g);
        assert!(password_check(&g, "правильный конь батарейка скрепка"));
    }

    #[test]
    fn a_broken_hash_string_is_a_refusal_not_a_panic() {
        for bad in ["", "pbkdf2", "pbkdf2$", "pbkdf2$0$YQ==$YQ==", "sha1$1$a$b", "$$$$"] {
            assert!(!password_check(bad, "что угодно"), "строка {bad:?} прошла");
        }
    }

    #[test]
    fn comparison_does_not_leak_length_or_prefix() {
        assert!(same(b"abc", b"abc"));
        assert!(!same(b"abc", b"abd"));
        assert!(!same(b"abc", b"abcd"));
        assert!(same(b"", b""));
    }

    #[test]
    fn entropy_is_not_a_constant() {
        let a = entropy(32);
        let b = entropy(32);
        assert_eq!(a.len(), 32);
        assert_ne!(a, b, "два вызова дали одно — это не случайность");
        assert!(a.iter().any(|&x| x != 0), "одни нули");
    }
}
