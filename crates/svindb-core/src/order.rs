//! Порядок у того, что числом не является: адреса и версии.

/// Адрес как пара чисел: старшие 64 бита и младшие.
///
/// ## Почему IPv6, даже когда адрес четвёртой версии
///
/// Так делает Elasticsearch, и причина не в единообразии, а в сравнимости.
/// Держи мы две разные разрядности, отбор «от `10.0.0.0` до `2001:db8::`» не имел
/// бы смысла вовсе, а он осмыслен: это один диапазон в одном пространстве.
/// Адрес четвёртой версии живёт в шестой как `::ffff:a.b.c.d` — так его и
/// записывает `InetAddress` в Lucene.
///
/// ## Почему пара, а не одно число
///
/// Сто двадцать восемь бит в шестьдесят четыре не укладываются, и никакая
/// хитрость этого не изменит. Хранить придётся двумя колонками; здесь же —
/// только преобразование.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Ip {
    pub hi: i64,
    pub lo: i64,
}

impl Ip {
    /// Из шестнадцати байт адреса.
    pub fn from_bytes(b: [u8; 16]) -> Ip {
        let hi = u64::from_be_bytes(b[0..8].try_into().expect("восемь байт"));
        let lo = u64::from_be_bytes(b[8..16].try_into().expect("восемь байт"));
        // Старший разряд переворачивается у обеих половин: беззнаковый порядок
        // превращается в знаковый без потери соседства.
        Ip { hi: (hi ^ (1u64 << 63)) as i64, lo: (lo ^ (1u64 << 63)) as i64 }
    }

    /// Обратно в шестнадцать байт.
    pub fn to_bytes(self) -> [u8; 16] {
        let hi = (self.hi as u64) ^ (1u64 << 63);
        let lo = (self.lo as u64) ^ (1u64 << 63);
        let mut out = [0u8; 16];
        out[0..8].copy_from_slice(&hi.to_be_bytes());
        out[8..16].copy_from_slice(&lo.to_be_bytes());
        out
    }
}

/// Разбирает адрес: `1.2.3.4`, `::1`, `2001:db8::ff00:42:8329`.
///
/// Четвёртая версия отображается в шестую как `::ffff:a.b.c.d`. `None` — не
/// адрес; молча подставлять ноль нельзя, иначе документ без адреса попадёт в
/// отбор «от `0.0.0.0`».
pub fn ip(text: &str) -> Option<Ip> {
    let t = text.trim();
    if t.is_empty() {
        return None;
    }
    if !t.contains(':') {
        return ipv4(t).map(|v| {
            let mut b = [0u8; 16];
            b[10] = 0xff;
            b[11] = 0xff;
            b[12..16].copy_from_slice(&v.to_be_bytes());
            Ip::from_bytes(b)
        });
    }
    ipv6(t).map(Ip::from_bytes)
}

/// Адрес обратно в текст. Четвёртая версия печатается как четвёртая.
pub fn ip_text(v: Ip) -> String {
    let b = v.to_bytes();
    // `::ffff:a.b.c.d` — это адрес четвёртой версии, и печатать его шестой
    // значило бы вернуть не то, что положили.
    if b[0..10].iter().all(|&x| x == 0) && b[10] == 0xff && b[11] == 0xff {
        return format!("{}.{}.{}.{}", b[12], b[13], b[14], b[15]);
    }
    let g: Vec<u16> = (0..8).map(|i| u16::from_be_bytes([b[i * 2], b[i * 2 + 1]])).collect();
    // Самая длинная цепочка нулей схлопывается в `::` — так требует RFC 5952, и
    // без этого запись не совпала бы с тем, что печатают все остальные.
    let (mut best_at, mut best_len) = (usize::MAX, 0usize);
    let (mut at, mut len) = (0usize, 0usize);
    for (i, &w) in g.iter().enumerate() {
        if w == 0 {
            if len == 0 {
                at = i;
            }
            len += 1;
            if len > best_len {
                best_at = at;
                best_len = len;
            }
        } else {
            len = 0;
        }
    }
    if best_len < 2 {
        best_at = usize::MAX;
    }
    let mut out = String::with_capacity(39);
    let mut i = 0usize;
    while i < 8 {
        if i == best_at {
            out.push_str("::");
            i += best_len;
            continue;
        }
        if !out.is_empty() && !out.ends_with(':') {
            out.push(':');
        }
        out.push_str(&format!("{:x}", g[i]));
        i += 1;
    }
    if out.is_empty() {
        out.push_str("::");
    }
    out
}

/// Подсеть: `10.0.0.0/8`, `2001:db8::/32`. Отдаёт границы включительно.
///
/// Подсеть — это диапазон, и отбор по ней ничем не отличается от отбора по
/// паре адресов. Отдельного вида запроса не нужно.
pub fn cidr(text: &str) -> Option<(Ip, Ip)> {
    let (addr, bits) = text.trim().split_once('/')?;
    let bits: u32 = bits.trim().parse().ok()?;
    // Маска считается в пространстве шестой версии, но записана бывает в
    // четвёртой: `/8` у `10.0.0.0` означает восемь бит от начала адреса
    // четвёртой версии, то есть 96 + 8 в шестой.
    let v4 = !addr.contains(':');
    let width = if v4 { 32 } else { 128 };
    if bits > width {
        return None;
    }
    let bits = if v4 { bits + 96 } else { bits };
    let base = ip(addr)?.to_bytes();
    let mut lo = [0u8; 16];
    let mut hi = [0xffu8; 16];
    for i in 0..16 {
        let take = (bits as i32 - (i as i32) * 8).clamp(0, 8) as u32;
        let mask: u8 = if take == 0 { 0 } else { (0xffu16 << (8 - take)) as u8 };
        lo[i] = base[i] & mask;
        hi[i] = (base[i] & mask) | !mask;
    }
    Some((Ip::from_bytes(lo), Ip::from_bytes(hi)))
}

fn ipv4(t: &str) -> Option<u32> {
    let mut out = 0u32;
    let mut parts = 0;
    for p in t.split('.') {
        if parts == 4 || p.is_empty() || p.len() > 3 {
            return None;
        }
        // Ведущий ноль запрещён: `010` в разных местах читают и как десять, и
        // как восемь, и разница видна только на боевом отборе.
        if p.len() > 1 && p.starts_with('0') {
            return None;
        }
        let v: u32 = p.parse().ok()?;
        if v > 255 {
            return None;
        }
        out = (out << 8) | v;
        parts += 1;
    }
    (parts == 4).then_some(out)
}

fn ipv6(t: &str) -> Option<[u8; 16]> {
    // Хвост вида `::ffff:1.2.3.4` — законная запись, и разбирать её надо до
    // групп: иначе точки уедут в шестнадцатеричное число.
    let (head, tail4) = match t.rsplit_once(':') {
        Some((h, last)) if last.contains('.') => (h, Some(ipv4(last)?)),
        _ => (t, None),
    };
    let head = if tail4.is_some() { format!("{head}:") } else { head.to_string() };

    let (left, right) = match head.split_once("::") {
        Some((l, r)) => (l, Some(r)),
        None => (head.as_str(), None),
    };
    let group = |s: &str| -> Option<Vec<u16>> {
        if s.is_empty() {
            return Some(Vec::new());
        }
        s.split(':')
            .filter(|p| !p.is_empty())
            .map(|p| {
                if p.len() > 4 {
                    return None;
                }
                u16::from_str_radix(p, 16).ok()
            })
            .collect()
    };
    let l = group(left)?;
    let r = match right {
        Some(r) => group(r)?,
        None => Vec::new(),
    };
    let tail_groups = if tail4.is_some() { 2 } else { 0 };
    let total = l.len() + r.len() + tail_groups;
    if total > 8 || (right.is_none() && total != 8) {
        return None;
    }
    let mut g = [0u16; 8];
    g[..l.len()].copy_from_slice(&l);
    let start = 8 - r.len() - tail_groups;
    g[start..start + r.len()].copy_from_slice(&r);
    let mut out = [0u8; 16];
    for i in 0..8 {
        out[i * 2..i * 2 + 2].copy_from_slice(&g[i].to_be_bytes());
    }
    if let Some(v4) = tail4 {
        out[12..16].copy_from_slice(&v4.to_be_bytes());
    }
    Some(out)
}

/// Версия как строка байт, сравнимая побайтово.
///
/// ## Почему не число
///
/// Порядок версий не лексикографический и не числовой: `1.10.0` больше `1.9.0`,
/// потому что десять больше девяти, а `1.0.0-alpha` **меньше** `1.0.0`, потому
/// что предварительный выпуск идёт перед готовым. Уложить это в шестьдесят
/// четыре бита без потерь нельзя: предварительная часть — произвольный текст.
///
/// ## Как записано
///
/// Метаданные сборки (`+сборка`) в порядок не входят вовсе — так требует semver,
/// и они отбрасываются.
pub fn version(text: &str) -> Option<Vec<u8>> {
    let t = text.trim();
    if t.is_empty() {
        return None;
    }
    // Метаданные сборки на порядок не влияют.
    let t = t.split('+').next().unwrap_or(t);
    let (rel, pre) = match t.split_once('-') {
        Some((r, p)) => (r, Some(p)),
        None => (t, None),
    };
    let mut out = Vec::with_capacity(t.len() + 8);
    for (i, part) in rel.split('.').enumerate() {
        if part.is_empty() || !part.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        if i > 0 {
            out.push(0x01);
        }
        // Ведущие нули semver запрещает, и мы тоже: иначе `1.01` и `1.1` дали бы
        // разные ключи при равном порядке, и обратное преобразование выбирало бы
        // между ними наугад.
        if part.len() > 1 && part.starts_with('0') {
            return None;
        }
        let n = part.len();
        if n > 255 {
            return None;
        }
        out.push(n as u8);
        out.extend_from_slice(part.as_bytes());
    }
    match pre {
        None => out.push(0x01),
        Some(p) => {
            out.push(0x00);
            if p.is_empty() {
                return None;
            }
            for (i, id) in p.split('.').enumerate() {
                if id.is_empty() {
                    return None;
                }
                if i > 0 {
                    out.push(0x01);
                }
                let numeric = id.bytes().all(|c| c.is_ascii_digit());
                if numeric {
                    if id.len() > 1 && id.starts_with('0') {
                        return None;
                    }
                    out.push(0x00);
                    out.push(id.len() as u8);
                } else {
                    if !id.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-') {
                        return None;
                    }
                    out.push(0x01);
                }
                out.extend_from_slice(id.as_bytes());
            }
            out.push(0x00);
        }
    }
    Some(out)
}

/// Версия обратно в текст. `None` — ключ негоден.
pub fn version_text(key: &[u8]) -> Option<String> {
    let mut out = String::with_capacity(key.len());
    let mut at = 0usize;
    loop {
        let n = *key.get(at)? as usize;
        at += 1;
        let part = std::str::from_utf8(key.get(at..at + n)?).ok()?;
        out.push_str(part);
        at += n;
        match key.get(at)? {
            0x01 if key.len() > at + 1 => {
                out.push('.');
                at += 1;
            }
            0x01 => return (at + 1 == key.len()).then_some(out),
            0x00 => {
                at += 1;
                out.push('-');
                break;
            }
            _ => return None,
        }
    }
    let mut first = true;
    while at < key.len() {
        if *key.get(at)? == 0x00 && at + 1 == key.len() {
            return Some(out);
        }
        if !first {
            if *key.get(at)? != 0x01 {
                return None;
            }
            out.push('.');
            at += 1;
        }
        first = false;
        match key.get(at)? {
            0x00 => {
                at += 1;
                let n = *key.get(at)? as usize;
                at += 1;
                out.push_str(std::str::from_utf8(key.get(at..at + n)?).ok()?);
                at += n;
            }
            0x01 => {
                at += 1;
                // Буквенный участок кончается разделителем или концом ключа.
                let end = key[at..]
                    .iter()
                    .position(|&c| c == 0x00 || c == 0x01)
                    .map(|p| at + p)
                    .unwrap_or(key.len());
                out.push_str(std::str::from_utf8(key.get(at..end)?).ok()?);
                at = end;
            }
            _ => return None,
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ipk(s: &str) -> (i64, i64) {
        let v = ip(s).unwrap_or_else(|| panic!("{s} не разобрался"));
        (v.hi, v.lo)
    }

    /// Порядок ключей обязан совпасть с порядком адресов.
    ///
    /// Пара сравнивается лексикографически: сперва старшая половина, потом
    /// младшая. Тот же класс ошибки, что был у дробных: у беззнакового числа,
    /// положенного в знаковое, половины меняются местами.
    #[test]
    fn addresses_keep_their_order() {
        let ordered = [
            "::",
            "::1",
            "::ffff:0.0.0.0",
            "0.0.0.0",
            "0.0.0.1",
            "1.2.3.4",
            "10.0.0.0",
            "127.0.0.1",
            "192.168.1.1",
            "255.255.255.255",
            "::ffff:255.255.255.255",
            "1::",
            "2001:db8::",
            "2001:db8::1",
            "ffff::",
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        ];
        for w in ordered.windows(2) {
            let (a, b) = (ipk(w[0]), ipk(w[1]));
            assert!(a <= b, "{} ({a:?}) не меньше {} ({b:?})", w[0], w[1]);
        }
        // `0.0.0.0` и `::ffff:0.0.0.0` — один адрес, записанный двумя способами.
        assert_eq!(ipk("0.0.0.0"), ipk("::ffff:0.0.0.0"));
        // Самый большой адрес обязан быть больше самого маленького, а не наоборот.
        assert!(ipk("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff") > ipk("::"));
    }

    /// Адрес возвращается тем же текстом, каким пришёл.
    #[test]
    fn an_address_comes_back_as_itself() {
        for s in [
            "0.0.0.0",
            "1.2.3.4",
            "255.255.255.255",
            "::",
            "::1",
            "1::",
            "2001:db8::1",
            "2001:db8:85a3::8a2e:370:7334",
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            "fe80::1:2:3:4",
        ] {
            let back = ip_text(ip(s).unwrap_or_else(|| panic!("{s} не разобрался")));
            assert_eq!(back, s, "{s} вернулось как {back}");
        }
    }

    /// Мусор не становится нулевым адресом.
    #[test]
    fn what_is_not_an_address_is_refused() {
        for s in [
            "",
            "   ",
            "1.2.3",
            "1.2.3.4.5",
            "256.1.1.1",
            "1.2.3.-1",
            "01.2.3.4",
            "gggg::",
            "1:2:3:4:5:6:7",
            "1:2:3:4:5:6:7:8:9",
            "12345::",
            "::ffff:1.2.3",
            "не адрес",
        ] {
            assert_eq!(ip(s), None, "{s:?} принято за адрес");
        }
    }

    /// Подсеть — это пара границ, и границы обязаны быть верными.
    #[test]
    fn a_subnet_is_a_pair_of_bounds() {
        let (lo, hi) = cidr("10.0.0.0/8").expect("подсеть");
        assert_eq!(ip_text(lo), "10.0.0.0");
        assert_eq!(ip_text(hi), "10.255.255.255");

        let (lo, hi) = cidr("192.168.1.0/24").expect("подсеть");
        assert_eq!(ip_text(lo), "192.168.1.0");
        assert_eq!(ip_text(hi), "192.168.1.255");

        // Одиночный адрес: границы совпадают.
        let (lo, hi) = cidr("1.2.3.4/32").expect("подсеть");
        assert_eq!(lo, hi);
        assert_eq!(ip_text(lo), "1.2.3.4");

        // Вся сеть.
        let (lo, hi) = cidr("0.0.0.0/0").expect("подсеть");
        assert_eq!(ip_text(lo), "0.0.0.0");
        assert_eq!(ip_text(hi), "255.255.255.255");

        let (lo, hi) = cidr("2001:db8::/32").expect("подсеть");
        assert_eq!(ip_text(lo), "2001:db8::");
        assert_eq!(ip_text(hi), "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff");

        // Адрес внутри подсети обязан попасть между границами.
        let inside = ip("10.1.2.3").expect("адрес");
        let (lo, hi) = cidr("10.0.0.0/8").expect("подсеть");
        assert!((lo.hi, lo.lo) <= (inside.hi, inside.lo));
        assert!((inside.hi, inside.lo) <= (hi.hi, hi.lo));
        // А снаружи — нет.
        let outside = ip("11.0.0.1").expect("адрес");
        assert!((outside.hi, outside.lo) > (hi.hi, hi.lo));

        assert_eq!(cidr("10.0.0.0/33"), None);
        assert_eq!(cidr("10.0.0.0"), None);
    }

    /// Порядок версий — тот, что описан в semver, а не лексикографический.
    #[test]
    fn versions_order_the_way_semver_says() {
        // Пример прямо из semver.org, пункт 11.
        let ordered = [
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-alpha.beta",
            "1.0.0-beta",
            "1.0.0-beta.2",
            "1.0.0-beta.11",
            "1.0.0-rc.1",
            "1.0.0",
            "1.0.1",
            "1.9.0",
            "1.10.0",
            "1.11.0",
            "2.0.0",
            "10.0.0",
        ];
        for w in ordered.windows(2) {
            let a = version(w[0]).unwrap_or_else(|| panic!("{} не разобралось", w[0]));
            let b = version(w[1]).unwrap_or_else(|| panic!("{} не разобралось", w[1]));
            assert!(a < b, "{} не меньше {}", w[0], w[1]);
        }
        assert!(version("1.10.0") > version("1.9.0"));
        assert!(version("1.0.0-alpha") < version("1.0.0"));
        assert!(version("1.0.0-2") < version("1.0.0-alpha"));
        // Метаданные сборки в порядок не входят.
        assert_eq!(version("1.0.0+build1"), version("1.0.0+build2"));
        assert_eq!(version("1.0.0+build1"), version("1.0.0"));
    }

    /// Версия возвращается тем же текстом.
    #[test]
    fn a_version_comes_back_as_itself() {
        for s in [
            "1.0.0",
            "0.0.1",
            "10.20.30",
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-0.3.7",
            "1.0.0-x-y-z.1",
            "1.2",
            "7",
            "1.0.0-rc.1",
        ] {
            let k = version(s).unwrap_or_else(|| panic!("{s} не разобралось"));
            let back = version_text(&k).unwrap_or_else(|| panic!("{s}: ключ не читается"));
            assert_eq!(back, s, "{s} вернулось как {back}");
        }
        // Метаданные сборки отброшены намеренно — они не часть порядка.
        let k = version("1.0.0+build").expect("версия");
        assert_eq!(version_text(&k).as_deref(), Some("1.0.0"));
    }

    /// Не версия — не версия.
    #[test]
    fn what_is_not_a_version_is_refused() {
        for s in
            ["", "  ", "1.0.0-", "1..0", "01.0.0", "1.0.0-01", "a.b.c", "1.0.0-альфа", "-1.0.0"]
        {
            assert_eq!(version(s), None, "{s:?} принято за версию");
        }
    }

    /// Порядок держится на случайных версиях, а не только на подобранных.
    #[test]
    fn any_two_versions_compare_the_same_way() {
        fn cmp(a: &str, b: &str) -> std::cmp::Ordering {
            use std::cmp::Ordering;
            let split = |s: &str| -> (Vec<u64>, Option<Vec<String>>) {
                let s = s.split('+').next().unwrap_or(s);
                let (r, p) = match s.split_once('-') {
                    Some((r, p)) => (r, Some(p)),
                    None => (s, None),
                };
                (
                    r.split('.').map(|x| x.parse().expect("число")).collect(),
                    p.map(|p| p.split('.').map(|x| x.to_string()).collect()),
                )
            };
            let (ra, pa) = split(a);
            let (rb, pb) = split(b);
            match ra.cmp(&rb) {
                Ordering::Equal => {}
                o => return o,
            }
            match (&pa, &pb) {
                (None, None) => Ordering::Equal,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some(x), Some(y)) => {
                    for (i, j) in x.iter().zip(y.iter()) {
                        let (ni, nj) = (i.parse::<u64>(), j.parse::<u64>());
                        let o = match (ni, nj) {
                            (Ok(a), Ok(b)) => a.cmp(&b),
                            (Ok(_), Err(_)) => Ordering::Less,
                            (Err(_), Ok(_)) => Ordering::Greater,
                            (Err(_), Err(_)) => i.cmp(j),
                        };
                        if o != Ordering::Equal {
                            return o;
                        }
                    }
                    x.len().cmp(&y.len())
                }
            }
        }

        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let words = ["alpha", "beta", "rc", "x", "dev", "1", "2", "10", "0"];
        let make = |n: &mut dyn FnMut() -> u64| -> String {
            let v = format!("{}.{}.{}", n() % 12, n() % 12, n() % 12);
            match n() % 3 {
                0 => v,
                1 => format!("{v}-{}", words[(n() % 9) as usize]),
                _ => format!("{v}-{}.{}", words[(n() % 9) as usize], words[(n() % 9) as usize]),
            }
        };
        let mut checked = 0usize;
        for _ in 0..20_000 {
            let a = make(&mut next);
            let b = make(&mut next);
            let (ka, kb) = (version(&a).expect(&a), version(&b).expect(&b));
            let want = cmp(&a, &b);
            let got = ka.cmp(&kb);
            // Равные ключи законны только у равных версий.
            if want == std::cmp::Ordering::Equal {
                assert_eq!(got, std::cmp::Ordering::Equal, "{a} и {b}: ключи разошлись");
            } else {
                assert_eq!(got, want, "{a} против {b}: эталон {want:?}, ключи {got:?}");
            }
            checked += 1;
        }
        assert!(checked > 19_000);
    }
}
