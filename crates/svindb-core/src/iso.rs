//! Дата ISO-8601 без единой внешней зависимости.
//!
//! ## Почему свой разбор
//!
//! Ядро не имеет зависимостей вовсе, и заводить их ради дат нельзя. Нужного
//! здесь немного: подмножество ISO-8601, которым датируют записи, — и оно
//! считается целочисленной арифметикой, без приближений.
//!
//! ## Чего здесь нет
//!
//! Именованных часовых поясов. Правильная `Europe/Moscow` требует базы
//! `tzdata`, которая живёт и меняется отдельно от программы; посчитать её
//! «примерно» значило бы сдвинуть границу суток на час и выдать неверный
//! отчёт, выглядящий верным. Постоянное смещение вида `+03:00` принимается.

/// Дней от 1970-01-01 до начала указанного дня. Отрицательное — до эпохи.
///
/// Формула Хоуарда Хиннанта: целочисленная, верная и до эпохи, и после, без
/// таблиц и без ветвлений по високосности.
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Обратное: год, месяц, день по числу дней от эпохи.
pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Сколько дней в месяце.
pub fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        _ => 0,
    }
}

/// Разбирает `ГГГГ-ММ-ДД[Tчч:мм:сс[.ммм]][Z|±чч:мм]` в миллисекунды от эпохи.
///
/// Отсутствие времени значит начало суток, отсутствие пояса — UTC. Оба
/// умолчания те же, что у Elasticsearch.
pub fn parse_millis(text: &str) -> Option<i64> {
    let t = text.trim();
    let b = t.as_bytes();
    if b.len() < 10 {
        return None;
    }
    let num = |from: usize, to: usize| -> Option<i64> {
        let part = t.get(from..to)?;
        if !part.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        part.parse().ok()
    };
    if b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let (y, m, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return None;
    }
    let mut millis = days_from_civil(y, m, d).checked_mul(86_400_000)?;
    let mut at = 10usize;
    if at < b.len() && (b[at] == b'T' || b[at] == b' ') {
        at += 1;
        if b.len() < at + 5 || b[at + 2] != b':' {
            return None;
        }
        let (hh, mm) = (num(at, at + 2)?, num(at + 3, at + 5)?);
        if hh > 23 || mm > 59 {
            return None;
        }
        millis += hh * 3_600_000 + mm * 60_000;
        at += 5;
        if at < b.len() && b[at] == b':' {
            let ss = num(at + 1, at + 3)?;
            // Шестьдесят — секунда координации. Она бывает, и отвергать её
            // значило бы терять настоящие записи.
            if ss > 60 {
                return None;
            }
            millis += ss.min(59) * 1000;
            at += 3;
            if at < b.len() && b[at] == b'.' {
                let mut frac = 0i64;
                let mut seen = 0;
                at += 1;
                while at < b.len() && b[at].is_ascii_digit() {
                    if seen < 3 {
                        frac = frac * 10 + (b[at] - b'0') as i64;
                        seen += 1;
                    }
                    at += 1;
                }
                if seen == 0 {
                    return None;
                }
                while seen < 3 {
                    frac *= 10;
                    seen += 1;
                }
                millis += frac;
            }
        }
    }
    // Пояс.
    if at >= b.len() {
        return Some(millis);
    }
    match b[at] {
        b'Z' | b'z' if at + 1 == b.len() => Some(millis),
        b'+' | b'-' => {
            let sign = if b[at] == b'-' { -1 } else { 1 };
            if b.len() != at + 6 || b[at + 3] != b':' {
                return None;
            }
            let (hh, mm) = (num(at + 1, at + 3)?, num(at + 4, at + 6)?);
            if hh > 23 || mm > 59 {
                return None;
            }
            // Смещение **вычитается**: `12:00+03:00` это девять часов UTC.
            Some(millis - sign * (hh * 3_600_000 + mm * 60_000))
        }
        _ => None,
    }
}

/// Обратно в `ГГГГ-ММ-ДДTчч:мм:сс.мммZ`.
pub fn to_text(millis: i64) -> String {
    let days = millis.div_euclid(86_400_000);
    let rest = millis.rem_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    let (hh, mm, ss, ms) = (rest / 3_600_000, rest / 60_000 % 60, rest / 1000 % 60, rest % 1000);
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{ms:03}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Опорные даты сходятся с известными значениями.
    #[test]
    fn known_moments_agree() {
        assert_eq!(parse_millis("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_millis("2024-01-01T00:00:00Z"), Some(1_704_067_200_000));
        // Без времени — начало суток; без пояса — UTC.
        assert_eq!(parse_millis("2024-01-01"), Some(1_704_067_200_000));
        // До эпохи — отрицательное, и это законно.
        assert_eq!(parse_millis("1969-12-31T00:00:00Z"), Some(-86_400_000));
    }

    /// Високосный день существует, а несуществующие даты отвергаются.
    #[test]
    fn a_leap_day_exists_and_impossible_dates_do_not() {
        parse_millis("2024-02-29").expect("високосный день отвергнут");
        assert!(parse_millis("2023-02-29").is_none(), "невисокосное 29 февраля принято");
        assert!(parse_millis("2024-13-01").is_none(), "тринадцатый месяц принят");
        assert!(parse_millis("2024-04-31").is_none(), "тридцать первое апреля принято");
        assert!(parse_millis("2024-00-10").is_none(), "нулевой месяц принят");
    }

    /// Смещение пояса вычитается, а не прибавляется.
    #[test]
    fn an_offset_is_subtracted_not_added() {
        let utc = parse_millis("2024-01-01T12:00:00Z").unwrap();
        let msk = parse_millis("2024-01-01T12:00:00+03:00").unwrap();
        assert_eq!(utc - msk, 3 * 3_600_000, "смещение ушло не в ту сторону");
        let west = parse_millis("2024-01-01T12:00:00-05:00").unwrap();
        assert_eq!(west - utc, 5 * 3_600_000, "западное смещение ушло не в ту сторону");
        // Именованный пояс не принимается: считать его «примерно» значило бы
        // сдвинуть границу суток и выдать неверный отчёт, выглядящий верным.
        assert!(parse_millis("2024-01-01T12:00:00 Europe/Moscow").is_none());
    }

    /// Доли секунды читаются, и лишние разряды не ломают разбор.
    #[test]
    fn fractions_of_a_second_are_read() {
        assert_eq!(parse_millis("2024-01-01T00:00:00.123Z"), Some(1_704_067_200_123));
        assert_eq!(parse_millis("2024-01-01T00:00:00.1Z"), Some(1_704_067_200_100));
        assert_eq!(parse_millis("2024-01-01T00:00:00.123456Z"), Some(1_704_067_200_123));
        assert!(parse_millis("2024-01-01T00:00:00.Z").is_none(), "точка без разрядов принята");
    }

    /// Круг «туда — обратно» точен на широком наборе мгновений.
    #[test]
    fn every_moment_survives_the_round_trip() {
        for ms in [0i64, 1, -1, 1_704_067_200_123, -86_400_000, 253_402_300_799_000] {
            let text = to_text(ms);
            assert_eq!(parse_millis(&text), Some(ms), "круг сломался на {ms} ({text})");
        }
        // И календарь обратим на каждом дне двух високосных лет подряд.
        for d in 0..800i64 {
            let z = days_from_civil(2023, 1, 1) + d;
            let (y, m, day) = civil_from_days(z);
            assert_eq!(days_from_civil(y, m, day), z, "день {z} не обратим");
        }
    }

    /// Мусор отвергается, а не читается наполовину.
    #[test]
    fn rubbish_is_refused() {
        for bad in ["", "2024", "2024-01", "вчера", "2024/01/01", "2024-01-01T25:00:00Z"] {
            assert!(parse_millis(bad).is_none(), "{bad:?} принято");
        }
    }
}
