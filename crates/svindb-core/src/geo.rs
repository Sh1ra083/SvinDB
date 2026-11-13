//! Точка на земле и расстояние между точками.
//!
//! ## Почему расстояние сферическое, а не планарное
//!
//! Планарное считает градус долготы равным градусу широты. На экваторе это
//! верно, на широте шестьдесят — уже нет: там градус долготы вдвое короче.
//! Ответ при этом остаётся правдоподобным — числа на месте, порядок
//! осмысленный, — и потому неверность никто не заметит. Это ровно тот случай,
//! когда отказ был бы лучше, а верный расчёт — лучше обоих.
//!
//! Земля считается шаром радиуса 6 371 008.8 м — среднего по объёму. Против
//! эллипсоида это даёт до полупроцента ошибки на длинных дугах; для отбора
//! «в пределах километра» и для порядка по близости этого достаточно, и
//! погрешность названа здесь, а не умолчана.

/// Средний по объёму радиус Земли, метры.
pub const R: f64 = 6_371_008.8;

/// Разбирает Base64 обычного алфавита с выравниванием.
///
/// URL-безопасный алфавит не принимается: `-` и `_` вместо `+` и `/` дают
/// **другие байты**, и принять оба алфавита молча значило бы, что одно и то же
/// написание значит разное в зависимости от того, кто прислал. Отказ честнее.
pub fn unb64(text: &str) -> Option<Vec<u8>> {
    let t = text.trim();
    if t.is_empty() {
        return Some(Vec::new());
    }
    if !t.len().is_multiple_of(4) {
        return None;
    }
    let val = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => (c - b'A') as u32,
            b'a'..=b'z' => (c - b'a') as u32 + 26,
            b'0'..=b'9' => (c - b'0') as u32 + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        })
    };
    let bytes = t.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks_exact(4) {
        let pad = chunk.iter().filter(|&&c| c == b'=').count();
        // Выравнивание бывает только в конце и только одним или двумя знаками.
        if pad > 2 || (pad > 0 && chunk[..4 - pad].contains(&b'=')) {
            return None;
        }
        let mut acc = 0u32;
        for &c in chunk.iter().take(4 - pad) {
            acc = (acc << 6) | val(c)?;
        }
        acc <<= 6 * pad;
        let three = acc.to_be_bytes();
        out.push(three[1]);
        if pad < 2 {
            out.push(three[2]);
        }
        if pad < 1 {
            out.push(three[3]);
        }
    }
    Some(out)
}

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Кодирует байты в Base64 обычного алфавита с выравниванием.
pub fn b64(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let mut acc = 0u32;
        for (i, &b) in chunk.iter().enumerate() {
            acc |= (b as u32) << (16 - 8 * i);
        }
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[((acc >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Точка: широта и долгота в градусах.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Point {
    pub lat: f64,
    pub lon: f64,
}

impl Point {
    /// Проверяет пределы. За ними — отказ, а не приведение к границе.
    pub fn new(lat: f64, lon: f64) -> Result<Point, String> {
        if !lat.is_finite() || !lon.is_finite() {
            return Err("координата не число".to_string());
        }
        if !(-90.0..=90.0).contains(&lat) {
            return Err(format!("широта {lat} вне пределов ±90"));
        }
        if !(-180.0..=180.0).contains(&lon) {
            return Err(format!("долгота {lon} вне пределов ±180"));
        }
        Ok(Point { lat, lon })
    }

    /// Разбирает `"широта,долгота"` — вид, в котором точка приходит строкой.
    ///
    /// Массив `[долгота, широта]` здесь **не** принимается: у строки и массива
    /// порядок обратный, и принять оба значило бы, что запись `[55, 37]` значит
    /// разное в зависимости от догадки. Приём переводит массив в строку сам,
    /// зная, откуда тот пришёл.
    pub fn parse(text: &str) -> Result<Point, String> {
        let (lat, lon) = text
            .split_once(',')
            .ok_or_else(|| format!("{text:?}: точка пишется «широта,долгота»"))?;
        let lat: f64 = lat.trim().parse().map_err(|_| format!("широта {lat:?} не число"))?;
        let lon: f64 = lon.trim().parse().map_err(|_| format!("долгота {lon:?} не число"))?;
        Point::new(lat, lon)
    }

    pub fn text(&self) -> String {
        format!("{},{}", self.lat, self.lon)
    }
}

/// Расстояние по большому кругу, метры.
///
/// Гаверсинус, а не теорема косинусов: на малых расстояниях вторая теряет
/// точность из-за косинуса, близкого к единице, — а «в пределах километра» это
/// как раз тот случай, ради которого гео и заводят.
pub fn distance(a: Point, b: Point) -> f64 {
    let (p1, p2) = (a.lat.to_radians(), b.lat.to_radians());
    let dp = (b.lat - a.lat).to_radians();
    let dl = (b.lon - a.lon).to_radians();
    let h = (dp / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dl / 2.0).sin().powi(2);
    2.0 * R * h.sqrt().clamp(0.0, 1.0).asin()
}

/// Рамка вокруг круга: за её пределами точки круга нет заведомо.
///
/// Отсекает дёшево, по колонке, до всякого гаверсинуса. Границы по долготе
/// расходятся тем шире, чем ближе к полюсу, потому что там градус долготы
/// короче; у самого полюса рамка вырождается в полный круг, и это верно —
/// сузить её значило бы потерять попадания.
pub fn around(c: Point, meters: f64) -> (f64, f64, f64, f64) {
    let dlat = (meters / R).to_degrees();
    let lat_lo = (c.lat - dlat).max(-90.0);
    let lat_hi = (c.lat + dlat).min(90.0);
    // Косинус берётся у **дальней** от экватора границы: там градус долготы
    // короче всего, и рамка выходит шире. Взять косинус центра значило бы
    // срезать угол круга.
    let worst = lat_lo.abs().max(lat_hi.abs()).min(89.999_999);
    let cos = worst.to_radians().cos();
    if cos <= 1e-9 {
        return (lat_lo, lat_hi, -180.0, 180.0);
    }
    let dlon = (meters / (R * cos)).to_degrees();
    if dlon >= 180.0 {
        return (lat_lo, lat_hi, -180.0, 180.0);
    }
    (lat_lo, lat_hi, c.lon - dlon, c.lon + dlon)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Круг «туда — обратно» точен на всех значениях байта.
    #[test]
    fn every_byte_survives_the_base64_round_trip() {
        for len in 0..=32usize {
            let raw: Vec<u8> = (0..len).map(|i| (i * 7 + 251) as u8).collect();
            let text = b64(&raw);
            assert_eq!(unb64(&text).as_deref(), Some(raw.as_slice()), "длина {len}");
        }
        // Все двести пятьдесят шесть значений байта разом, включая нулевой и
        // старший: на них и ломается всякая наивная перекодировка.
        let all: Vec<u8> = (0..=255u8).collect();
        assert_eq!(unb64(&b64(&all)).as_deref(), Some(all.as_slice()));
    }

    /// Регистр значим, а негодная запись отвергается.
    #[test]
    fn case_matters_and_bad_text_is_refused() {
        let a = unb64("AQAAAB8ABbBc").expect("верхний регистр");
        let b = unb64("aqaaab8abbbc").expect("нижний регистр");
        assert_ne!(a, b, "регистр Base64 потерян — значения слиплись");

        assert!(unb64("AQ").is_none(), "невыровненная длина принята");
        assert!(unb64("AQ=A").is_none(), "выравнивание в середине принято");
        // URL-безопасный алфавит даёт другие байты, и принимать его молча
        // нельзя.
        assert!(unb64("a-b_").is_none(), "URL-безопасный алфавит принят");
    }

    /// Расстояние сферическое: на широте шестьдесят градус долготы вдвое короче.
    ///
    /// Планарный расчёт этой проверки не проходит — он даёт те же сто
    /// одиннадцать километров и на экваторе, и здесь.
    #[test]
    fn a_degree_of_longitude_is_shorter_away_from_the_equator() {
        let eq = distance(Point::new(0.0, 0.0).unwrap(), Point::new(0.0, 1.0).unwrap());
        let up = distance(Point::new(60.0, 0.0).unwrap(), Point::new(60.0, 1.0).unwrap());
        assert!((eq / 1000.0 - 111.3).abs() < 0.5, "экватор: {} км", eq / 1000.0);
        assert!((up / 1000.0 - 55.7).abs() < 0.5, "широта 60: {} км", up / 1000.0);
        assert!(up < eq * 0.55, "градус долготы не укоротился: {up} против {eq}");
    }

    /// Настоящие расстояния сходятся в пределах процента.
    #[test]
    fn known_distances_agree_within_a_percent() {
        let msk = Point::new(55.7558, 37.6173).unwrap();
        for (name, p, want_km) in [
            ("Петербург", Point::new(59.9311, 30.3609).unwrap(), 635.0),
            ("Новосибирск", Point::new(55.0084, 82.9357).unwrap(), 2812.0),
        ] {
            let got = distance(msk, p) / 1000.0;
            assert!((got - want_km).abs() / want_km < 0.01, "{name}: {got:.1} км вместо {want_km}");
        }
        assert_eq!(distance(msk, msk), 0.0, "расстояние до себя не ноль");
    }

    /// Рамка вокруг круга не теряет точек круга — в том числе у своего угла.
    #[test]
    fn the_box_around_a_circle_keeps_every_point_of_it() {
        let c = Point::new(60.0, 30.0).unwrap();
        let radius = 50_000.0;
        let (lat_lo, lat_hi, lon_lo, lon_hi) = around(c, radius);
        // Точка внутри круга, но у самого угла рамки: направление на
        // северо-восток, расстояние чуть меньше радиуса.
        let d = (radius / R).to_degrees() * 0.70;
        let corner = Point::new(c.lat + d, c.lon + d / 60f64.to_radians().cos()).unwrap();
        assert!(distance(c, corner) < radius, "точка вышла из круга — проверять нечего");
        assert!(
            corner.lat >= lat_lo && corner.lat <= lat_hi,
            "рамка срезала по широте: {} вне [{lat_lo}, {lat_hi}]",
            corner.lat
        );
        assert!(
            corner.lon >= lon_lo && corner.lon <= lon_hi,
            "рамка срезала по долготе: {} вне [{lon_lo}, {lon_hi}]",
            corner.lon
        );
        // И рамка при этом не вырождена во всю землю: иначе она ничего не
        // отсекает, и проверка выше проходила бы даром.
        assert!(lon_hi - lon_lo < 10.0, "рамка накрыла всё: {lon_lo}..{lon_hi}");
    }

    /// У полюса рамка честно вырождается в полный круг по долготе.
    #[test]
    fn near_the_pole_the_box_spans_every_longitude() {
        let (_, _, lon_lo, lon_hi) = around(Point::new(89.99, 0.0).unwrap(), 50_000.0);
        assert_eq!((lon_lo, lon_hi), (-180.0, 180.0), "у полюса рамку сузили");
    }

    /// Пределы координат проверяются, а не приводятся к границе.
    #[test]
    fn coordinates_outside_the_globe_are_refused() {
        assert!(Point::new(91.0, 0.0).is_err(), "широта 91 принята");
        assert!(Point::new(0.0, 181.0).is_err(), "долгота 181 принята");
        assert!(Point::new(f64::NAN, 0.0).is_err(), "NaN принят");
        Point::new(-90.0, 180.0).expect("границы обязаны быть законны");
    }

    /// Точка читается и пишется одинаково.
    #[test]
    fn a_point_survives_text_round_trip() {
        let p = Point::new(55.7558, 37.6173).unwrap();
        assert_eq!(Point::parse(&p.text()).unwrap(), p);
        assert!(Point::parse("55.7558").is_err(), "точка без долготы принята");
        assert!(Point::parse("север,восток").is_err(), "нечисловая точка принята");
    }
}
