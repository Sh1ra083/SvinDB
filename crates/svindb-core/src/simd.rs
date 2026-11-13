//! Расстояния между векторами на векторных же командах процессора.

/// Квадрат евклидова расстояния между кодами `int8`. Истина.
#[inline]
pub fn l2_i8_scalar(a: &[i8], b: &[i8]) -> i32 {
    let mut s = 0i32;
    for (x, y) in a.iter().zip(b.iter()) {
        let d = i32::from(*x) - i32::from(*y);
        s += d * d;
    }
    s
}

/// Расстояние Хэмминга между упакованными кодами. Истина.
#[inline]
pub fn hamming_scalar(a: &[u64], b: &[u64]) -> u32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x ^ y).count_ones()).sum()
}

/// Скалярное произведение полного запроса на сжатый документ. Истина.
///
/// Несимметрично: запрос остаётся `f32`, документ — код. Квантование запроса это
/// потеря, которой можно не платить: запрос один, а документов миллион.
#[inline]
pub fn dot_f32_i8_scalar(q: &[f32], c: &[i8]) -> f32 {
    let mut s = 0f32;
    for (x, y) in q.iter().zip(c.iter()) {
        s += *x * f32::from(*y);
    }
    s
}

/// Квадрат евклидова расстояния между `f32`. Истина.
#[inline]
pub fn l2_f32_scalar(a: &[f32], b: &[f32]) -> f32 {
    let mut s = 0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        let d = *x - *y;
        s += d * d;
    }
    s
}

#[cfg(target_arch = "aarch64")]
mod neon {
    use std::arch::aarch64::*;

    /// Квадрат евклидова расстояния между `int8`, по шестнадцать за раз.
    ///
    /// Идёт через **модуль разности**, а не через саму разность, и это не
    /// хитрость, а обход переполнения: `vabdq_u8` даёт модуль без выхода за
    /// байт, тогда как знаковая разность двух краёв диапазона за байт выходит.
    /// Квадрат модуля равен квадрату разности, поэтому ответ тот же.
    ///
    /// Накопление двухступенчатое: квадраты в `u16` (до 65025 — влезает), затем
    /// попарно в `u32`. Копить шестнадцать квадратов в `u16` нельзя: переполнит.
    ///
    /// # Safety
    ///
    /// Требует `neon`, который на `aarch64` есть всегда.
    #[target_feature(enable = "neon")]
    pub unsafe fn l2_i8(a: &[i8], b: &[i8]) -> i32 {
        let n = a.len().min(b.len());
        let mut acc = vdupq_n_u32(0);
        let mut i = 0usize;
        while i + 16 <= n {
            // Коды знаковые, а модуль разности берётся у беззнаковых. Сдвиг на
            // 128 у обоих операндов разности не меняет.
            let x = vaddq_s8(vld1q_s8(a.as_ptr().add(i)), vdupq_n_s8(-128));
            let y = vaddq_s8(vld1q_s8(b.as_ptr().add(i)), vdupq_n_s8(-128));
            let d = vabdq_u8(vreinterpretq_u8_s8(x), vreinterpretq_u8_s8(y));
            let lo = vmull_u8(vget_low_u8(d), vget_low_u8(d));
            let hi = vmull_high_u8(d, d);
            acc = vpadalq_u16(acc, lo);
            acc = vpadalq_u16(acc, hi);
            i += 16;
        }
        let mut s = vaddvq_u32(acc) as i32;
        while i < n {
            let d = i32::from(a[i]) - i32::from(b[i]);
            s += d * d;
            i += 1;
        }
        s
    }

    /// Расстояние Хэмминга: `vcntq_u8` считает единицы в каждом байте разом.
    ///
    /// # Safety
    ///
    /// Требует `neon`.
    #[target_feature(enable = "neon")]
    pub unsafe fn hamming(a: &[u64], b: &[u64]) -> u32 {
        let n = a.len().min(b.len());
        let mut acc = vdupq_n_u32(0);
        let mut i = 0usize;
        while i + 2 <= n {
            let x = vld1q_u64(a.as_ptr().add(i));
            let y = vld1q_u64(b.as_ptr().add(i));
            let d = vreinterpretq_u8_u64(veorq_u64(x, y));
            // Счёт единиц побайтно, затем попарные сложения до `u32`.
            acc = vpadalq_u16(acc, vpaddlq_u8(vcntq_u8(d)));
            i += 2;
        }
        let mut s = vaddvq_u32(acc);
        while i < n {
            s += (a[i] ^ b[i]).count_ones();
            i += 1;
        }
        s
    }

    /// Скалярное произведение `f32` на `int8`, по шестнадцать кодов за раз.
    ///
    /// Коды расширяются до `i16`, затем до `f32`, и накапливаются умножением со
    /// сложением. Четыре накопителя — по той же причине, что и у `l2_f32`:
    /// цепочка сложений связала бы конвейер задержкой.
    ///
    /// # Safety
    ///
    /// Требует `neon`.
    #[target_feature(enable = "neon")]
    pub unsafe fn dot_f32_i8(q: &[f32], c: &[i8]) -> f32 {
        let n = q.len().min(c.len());
        let mut a0 = vdupq_n_f32(0.0);
        let mut a1 = vdupq_n_f32(0.0);
        let mut a2 = vdupq_n_f32(0.0);
        let mut a3 = vdupq_n_f32(0.0);
        let mut i = 0usize;
        while i + 16 <= n {
            let cv = vld1q_s8(c.as_ptr().add(i));
            let lo = vmovl_s8(vget_low_s8(cv));
            let hi = vmovl_high_s8(cv);
            a0 = vfmaq_f32(
                a0,
                vld1q_f32(q.as_ptr().add(i)),
                vcvtq_f32_s32(vmovl_s16(vget_low_s16(lo))),
            );
            a1 = vfmaq_f32(a1, vld1q_f32(q.as_ptr().add(i + 4)), vcvtq_f32_s32(vmovl_high_s16(lo)));
            a2 = vfmaq_f32(
                a2,
                vld1q_f32(q.as_ptr().add(i + 8)),
                vcvtq_f32_s32(vmovl_s16(vget_low_s16(hi))),
            );
            a3 =
                vfmaq_f32(a3, vld1q_f32(q.as_ptr().add(i + 12)), vcvtq_f32_s32(vmovl_high_s16(hi)));
            i += 16;
        }
        let mut s = vaddvq_f32(vaddq_f32(vaddq_f32(a0, a1), vaddq_f32(a2, a3)));
        while i < n {
            s += q[i] * f32::from(c[i]);
            i += 1;
        }
        s
    }

    /// Квадрат евклидова расстояния между `f32`, по четыре за раз.
    ///
    /// # Safety
    ///
    /// Требует `neon`.
    #[target_feature(enable = "neon")]
    pub unsafe fn l2_f32(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        // Четыре накопителя, чтобы цепочка сложений не связывала конвейер:
        // у сложения задержка в несколько тактов, и один накопитель заставил бы
        // ждать её на каждом шаге.
        let mut a0 = vdupq_n_f32(0.0);
        let mut a1 = vdupq_n_f32(0.0);
        let mut a2 = vdupq_n_f32(0.0);
        let mut a3 = vdupq_n_f32(0.0);
        let mut i = 0usize;
        while i + 16 <= n {
            let d0 = vsubq_f32(vld1q_f32(a.as_ptr().add(i)), vld1q_f32(b.as_ptr().add(i)));
            let d1 = vsubq_f32(vld1q_f32(a.as_ptr().add(i + 4)), vld1q_f32(b.as_ptr().add(i + 4)));
            let d2 = vsubq_f32(vld1q_f32(a.as_ptr().add(i + 8)), vld1q_f32(b.as_ptr().add(i + 8)));
            let d3 =
                vsubq_f32(vld1q_f32(a.as_ptr().add(i + 12)), vld1q_f32(b.as_ptr().add(i + 12)));
            a0 = vfmaq_f32(a0, d0, d0);
            a1 = vfmaq_f32(a1, d1, d1);
            a2 = vfmaq_f32(a2, d2, d2);
            a3 = vfmaq_f32(a3, d3, d3);
            i += 16;
        }
        let mut s = vaddvq_f32(vaddq_f32(vaddq_f32(a0, a1), vaddq_f32(a2, a3)));
        while i < n {
            let d = a[i] - b[i];
            s += d * d;
            i += 1;
        }
        s
    }
}

/// Квадрат евклидова расстояния между кодами `int8`.
#[inline]
pub fn l2_i8(a: &[i8], b: &[i8]) -> i32 {
    #[cfg(target_arch = "aarch64")]
    {
        // Safety: `neon` на `aarch64` есть всегда, признак проверять незачем.
        return unsafe { neon::l2_i8(a, b) };
    }
    #[allow(unreachable_code)]
    l2_i8_scalar(a, b)
}

/// Расстояние Хэмминга между упакованными кодами.
#[inline]
pub fn hamming(a: &[u64], b: &[u64]) -> u32 {
    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { neon::hamming(a, b) };
    }
    #[allow(unreachable_code)]
    hamming_scalar(a, b)
}

/// Скалярное произведение полного запроса на сжатый документ.
///
/// Как и у [`l2_f32`], порядок сложений иной, чем у скалярного, поэтому равенство
/// бит в бит здесь не обещается — только совпадение с точностью до округления.
#[inline]
pub fn dot_f32_i8(q: &[f32], c: &[i8]) -> f32 {
    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { neon::dot_f32_i8(q, c) };
    }
    #[allow(unreachable_code)]
    dot_f32_i8_scalar(q, c)
}

/// Квадрат евклидова расстояния между `f32`.
///
/// Ответ может отличаться от скалярного в последнем разряде: сложение `f32` не
/// ассоциативно, а накопителей здесь четыре. Для **порядка** это безразлично —
/// расстояния отличаются на 10⁻⁷ доли, — но равенство бит в бит здесь обещать
/// нельзя, в отличие от целочисленных ядер.
#[inline]
pub fn l2_f32(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { neon::l2_f32(a, b) };
    }
    #[allow(unreachable_code)]
    l2_f32_scalar(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rng(seed: u64) -> impl FnMut() -> u64 {
        let mut s = seed;
        move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        }
    }

    /// Целочисленные ядра обязаны совпадать со скалярными **бит в бит**.
    ///
    /// Не «примерно»: у целых расхождению взяться неоткуда. Появилось — значит
    /// ошибка в ядре, и списывать её на погрешность нельзя.
    #[test]
    fn the_integer_kernels_agree_exactly() {
        let mut next = rng(0xa5a5_1234);
        // Длины вокруг ширины регистра: 15 и 17 ловят хвост, 0 — пустой вход.
        for len in [0usize, 1, 7, 15, 16, 17, 31, 32, 33, 64, 128, 129, 384, 768, 1000] {
            for _ in 0..40 {
                let a: Vec<i8> = (0..len).map(|_| (next() as u8) as i8).collect();
                let b: Vec<i8> = (0..len).map(|_| (next() as u8) as i8).collect();
                assert_eq!(l2_i8(&a, &b), l2_i8_scalar(&a, &b), "int8, длина {len}");
            }
        }
        // Края диапазона: именно там знаковая разность вышла бы за байт.
        for (x, y) in [(-128i8, 127i8), (127, -128), (-128, -128), (127, 127)] {
            let a = vec![x; 64];
            let b = vec![y; 64];
            assert_eq!(l2_i8(&a, &b), l2_i8_scalar(&a, &b), "края {x} и {y}");
        }

        for len in [0usize, 1, 2, 3, 5, 6, 12, 16, 64] {
            for _ in 0..40 {
                let a: Vec<u64> = (0..len).map(|_| next()).collect();
                let b: Vec<u64> = (0..len).map(|_| next()).collect();
                assert_eq!(hamming(&a, &b), hamming_scalar(&a, &b), "хэмминг, длина {len}");
            }
        }
        // Все биты разные и все одинаковые.
        assert_eq!(hamming(&[u64::MAX; 8], &[0u64; 8]), 512);
        assert_eq!(hamming(&[0u64; 8], &[0u64; 8]), 0);
    }

    /// Дробное ядро совпадает со скалярным с точностью до порядка сложений.
    #[test]
    fn the_float_kernel_agrees_to_rounding() {
        let mut next = rng(0x1357_9bdf);
        for len in [0usize, 1, 15, 16, 17, 128, 768, 1001] {
            for _ in 0..20 {
                let a: Vec<f32> = (0..len).map(|_| (next() % 2000) as f32 / 100.0 - 10.0).collect();
                let b: Vec<f32> = (0..len).map(|_| (next() % 2000) as f32 / 100.0 - 10.0).collect();
                let (x, y) = (l2_f32(&a, &b), l2_f32_scalar(&a, &b));
                let scale = x.abs().max(y.abs()).max(1.0);
                assert!((x - y).abs() <= scale * 1e-5, "длина {len}: {x} против {y}");
            }
        }
    }

    /// Несимметричное произведение совпадает со скалярным до округления.
    #[test]
    fn the_asymmetric_dot_agrees_to_rounding() {
        let mut next = rng(0x2468_ace0);
        for len in [0usize, 1, 15, 16, 17, 33, 128, 768, 1001] {
            for _ in 0..20 {
                let q: Vec<f32> = (0..len).map(|_| (next() % 4000) as f32 / 100.0 - 20.0).collect();
                let c: Vec<i8> = (0..len).map(|_| (next() as u8) as i8).collect();
                let (x, y) = (dot_f32_i8(&q, &c), dot_f32_i8_scalar(&q, &c));
                // Погрешность сложения ограничена суммой МОДУЛЕЙ слагаемых, а
                // не величиной ответа. У произведения с сокращением ответ бывает
                // близок к нулю при крупных слагаемых: сравнивать с ним значило
                // бы требовать точности, которой у `f32` нет.
                let mag: f32 = q.iter().zip(c.iter()).map(|(a, b)| (a * f32::from(*b)).abs()).sum();
                assert!(
                    (x - y).abs() <= mag.max(1.0) * 1e-6,
                    "длина {len}: {x} против {y}, слагаемых на {mag}"
                );
            }
        }
        // Края кода: -128 и 127 обязаны расшириться со знаком, а не как байты.
        assert_eq!(dot_f32_i8(&[1.0; 32], &[-128i8; 32]), -4096.0);
        assert_eq!(dot_f32_i8(&[1.0; 32], &[127i8; 32]), 4064.0);
    }

    /// Разной длины входы не читают за концом короткого.
    #[test]
    fn a_short_side_is_not_read_past() {
        assert_eq!(
            l2_i8(&[1, 2, 3], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]),
            0
        );
        assert_eq!(hamming(&[0, 0], &[0; 9]), 0);
    }
}
