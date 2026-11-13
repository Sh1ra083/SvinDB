//! Сопоставление фразы по позициям слов.

/// Сколько раз фраза встретилась. Ноль — не встретилась.
///
/// `lists` — позиции слов по порядку фразы, каждый список возрастает.
/// Повторы одного слова во фразе — это просто два одинаковых списка.
pub fn count(lists: &[&[u32]], slop: u32) -> u32 {
    count_guarded(lists, slop, None).unwrap_or(0)
}

/// Отмена относится к вычислению одной длинной фразы, а не только к границе
/// между документами. None не является нулевым числом совпадений.
pub fn count_guarded(
    lists: &[&[u32]],
    slop: u32,
    guard: Option<&crate::work::Guard>,
) -> Option<u32> {
    if guard.is_some_and(|g| !g.check()) {
        return None;
    }
    if lists.len() < 2 {
        // Фраза из одного слова — это слово. Считать её отдельно нечего, но и
        // отказывать незачем: пусть отвечает своей частотой.
        return Some(lists.first().map(|l| l.len() as u32).unwrap_or(0));
    }
    if lists.iter().any(|l| l.is_empty()) {
        return Some(0);
    }

    let n = lists.len();
    let mut hits = 0u32;
    // Курсоры по спискам: списки возрастают, и начало фразы тоже сдвигается
    // только вперёд — значит ни один курсор назад не пойдёт.
    let mut at = vec![0usize; n];
    let mut start = 0usize;
    let mut tick = 0;
    let span = slop.saturating_add(u32::try_from(n - 1).unwrap_or(u32::MAX));

    while start < lists[0].len() {
        if guard.is_some_and(|g| !g.go(&mut tick)) {
            return None;
        }
        let p0 = lists[0][start];
        let mut last = p0;
        let mut ok = true;
        for i in 1..n {
            // Наименьшая позиция этого слова строго правее предыдущего.
            while at[i] < lists[i].len() && lists[i][at[i]] <= last {
                if guard.is_some_and(|g| !g.go(&mut tick)) {
                    return None;
                }
                at[i] += 1;
            }
            if at[i] >= lists[i].len() {
                // Слово кончилось: дальше по этому началу и по всем следующим
                // совпадений быть не может.
                return guard.is_none_or(|g| g.check()).then_some(hits);
            }
            last = lists[i][at[i]];
            if last - p0 > span {
                ok = false;
                break;
            }
        }
        // Следующее начало — просто следующее вхождение первого слова.
        //
        // Поймано на том, что комментарий к собственному тесту говорил «два», а
        // утверждение требовало единицы. Расхождение слов и проверки в одном
        // месте — признак, что одно из двух написано не думая.
        hits += u32::from(ok);
        start += 1;
    }
    guard.is_none_or(|g| g.check()).then_some(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_maximum_slop_does_not_wrap_and_cancel_is_not_zero_matches() {
        let positions = [&[0u32][..], &[u32::MAX][..]];
        assert_eq!(count(&positions, u32::MAX), 1);
        let guard = crate::work::Guard::new(0);
        guard.cancel();
        assert_eq!(count_guarded(&positions, u32::MAX, Some(&guard)), None);
    }

    fn c(lists: &[&[u32]], slop: u32) -> u32 {
        count(lists, slop)
    }

    /// Точная фраза: слова подряд и в порядке.
    #[test]
    fn an_exact_phrase_needs_adjacent_words_in_order() {
        // «красный квадрат» в «красный квадрат синий круг»
        assert_eq!(c(&[&[0], &[1]], 0), 1);
        // обратный порядок не считается
        assert_eq!(c(&[&[1], &[0]], 0), 0);
        // разрыв в одно слово не считается при slop = 0
        assert_eq!(c(&[&[0], &[2]], 0), 0);
        // а при slop = 1 считается
        assert_eq!(c(&[&[0], &[2]], 1), 1);
        // разрыв в два при slop = 1 уже нет
        assert_eq!(c(&[&[0], &[3]], 1), 0);
    }

    /// Три слова: размах считается от первого до последнего, а не по парам.
    #[test]
    fn the_span_is_measured_end_to_end_not_pairwise() {
        // 0,1,2 — подряд
        assert_eq!(c(&[&[0], &[1], &[2]], 0), 1);
        // 0,2,3 — один лишний в начале: размах 3, минус два обязательных = 1
        assert_eq!(c(&[&[0], &[2], &[3]], 0), 0);
        assert_eq!(c(&[&[0], &[2], &[3]], 1), 1);
        // 0,2,4 — два лишних
        assert_eq!(c(&[&[0], &[2], &[4]], 1), 0);
        assert_eq!(c(&[&[0], &[2], &[4]], 2), 1);
    }

    /// Несколько вхождений считаются, а перекрывающиеся — нет.
    #[test]
    fn overlapping_occurrences_are_counted_once_each() {
        // «а а а», фраза «а а»: вхождения (0,1) и (1,2) перекрываются по слову
        // в середине, и засчитываются оба — как у Lucene.
        assert_eq!(c(&[&[0, 1, 2], &[0, 1, 2]], 0), 2);
        // «а а а а» — три вхождения.
        assert_eq!(c(&[&[0, 1, 2, 3], &[0, 1, 2, 3]], 0), 3);
        // «а б а б»: два непересекающихся вхождения «а б»
        assert_eq!(c(&[&[0, 2], &[1, 3]], 0), 2);
        // «а б б б а б»: вхождения на 0-1 и 4-5
        assert_eq!(c(&[&[0, 4], &[1, 2, 3, 5]], 0), 2);
    }

    /// Пустой список слова — фразы нет вовсе, чем бы ни были остальные.
    #[test]
    fn a_missing_word_means_no_phrase() {
        assert_eq!(c(&[&[0, 1, 2], &[]], 0), 0);
        assert_eq!(c(&[&[], &[0, 1, 2]], 5), 0);
        assert_eq!(c(&[], 0), 0);
    }

    /// Фраза из одного слова — это слово.
    #[test]
    fn a_phrase_of_one_is_a_word() {
        assert_eq!(c(&[&[0, 5, 9]], 0), 3);
    }

    /// Жадный выбор обязан находить совпадение там, где оно есть, а не там, где
    /// проще. Проверка перебором на случайных данных: медленная эталонная
    /// реализация против быстрой.
    #[test]
    fn the_greedy_walk_agrees_with_brute_force() {
        // Эталон: есть ли хоть одно сочетание позиций, укладывающееся в slop.
        fn brute(lists: &[&[u32]], slop: u32) -> bool {
            fn go(lists: &[&[u32]], i: usize, prev: u32, p0: u32, slop: u32) -> bool {
                if i == lists.len() {
                    return true;
                }
                for &p in lists[i] {
                    if p > prev
                        && p - p0 <= slop + (lists.len() as u32 - 1)
                        && go(lists, i + 1, p, p0, slop)
                    {
                        return true;
                    }
                }
                false
            }
            for &p in lists[0] {
                if go(lists, 1, p, p, slop) {
                    return true;
                }
            }
            false
        }

        let mut seed = 0xC0FFEEu64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..3000 {
            let n = 2 + (next() % 3) as usize;
            let lists: Vec<Vec<u32>> = (0..n)
                .map(|_| {
                    let mut v: Vec<u32> =
                        (0..(1 + next() % 6)).map(|_| (next() % 14) as u32).collect();
                    v.sort_unstable();
                    v.dedup();
                    v
                })
                .collect();
            let refs: Vec<&[u32]> = lists.iter().map(|l| l.as_slice()).collect();
            for slop in 0..4u32 {
                let fast = count(&refs, slop) > 0;
                let slow = brute(&refs, slop);
                assert_eq!(
                    fast, slow,
                    "круг {round}, slop {slop}, списки {lists:?}: жадный обход разошёлся с перебором"
                );
            }
        }
    }
}
