//! Карта кластера: кто держит какой шард и с каким термом.
//!
//! ## Что здесь есть и чего здесь нет
//!
//! Здесь — **вид** карты и правила перехода между её поколениями: что законно,
//! что нет и почему. Здесь **нет** ни выбора лидера, ни сети, ни хранилища:
//! откуда берётся истина, решено отдельно и записано в `docs/adr/0001`.
//! Разделение намеренное — правила перехода можно проверить целиком, не поднимая
//! ни одного узла, и именно они решают, разойдётся запись или нет.
//!
//! ## Терм и почему он не номер поколения
//!
//! Поколение — счётчик самой карты: любое изменение двигает его на единицу, и
//! опоздавший получает конфликт вместо молчаливой перезаписи.
//!
//! Терм — счётчик **права писать в шард**. Он свой у каждого шарда и растёт
//! только когда primary меняется. Разделять их обязательно: перебалансировка
//! двигает поколение, не трогая право писать, а смена primary обязана
//! обесценить всё, что старый primary ещё не успел подтвердить, — и ни то, ни
//! другое не выражается одним счётчиком.
//!
//! ## Правило, ради которого всё остальное
//!
//! **Терм шарда никогда не убывает, и у одного терма не бывает двух primary.**
//! Отсюда следует и защита от разделения: узел, потерявший аренду, узнаёт об
//! этом по тому, что его терм устарел, — и перестаёт быть primary, ни у кого не
//! спрашивая.

use std::collections::BTreeMap;

/// Имя узла: адрес, по которому к нему идут. Оно же ключ в карте.
pub type Node = String;

/// Одно назначение: кто держит шард и в каком качестве.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shard {
    /// Полоса ячеек, за которую шард отвечает: `[lo, hi)`.
    ///
    /// ## Почему полоса, а не «шард номер k из n»
    ///
    /// Позиционная принадлежность («документ идёт в шард `hash % n`») привязывает
    /// адрес к **числу** шардов. Добавь шард или раздели один — и адрес меняется
    /// почти у каждого документа: перебалансировка превращается в переезд всего
    /// корпуса, а деление одного шарда — в переезд остальных, которых никто не
    /// трогал.
    ///
    /// Полоса разрывает эту связь. Ячейка документа ([`crate::shards::slot_of`])
    /// от числа шардов не зависит, и деление шарда описывается **одним числом** —
    /// границей, по которой его полоса режется надвое.
    pub lo: u64,
    pub hi: u64,
    /// Кто пишет. `None` — писать некому, шард доступен только на чтение.
    pub primary: Option<Node>,
    /// Кто повторяет за primary. Порядок ничего не значит.
    pub replicas: Vec<Node>,
    /// Терм права писать. Растёт при каждой смене primary и **только** при ней.
    pub term: u64,
}

impl Shard {
    /// Шард на всю полосу ячеек. Годится, когда шард один.
    pub fn whole(primary: Option<Node>, replicas: Vec<Node>) -> Shard {
        Shard { lo: 0, hi: crate::shards::SLOTS, primary, replicas, term: 1 }
    }

    /// Держит ли шард эту ячейку.
    pub fn holds(&self, slot: u64) -> bool {
        slot >= self.lo && slot < self.hi
    }

    /// Сколько ячеек за ним. Мера «сколько корпуса» при перебалансировке.
    pub fn width(&self) -> u64 {
        self.hi.saturating_sub(self.lo)
    }

    /// Все, кто держит копию: primary и реплики.
    pub fn holders(&self) -> Vec<Node> {
        let mut out: Vec<Node> = self.primary.iter().cloned().collect();
        out.extend(self.replicas.iter().cloned());
        out
    }

    /// Сколько подтверждений нужно по этому договору.
    ///
    /// Считается от **числа держателей**, а не от числа живых: договор — это
    /// обещание клиенту, и ослаблять его по факту чьей-то смерти значит менять
    /// обещание молча. Не набралось подтверждений — отказ с числом, а не тихая
    /// запись в меньшинство.
    pub fn need(&self, ack: Ack) -> usize {
        let n = self.holders().len();
        match ack {
            Ack::One => 1.min(n),
            // Большинство от **всех** держателей: два из трёх, три из пяти.
            Ack::Quorum => n / 2 + 1,
            Ack::All => n,
        }
    }
}

/// Договор о подтверждении записи.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ack {
    /// Достаточно primary. Быстро, и переживает потерю реплик.
    One,
    /// Большинство держателей. Пережившее большинство содержит запись.
    Quorum,
    /// Все. Никакой отказ не теряет записи — и любой отказ её останавливает.
    All,
}

impl Ack {
    pub fn parse(s: &str) -> Option<Ack> {
        Some(match s {
            "one" => Ack::One,
            "quorum" => Ack::Quorum,
            "all" => Ack::All,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Ack::One => "one",
            Ack::Quorum => "quorum",
            Ack::All => "all",
        }
    }
}

/// Карта: все шарды и поколение целиком.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Map {
    /// Шарды по номеру. `BTreeMap`, а не `Vec`: номера не обязаны идти подряд —
    /// деление шарда заводит новые номера, не трогая существующие.
    pub shards: BTreeMap<u32, Shard>,
    /// Поколение карты. Двигается на любое изменение.
    pub gen: u64,
}

/// Почему переход отвергнут.
///
/// Отдельным перечислением, а не строкой: по этим случаям принимают решения —
/// повторить с новой картой, отказать клиенту, снять узел с трафика, — и
/// разбирать текст ради решения значит завести второй источник правды.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reject {
    /// Поколение не то: кто-то изменил карту раньше. Перечитать и повторить.
    Stale { have: u64, want: u64 },
    /// Последовательность поколений исчерпана; заворачивать её к нулю нельзя.
    GenerationExhausted,
    /// Терм шарда попытались уменьшить. Это и есть расхождение, и его не бывает.
    TermWentBack { shard: u32, from: u64, to: u64 },
    /// Primary сменился, а терм остался прежним: два primary на один терм.
    PrimaryWithoutTerm { shard: u32, term: u64 },
    /// Один узел назван и primary, и репликой того же шарда.
    HolderTwice { shard: u32, node: Node },
    /// Шард исчез из карты. Удаление шарда — не переход карты, а перезаливка.
    ShardVanished { shard: u32 },
    /// Полосы не покрывают пространство ячеек ровно один раз.
    ///
    /// Дыра означает документы, которые некуда адресовать; перекрытие — документы
    /// с двумя адресами. Оба молчаливы: запрос уйдёт куда-то и что-то вернёт.
    NotATiling { at: u64, held_by: usize },
    /// Полоса пуста или вывернута. Шард без ячеек — это не шард.
    EmptyRange { shard: u32, lo: u64, hi: u64 },
}

impl Map {
    /// Законен ли переход из этой карты в предложенную.
    ///
    /// ## Почему проверка здесь, а не у хранилища
    ///
    /// Хранилище умеет ровно одно — не дать двум писателям записать поверх друг
    /// друга (сравнение с ожидаемым поколением). Оно не знает и не должно
    /// знать, что терм не убывает и что у одного терма один primary. Спрячь эти
    /// правила в хранилище — и они станут непроверяемыми без него; оставь их
    /// здесь — и они проверяются перебором на столе.
    pub fn check(&self, next: &Map) -> Result<(), Reject> {
        let Some(want) = self.gen.checked_add(1) else {
            return Err(Reject::GenerationExhausted);
        };
        if next.gen != want {
            return Err(Reject::Stale { have: self.gen, want: next.gen });
        }
        for (id, was) in self.shards.iter() {
            let Some(now) = next.shards.get(id) else {
                // Шард нельзя потерять переходом карты: его документы никуда не
                // делись, и карта без него объявляет их несуществующими.
                return Err(Reject::ShardVanished { shard: *id });
            };
            if now.term < was.term {
                return Err(Reject::TermWentBack { shard: *id, from: was.term, to: now.term });
            }
            // Смена того, кто пишет, обязана двигать терм. Иначе подтверждения
            // старого primary и нового неотличимы по терму — а различать их
            // больше нечем.
            if now.primary != was.primary && now.term == was.term {
                return Err(Reject::PrimaryWithoutTerm { shard: *id, term: now.term });
            }
        }
        next.validate()
    }

    /// Проверяет самодостаточные свойства карты без сравнения поколений.
    ///
    /// Загруженная с диска карта не имеет «предыдущей», но от этого дыра,
    /// перекрытие или два одинаковых держателя не становятся допустимыми.
    /// Отделение этой части от [`Map::check`] позволяет отказать сразу при
    /// чтении, а не только при первой попытке что-нибудь изменить.
    pub fn validate(&self) -> Result<(), Reject> {
        // Полосы обязаны покрыть пространство ячеек **ровно один раз**.
        //
        // Проверяется перебором границ, а не суммой ширин: сумма сходится и у
        // карты с дырой напротив перекрытия, то есть ровно у той, которую надо
        // отвергнуть.
        let mut edge = 0u64;
        let mut ranges: Vec<(u64, u64, u32)> =
            self.shards.iter().map(|(id, s)| (s.lo, s.hi, *id)).collect();
        ranges.sort_unstable();
        for (lo, hi, id) in ranges.iter() {
            if hi <= lo {
                return Err(Reject::EmptyRange { shard: *id, lo: *lo, hi: *hi });
            }
            if *lo != edge {
                let held = self.shards.values().filter(|s| s.holds(edge.min(*lo))).count();
                return Err(Reject::NotATiling { at: edge.min(*lo), held_by: held });
            }
            edge = *hi;
        }
        if !self.shards.is_empty() && edge != crate::shards::SLOTS {
            let at = edge.min(crate::shards::SLOTS.saturating_sub(1));
            let held = self.shards.values().filter(|s| s.holds(at)).count();
            return Err(Reject::NotATiling { at, held_by: held });
        }
        for (id, s) in self.shards.iter() {
            if let Some(p) = s.primary.as_ref() {
                if s.replicas.contains(p) {
                    return Err(Reject::HolderTwice { shard: *id, node: p.clone() });
                }
            }
            let mut seen = std::collections::BTreeSet::new();
            for r in s.replicas.iter() {
                if !seen.insert(r) {
                    return Err(Reject::HolderTwice { shard: *id, node: r.clone() });
                }
            }
        }
        Ok(())
    }

    /// Карта с новым primary у шарда: терм двигается, поколение тоже.
    ///
    /// Возвращает готовую карту, а не правит на месте: переход обязан пройти
    /// [`Map::check`] у того, кто его записывает, и полуприменённой карты не
    /// должно существовать даже мгновение.
    pub fn with_primary(&self, shard: u32, node: Option<Node>) -> Map {
        let mut next = self.clone();
        next.gen = next.gen.saturating_add(1);
        if let Some(s) = next.shards.get_mut(&shard) {
            if s.primary != node {
                s.term = s.term.saturating_add(1);
                // Прежний primary становится репликой, новый перестаёт ею быть:
                // держатель не бывает в двух качествах сразу.
                if let Some(old) = s.primary.take() {
                    if !s.replicas.contains(&old) {
                        s.replicas.push(old);
                    }
                }
                if let Some(n) = node.as_ref() {
                    s.replicas.retain(|r| r != n);
                }
                s.primary = node;
            }
        }
        next
    }

    /// Куда идёт документ с этим идентификатором.
    ///
    /// Ячейка та же, что и внутри процесса ([`crate::shards::slot_of`]), — и это
    /// не совпадение, а требование: раскладка обязана быть одной, иначе один и
    /// тот же документ по сети и в процессе попадал бы в разные шарды.
    pub fn route(&self, id: &str) -> Option<u32> {
        let slot = crate::shards::slot_of(id);
        self.shards.iter().find(|(_, s)| s.holds(slot)).map(|(id, _)| *id)
    }

    /// Делит шард надвое по границе `at`, отдавая верхнюю половину номеру `into`.
    ///
    /// ## Почему деление — это одно поколение, а не два
    ///
    /// Соблазн разделить в два шага (завести шард, потом отдать ему полосу)
    /// велик: каждый шаг проще. Но между шагами карта либо не покрывает часть
    /// ячеек, либо покрывает их дважды, и всякий, кто прочтёт её в этот миг,
    /// получит верный ответ на неверной карте. Поэтому переход один, и
    /// [`Map::check`] отвергает всё, что не покрытие.
    ///
    /// Термы: у обеих половин свои и они не связаны. Новая половина начинает с
    /// терма исходной — её содержимое пришло оттуда, и обесценивать чужие
    /// подтверждения ей нечем.
    pub fn split(&self, shard: u32, at: u64, into: u32, node: Option<Node>) -> Option<Map> {
        let was = self.shards.get(&shard)?;
        if at <= was.lo || at >= was.hi || self.shards.contains_key(&into) {
            return None;
        }
        let mut next = self.clone();
        next.gen = next.gen.checked_add(1)?;
        let upper = Shard {
            lo: at,
            hi: was.hi,
            primary: node.or_else(|| was.primary.clone()),
            replicas: was.replicas.clone(),
            term: was.term,
        };
        if let Some(s) = next.shards.get_mut(&shard) {
            s.hi = at;
        }
        next.shards.insert(into, upper);
        Some(next)
    }

    /// Ровно посередине полосы — самое частое деление и потому названное.
    pub fn split_in_half(&self, shard: u32, into: u32, node: Option<Node>) -> Option<Map> {
        let was = self.shards.get(&shard)?;
        self.split(shard, was.lo + was.width() / 2, into, node)
    }

    /// Все узлы, упомянутые картой.
    pub fn nodes(&self) -> Vec<Node> {
        let mut out: std::collections::BTreeSet<Node> = std::collections::BTreeSet::new();
        for s in self.shards.values() {
            for h in s.holders() {
                out.insert(h);
            }
        }
        out.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(primary: Option<&str>, replicas: &[&str], term: u64) -> Shard {
        Shard {
            lo: 0,
            hi: crate::shards::SLOTS,
            primary: primary.map(str::to_string),
            replicas: replicas.iter().map(|s| s.to_string()).collect(),
            term,
        }
    }

    fn map(v: &[(u32, Shard)], gen: u64) -> Map {
        Map { shards: v.iter().cloned().collect(), gen }
    }

    /// Карта из `n` равных полос — та же раскладка, что даёт `slot_owner`.
    fn spread(who: &[&str], gen: u64) -> Map {
        let n = who.len();
        let shards = who
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let lo = (i as u64 * crate::shards::SLOTS) / n as u64;
                let hi = ((i as u64 + 1) * crate::shards::SLOTS) / n as u64;
                (
                    i as u32,
                    Shard { lo, hi, primary: Some(name.to_string()), replicas: vec![], term: 1 },
                )
            })
            .collect();
        Map { shards, gen }
    }

    /// Смена того, кто пишет, обязана двигать терм.
    ///
    /// Это главное правило карты, и его нарушение и есть расхождение записи: у
    /// одного терма два primary, чьи подтверждения ничем не различить.
    #[test]
    fn a_new_primary_must_move_the_term() {
        let was = map(&[(0, shard(Some("a"), &["b"], 7))], 1);

        // Честная смена: терм вырос.
        let good = was.with_primary(0, Some("b".into()));
        assert_eq!(good.shards[&0].term, 8);
        assert_eq!(good.shards[&0].primary.as_deref(), Some("b"));
        assert!(was.check(&good).is_ok(), "законная смена отвергнута");

        // Подделка: primary другой, терм прежний.
        let mut bad = good.clone();
        bad.shards.get_mut(&0).unwrap().term = 7;
        assert_eq!(
            was.check(&bad),
            Err(Reject::PrimaryWithoutTerm { shard: 0, term: 7 }),
            "смена primary без терма прошла"
        );
    }

    /// Исчерпанное поколение или терм не заворачиваются к нулю.
    #[test]
    fn generations_and_terms_never_wrap() {
        let at_end = map(&[(0, shard(Some("a"), &[], u64::MAX))], u64::MAX);
        let changed = at_end.with_primary(0, Some("b".into()));
        assert_eq!(changed.gen, u64::MAX, "поколение завернулось");
        assert_eq!(changed.shards[&0].term, u64::MAX, "терм завернулся");
        assert_eq!(at_end.check(&changed), Err(Reject::GenerationExhausted));
        assert!(
            at_end.split(0, crate::shards::SLOTS / 2, 1, Some("b".into())).is_none(),
            "деление создало поколение после u64::MAX"
        );
    }

    /// Терм не убывает никогда.
    #[test]
    fn a_term_never_goes_back() {
        let was = map(&[(0, shard(Some("a"), &[], 9))], 4);
        let mut back = was.clone();
        back.gen = 5;
        back.shards.get_mut(&0).unwrap().term = 8;
        assert_eq!(
            was.check(&back),
            Err(Reject::TermWentBack { shard: 0, from: 9, to: 8 }),
            "терм откатился"
        );
    }

    /// Опоздавший переход отвергается, а не переписывает чужой.
    #[test]
    fn a_late_transition_is_refused() {
        let was = map(&[(0, shard(Some("a"), &[], 1))], 10);
        let mut same = was.clone();
        same.gen = 10;
        assert_eq!(was.check(&same), Err(Reject::Stale { have: 10, want: 10 }));
        let mut ahead = was.clone();
        ahead.gen = 12;
        assert_eq!(was.check(&ahead), Err(Reject::Stale { have: 10, want: 12 }));
    }

    /// Шард нельзя потерять переходом карты.
    #[test]
    fn a_shard_cannot_vanish() {
        let was = spread(&["a", "b"], 2);
        let mut gone = was.clone();
        gone.gen = 3;
        gone.shards.remove(&1);
        assert_eq!(was.check(&gone), Err(Reject::ShardVanished { shard: 1 }));
    }

    /// Один узел не бывает держателем дважды.
    #[test]
    fn a_node_holds_a_shard_once() {
        let was = map(&[(0, shard(Some("a"), &["b"], 1))], 1);
        let mut twice = was.clone();
        twice.gen = 2;
        twice.shards.get_mut(&0).unwrap().replicas.push("a".into());
        assert_eq!(
            was.check(&twice),
            Err(Reject::HolderTwice { shard: 0, node: "a".into() }),
            "узел стал и primary, и репликой"
        );

        // И смена primary сама по себе такого не создаёт: прежний уходит в
        // реплики, новый оттуда уходит.
        let ok = was.with_primary(0, Some("b".into()));
        assert_eq!(ok.shards[&0].primary.as_deref(), Some("b"));
        assert_eq!(ok.shards[&0].replicas, vec!["a".to_string()]);
        assert!(was.check(&ok).is_ok());
    }

    /// Сколько подтверждений требует каждый договор.
    ///
    /// Считается от числа держателей, а не живых: договор — обещание клиенту, и
    /// ослаблять его по факту чьей-то смерти значит менять обещание молча.
    #[test]
    fn the_contract_says_how_many_acks_it_needs() {
        let three = shard(Some("a"), &["b", "c"], 1);
        assert_eq!(three.need(Ack::One), 1);
        assert_eq!(three.need(Ack::Quorum), 2, "большинство от трёх — двое");
        assert_eq!(three.need(Ack::All), 3);

        let five = shard(Some("a"), &["b", "c", "d", "e"], 1);
        assert_eq!(five.need(Ack::Quorum), 3, "большинство от пяти — трое");

        let alone = shard(Some("a"), &[], 1);
        assert_eq!(alone.need(Ack::Quorum), 1);
        assert_eq!(alone.need(Ack::All), 1);

        // Шард без primary: держателей нет, и требовать не с кого.
        let none = shard(None, &[], 0);
        assert_eq!(none.need(Ack::One), 0);
        assert_eq!(none.need(Ack::All), 0);
    }

    /// Раскладка по сети совпадает с раскладкой внутри процесса.
    ///
    /// Разойдись они — и один и тот же документ по сети и в процессе попадал бы
    /// в разные шарды, то есть запись и чтение искали бы его в разных местах.
    #[test]
    fn routing_matches_the_in_process_one() {
        let m = spread(&["a", "b", "c", "d"], 1);
        for i in 0..500u32 {
            let id = format!("документ-{i}");
            let by_map = m.route(&id).expect("шард нашёлся");
            let by_proc = crate::shards::route(&id, 4) as u32;
            assert_eq!(by_map, by_proc, "{id}: карта и процесс развели документ");
        }
        assert_eq!(Map::default().route("что угодно"), None, "пустая карта кого-то нашла");
    }

    /// Договор читается и печатается одним и тем же именем.
    #[test]
    fn the_contract_round_trips_by_name() {
        for a in [Ack::One, Ack::Quorum, Ack::All] {
            assert_eq!(Ack::parse(a.name()), Some(a));
        }
        assert_eq!(Ack::parse("большинство"), None, "неизвестный договор принят");
    }

    /// Деление шарда переселяет **только его** документы.
    ///
    /// Это и есть довод в пользу ячеек. При позиционной раскладке `hash % n`
    /// добавление четвёртого шарда к трём меняет адрес примерно трём четвертям
    /// корпуса — включая тот, который никто не делил. Здесь обязано переехать
    /// ровно то, что попало в отданную половину полосы, и ничего больше.
    #[test]
    fn a_split_moves_only_what_it_split() {
        let was = spread(&["a", "b", "c"], 5);
        let ids: Vec<String> = (0..2000).map(|i| format!("док-{i}")).collect();
        let before: Vec<u32> = ids.iter().map(|id| was.route(id).unwrap()).collect();

        let now = was.split_in_half(1, 9, Some("г".into())).expect("деление не состоялось");
        assert_eq!(now.gen, 6, "деление обязано двигать поколение");
        assert!(was.check(&now).is_ok(), "законное деление отвергнуто");

        let after: Vec<u32> = ids.iter().map(|id| now.route(id).unwrap()).collect();
        let mut moved = 0;
        for (k, (was_at, now_at)) in before.iter().zip(after.iter()).enumerate() {
            if was_at == now_at {
                continue;
            }
            moved += 1;
            assert_eq!(*was_at, 1, "переехал документ {} из чужого шарда {was_at}", ids[k]);
            assert_eq!(*now_at, 9, "документ уехал не в новую половину");
        }
        assert!(moved > 0, "деление не переселило ни одного документа — проверять нечего");
        // Переехала примерно половина одной трети: около шестой части.
        let share = moved as f64 / ids.len() as f64;
        assert!((0.10..0.24).contains(&share), "переехало {share:.3} корпуса вместо ~1/6");
    }

    /// Карта, не покрывающая ячейки ровно один раз, отвергается.
    #[test]
    fn a_map_that_is_not_a_tiling_is_refused() {
        let was = spread(&["a", "b"], 1);

        // Дыра: у второго шарда отрезали начало полосы.
        let mut hole = was.clone();
        hole.gen = 2;
        hole.shards.get_mut(&1).unwrap().lo += 10;
        assert!(
            matches!(was.check(&hole), Err(Reject::NotATiling { .. })),
            "карта с дырой принята: документы этих ячеек некуда адресовать"
        );

        // Перекрытие: первый залез на второго.
        let mut over = was.clone();
        over.gen = 2;
        over.shards.get_mut(&0).unwrap().hi += 10;
        assert!(
            matches!(was.check(&over), Err(Reject::NotATiling { .. })),
            "карта с перекрытием принята: у документов два адреса"
        );

        // Пустая полоса — не шард.
        let mut empty = was.clone();
        empty.gen = 2;
        empty.shards.get_mut(&1).unwrap().lo = empty.shards[&1].hi;
        assert!(matches!(was.check(&empty), Err(Reject::EmptyRange { .. })));
    }

    /// Деление, которого не бывает, не делается.
    #[test]
    fn an_impossible_split_is_refused() {
        let was = spread(&["a", "b"], 1);
        assert!(was.split(0, 0, 9, None).is_none(), "деление по краю полосы прошло");
        assert!(was.split(0, was.shards[&0].hi, 9, None).is_none(), "деление по дальнему краю");
        assert!(was.split(0, 100, 1, None).is_none(), "новый шард занял чужой номер");
        assert!(was.split(7, 100, 9, None).is_none(), "разделили несуществующий шард");
    }
}
