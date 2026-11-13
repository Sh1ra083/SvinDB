#!/usr/bin/env python3
"""Перевод маппинга OpenSearch в схему SvinDB — и честный отчёт о непереносимом.

Зачем отдельным инструментом, а не разово руками
------------------------------------------------
Схем шестнадцать, полей в них 1958. Переводить руками — значит однажды ошибиться
и не заметить: поле, переведённое не тем типом, принимается, ищется и молча
отвечает не то. Перевод должен быть один, воспроизводимый и проверяемый.

Второе назначение — **сказать вслух, чего SvinDB не умеет**. Стенд, который
тихо выбрасывает непереносимые поля и печатает победные числа, сравнивает не те
базы. Поэтому `translate` печатает и схему, и перечень отброшенного с причиной.

Использование
-------------
    python3 tools/schemas.py translate <mapping.json>     схема и отчёт
    python3 tools/schemas.py survey <каталог opensearch> [examples]  сводка
    python3 tools/schemas.py corpus <mapping.json> <каталог examples> \\
            <индекс> <N> <файл.ndjson>                    корпус нужного объёма
"""
import json, os, random, sys, collections

# Как тип OpenSearch ложится на тип SvinDB.
#
# `short` сводится к `int32` без потерь: у SvinDB нет двухбайтового целого, а
# диапазон short в int32 входит целиком.
#
# `scaled_float` — к `double`: множитель масштаба у OpenSearch нужен для
# компактного хранения, а не для смысла, и число остаётся тем же.
DIRECT = {
    "boolean": "bool",
    "long": "int64",
    "integer": "int32",
    "short": "int32",
    "double": "double",
    "float": "float",
    "scaled_float": "double",
    "keyword": "keyword",
    "constant_keyword": "keyword",
    "text": "text",
    "geo_point": "geo_point",
    "binary": "binary",
}

# Чего нет, и что это значит для стенда.
MISSING = {
    "object": "контейнер без листьев: полем не является",
}


def leaves(props, pre="", group=None):
    """Листья маппинга, сохраняя путь и ближайший массив `nested`."""
    for name, node in props.items():
        path = f"{pre}{name}"
        own_group = group
        if node.get("type") == "nested":
            # SvinDB держит арность одного массива на документ. Массив внутри
            # массива потребовал бы арность на родительский слот и потому не
            # может быть переведён молча.
            if group is not None:
                yield path, node, group, "массив nested внутри nested не поддерживается"
                continue
            own_group = path
        if "properties" in node:
            yield from leaves(node["properties"], path + ".", own_group)
        else:
            yield path, node, own_group, None


def mapping_of(doc):
    m = doc.get("mappings", doc)
    return m.get("properties", {}), m.get("dynamic")


def translate(doc):
    """Отдаёт (поля, отброшенное, заметки)."""
    props, dynamic = mapping_of(doc)
    fields, dropped, notes = [], [], []
    if dynamic == "strict":
        notes.append("dynamic: strict → settings.json: {\"strict_ingest\": true}")
    for path, node, group, problem in leaves(props):
        if problem:
            dropped.append((path, node.get("type", "nested"), problem))
            continue
        t = node.get("type")
        if t == "knn_vector":
            dim = node.get("dimension")
            if not isinstance(dim, int) or dim <= 0:
                dropped.append((path, t, "у knn_vector не задана положительная dimension"))
                continue
            kind = f"dense_vector:{dim}"
            if group:
                kind += ":slots"
            fields.append((path, kind))
            continue
        if t in MISSING:
            dropped.append((path, t, MISSING[t]))
            continue
        if t == "date":
            fmt = node.get("format", "strict_date_optional_time||epoch_millis")
            # Каноническая дата SvinDB — ISO-8601 с хранением миллисекунд.
            # Числовые варианты смешанного формата перед заливкой надо привести
            # к ISO; иначе одна колонка получила бы разные единицы.
            kind = "date:iso"
            if "epoch" in fmt:
                notes.append(f"{path}: формат даты {fmt!r} — epoch-значения надо привести к ISO-8601")
        else:
            kind = DIRECT.get(t)
        if kind is None:
            dropped.append((path, t or "?", "тип неизвестен переводчику"))
            continue
        # `index: false` — хранить, не искать. У SvinDB это `noindex`.
        if node.get("index") is False:
            kind += ":noindex"
        if group:
            kind += ":slots"
        # Мультиполя: владелец решил брать одно представление из двух, и это
        # решение принимается здесь, а не молча теряется.
        if "fields" in node:
            notes.append(
                f"{path}: мультиполе ({', '.join(node['fields'])}) — берётся только {t}"
            )
        fields.append((path, kind))
    return fields, dropped, notes


def spec_of(fields):
    """Строка для `--fields=` с исходными путями через точку."""
    return ",".join(f"{p}:{k}" for p, k in fields)


def cmd_translate(path):
    doc = json.load(open(path))
    fields, dropped, notes = translate(doc)
    print(f"# {os.path.basename(path)}: {len(fields)} полей переносится, "
          f"{len(dropped)} нет")
    for n in notes:
        print(f"# заметка: {n}")
    nested = sum(1 for _, kind in fields if kind.endswith(":slots"))
    if nested:
        print(f"# {nested} полей лежат в nested-массивах и объявлены с :slots")
    for p, t, why in dropped:
        print(f"# НЕ ПЕРЕНОСИТСЯ {p} ({t}): {why}")
    print(spec_of(fields))


def cmd_survey(dirname, examples_dir=None):
    """Сводка по всем маппингам.

    Считается не только перевод типа. «Тип лёг» и «поле работает как прежде» —
    разные утверждения: обычные object-пути сохраняются, один уровень nested
    получает слоты, а массив внутри массива и автоматические мультиполя требуют
    явного преобразования.
    """
    total = kept = lost = 0
    rows = []
    lost_by_type = collections.Counter()
    nested_all = multifield_all = 0
    for name in sorted(os.listdir(dirname)):
        if not name.endswith(".mapping.json"):
            continue
        index = name.replace(".mapping.json", "")
        doc = json.load(open(os.path.join(dirname, name)))
        fields, dropped, notes = translate(doc)
        for _, t, _ in dropped:
            lost_by_type[t] += 1
        nested = sum(1 for _, kind in fields if kind.endswith(":slots"))
        multifield = sum(1 for n in notes if "мультиполе" in n)
        nested_all += nested
        multifield_all += multifield

        # Многозначность видна только по примерам: маппинг о ней не говорит
        # вовсе — у OpenSearch всякое поле может быть массивом.
        multi = 0
        if examples_dir:
            kinds = dict(fields)
            for e in load_examples(examples_dir, index):
                for k, v in flat(e).items():
                    if k in kinds and isinstance(v, list) and len(v) > 1:
                        multi += 1
                        break
            multi = sum(
                1
                for k in {
                    k
                    for e in load_examples(examples_dir, index)
                    for k, v in flat(e).items()
                    if k in kinds and isinstance(v, list) and len(v) > 1
                }
            )
        total += len(fields) + len(dropped)
        kept += len(fields)
        lost += len(dropped)
        rows.append((index, len(fields), len(dropped), nested, multi, multifield))

    w = max(len(r[0]) for r in rows)
    print(f"| {'индекс':<{w}} | тип лёг | тип не лёг | вложенных | многозначных | мультиполей |")
    print(f"|{'-' * (w + 2)}|--------:|-----------:|----------:|-------------:|------------:|")
    for n, k, d, nested, multi, mf in rows:
        print(f"| {n:<{w}} | {k:7} | {d:10} | {nested:9} | {multi:12} | {mf:11} |")
    print(f"\nвсего {total} полей.")
    print(f"Тип ложится у {kept} ({100 * kept // total}%), не ложится у {lost}:")
    for t, n in lost_by_type.most_common():
        print(f"  {t}: {n} — {MISSING.get(t, 'неизвестен')}")
    print()
    print("Но «тип лёг» не значит «поле работает как прежде». Требует внимания:")
    print(f"  {nested_all} полей nested — сохраняются слотами одного уровня; "
          "nested внутри nested не переносится")
    print(f"  {multifield_all} мультиполей — берётся одно представление из двух "
          "(решение владельца)")
    if examples_dir:
        tm = sum(r[4] for r in rows)
        print(f"  {tm} многозначных путей — генератор корпуса берёт первое значение; "
              "боевой преобразователь обязан задать семантику массива явно")
    else:
        print("  многозначность не посчитана: передайте каталог examples вторым доводом")


def load_examples(dirname, index):
    """Примеры документов этого индекса."""
    out = []
    for name in sorted(os.listdir(dirname)):
        if name.startswith(index + ".") and name.endswith(".example.json"):
            out.append(json.load(open(os.path.join(dirname, name))))
    return out


def flat(doc, pre=""):
    """Документ путями через точку. Массивы отдаются списком значений."""
    out = {}
    for k, v in doc.items():
        path = f"{pre}{k}"
        if isinstance(v, dict):
            out.update(flat(v, path + "."))
        elif isinstance(v, list):
            # Массив объектов раскладывается по путям, как решил владелец: все
            # значения одного пути собираются вместе. Многозначность отсюда
            # никуда не девается — её закрывает этап 3.5.
            if v and isinstance(v[0], dict):
                acc = collections.defaultdict(list)
                for el in v:
                    for kk, vv in flat(el, path + ".").items():
                        acc[kk].append(vv)
                for kk, vv in acc.items():
                    out[kk] = vv
            else:
                out[path] = v
        else:
            out[path] = v
    return out


def assign_path(root, path, value):
    """Кладёт значение во вложенный объект по пути через точку."""
    parts = path.split(".")
    at = root
    for part in parts[:-1]:
        child = at.get(part)
        if not isinstance(child, dict):
            child = {}
            at[part] = child
        at = child
    at[parts[-1]] = value


def assign_value(root, path, kind, value, group_objects):
    """Восстанавливает объект или один элемент nested-массива для корпуса."""
    if not kind.endswith(":slots"):
        assign_path(root, path, value)
        return
    group = path.rsplit(".", 1)[0]
    element = group_objects.get(group)
    if element is None:
        element = {}
        group_objects[group] = element
        # Один представитель массива сохраняет вид документа и связи между
        # его листьями. Несколько элементов генератор намеренно не выдумывает:
        # пары между ними после `flat()` уже неизвестны.
        parent, _, leaf = group.rpartition(".")
        target = root
        if parent:
            holder = {}
            assign_path(root, parent, holder)
            target = holder
        target[leaf or group] = [element]
    relative = path[len(group) :].lstrip(".")
    assign_path(element, relative, value)


def cmd_corpus(mapping, examples_dir, index, n, out_path):
    """Корпус на N документов с заполненностью, взятой из примеров.

    Заполненность важнее содержания. Документ трогает от трёх до десяти
    процентов объявленной схемы, и корпус, где заполнены все поля, мерил бы
    совсем другую задачу — ту, которой у владельца нет.
    """
    random.seed(20260809)
    doc = json.load(open(mapping))
    fields, _, _ = translate(doc)
    kinds = dict(fields)
    ex = [flat(e) for e in load_examples(examples_dir, index)]
    if not ex:
        sys.exit(f"примеров для {index} не нашлось в {examples_dir}")

    # Какие поля вообще встречаются, с какой частотой и сколько у них значений.
    seen = collections.Counter()
    multi = collections.Counter()
    values = collections.defaultdict(list)
    for e in ex:
        for k, v in e.items():
            if k not in kinds:
                continue
            seen[k] += 1
            if isinstance(v, list) and len(v) > 1:
                multi[k] = max(multi[k], len(v))
            values[k].append(v)
    fill = {k: c / len(ex) for k, c in seen.items()}

    written = 0
    with open(out_path, "w") as f:
        for i in range(n):
            base = ex[i % len(ex)]
            d = {"id": f"{index[:2]}{i}"}
            group_objects = {}
            for k, rate in fill.items():
                if random.random() > rate:
                    continue
                v = random.choice(values[k])
                if isinstance(v, list):
                    v = v[0] if v else None
                if v is None:
                    continue
                # Значения разводятся, иначе весь корпус — один документ, и
                # словарь не растёт вовсе. Но **только** там, где вид поля это
                # допускает: вставь цифры в дату или число — и приём их
                # отвергнет, а замер вместо базы измерит генератор.
                if (
                    isinstance(v, str)
                    and len(v) > 8
                    and kinds[k].split(":")[0] in ("keyword", "text")
                    and random.random() < 0.3
                ):
                    v = f"{v[:8]}{i % 9973}{v[8:]}"
                assign_value(d, k, kinds[k], v, group_objects)
            f.write(json.dumps(d, ensure_ascii=False) + "\n")
            written += 1
    filled = sum(fill.values())
    print(json.dumps({
        "index": index, "docs": written, "declared": len(kinds),
        "touched_avg": round(filled, 1),
        "share_pct": round(100 * filled / max(len(kinds), 1), 1),
        # Сколько путей приходят с несколькими значениями и сколько их бывает.
        # Число не украшение: сегодня генератор берёт первое, то есть корпус
        # **беднее** настоящего, и сравнивать его надо с оговоркой.
        "multivalued_paths": len(multi),
        "multivalued_max": max(multi.values(), default=0),
        "examples": len(ex), "path": out_path,
        "bytes": os.path.getsize(out_path),
    }, ensure_ascii=False))
    if multi:
        top = ", ".join(f"{k}×{n}" for k, n in multi.most_common(5))
        print(f"# {len(multi)} путей многозначны, берётся первое значение: {top}",
              file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    cmd = sys.argv[1]
    if cmd == "translate":
        cmd_translate(sys.argv[2])
    elif cmd == "survey":
        cmd_survey(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None)
    elif cmd == "corpus":
        cmd_corpus(sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]), sys.argv[6])
    else:
        sys.exit(__doc__)
