#!/bin/zsh
# Прогон на 100 млн документов.
#
# Корпус: исходные 25 млн, пройденные четыре раза с приставкой к идентификатору.
# Содержимое то же, частоты слов растут во столько же раз, во сколько корпус,
# значит обратная частота не меняется и вес слова остаётся тем же — иначе замер
# мерил бы другую задачу. Файла на 20 ГБ при этом не создаётся.
#
# Заливки идут ПО ОЧЕРЕДИ, и сосед на время чужой заливки останавливается.
# Машина — 16 ГиБ; индекс на 100 млн в кеш страниц не помещается, и резидентный
# сосед на несколько гигабайт меняет результат больше, чем всё, что мы мерим.
# На 25 млн это стоило SvinDB половины скорости, и это было видно.
set -e
S=${0:A:h}
BIN=${BIN:-/tmp/svindb-final}
N=${N:-100000000}

say() { print -r -- "== $1 ($(date +%H:%M:%S))" }

say "убираем 25-миллионный индекс ES: место и память"
curl -s -X DELETE localhost:19200/bench25m > /dev/null || true
say "останавливаем ES на время заливки SvinDB"
docker stop searchd-es > /dev/null

say "SvinDB: заливка $N"
pkill -f "serve /tmp/sv100m" 2>/dev/null || true
sleep 2
rm -rf /tmp/sv100m && mkdir -p /tmp/sv100m
# Без `--flush=`: умолчание сброса равно целевому размеру сегмента, и это
# несущее. Прежде здесь стояло 200 000 при цели в миллион — приём писал пять
# мелких сегментов, обслуживание сливало их в один, и содержимое каждого
# документа проходило компрессор дважды. На 10 млн это стоило 136.5 с против
# 76.2 с; на 100 млн — 1694.8 с против 1034.2 с.
($BIN serve /tmp/sv100m 19410 \
    --single-index --fields=message:1,views:int64,ts:int64,channel:int64 > /tmp/sv19410.log 2>&1 &)
sleep 5
python3 $S/load.py svindb /tmp/msgs25m.ndjson 19410 $N 4 5000 > /tmp/load100m-sv.json
cat /tmp/load100m-sv.json
print -r -- "сразу после заливки: $(du -sm /tmp/sv100m|cut -f1) МБ"
# Место мерится **после** того, как обслуживание перестало менять состав.
# Сразу после заливки на диске лежат ещё не слитые сегменты, и число выходит
# завышенным: на 10 млн это 797 МБ против 729 МБ осевших — разница в девять
# процентов там, где мы сравниваем разы.
say "ждём, пока обслуживание осядет"
prev=0
for i in {1..240}; do
  sleep 5
  now=$(ls -d /tmp/sv100m/*/ 2>/dev/null | wc -l | tr -d " ")
  [[ "$now" == "$prev" ]] && break
  prev=$now
done
print -r -- "осело: сегментов $prev"
du -sh /tmp/sv100m > /tmp/size100m-sv.txt
cat /tmp/size100m-sv.txt
ps -o rss= -p $(pgrep -f "serve /tmp/sv100m") | awk "{printf \"RSS %.0f МБ\\n\", \$1/1024}"
curl -s localhost:19410/_health > /tmp/health100m-sv.json

say "SvinDB: запросы"
python3 $S/qset100.py 19410 15 > /tmp/q100m-sv.txt 2>&1 || true
tail -20 /tmp/q100m-sv.txt

say "останавливаем SvinDB, поднимаем ES"
pkill -f "serve /tmp/sv100m" 2>/dev/null || true
docker start searchd-es > /dev/null
until curl -s localhost:19200/_cluster/health > /dev/null 2>&1; do sleep 2; done
sleep 5

say "Elasticsearch: заливка $N"
python3 $S/es_setup.py 19200 bench100m 1
python3 $S/load.py es /tmp/msgs25m.ndjson 19200 $N 4 5000 bench100m > /tmp/load100m-es.json
cat /tmp/load100m-es.json
curl -s "localhost:19200/bench100m/_stats" \
  | python3 -c "import sys,json;d=json.load(sys.stdin)['_all']['primaries'];print(json.dumps({'bytes':d['store']['size_in_bytes'],'segments':d['segments']['count'],'docs':d['docs']['count']}))" \
  > /tmp/size100m-es.txt
cat /tmp/size100m-es.txt

say "Elasticsearch: запросы"
python3 $S/esq.py 19200 bench100m 15 > /tmp/q100m-es.txt 2>&1 || true
tail -20 /tmp/q100m-es.txt

say "поднимаем SvinDB обратно"
($BIN serve /tmp/sv100m 19410 --single-index > /tmp/sv19410.log 2>&1 &)
say ГОТОВО
