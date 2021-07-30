# 13. RDD 고급 개념

### 13.1 키-값 형태의 기초 (키-값 형태의 RDD)
- <연산명>ByKey 형태의 이름을 가진 메서드는 RDD 에서 키-값 형태로 다룰 수 있는 메서드
- 이 연산은 PairRDD 타입만 사용가능
- pairRDD 타입을 만드는 가장 쉬운 방법은 RDD에 맵 연산을 수행하여 키-값 구조로 만드는 것
*words.map(lambda word: (word.lower(), 1))

13.1.1 KeyBy : 현재 값으로부터 키를 생성하는 함수 
*keyword = words.keyBy(lambda word: word.lower()[0])  # 단어의 첫 번째 문자를 키로 만들어 RDD를 생성

13.1.2 값 매핑하기
- 튜플 형태의 데이터를 사용한다면 스파크는 튜플의 첫 번째 요소를 키로, 두 번째 요소를 값으로 추정
*keyword.mapValues(lambda word : word.upper()).collect()     #('s', 'SPARK'), ('t', 'THE')..
*keyword.flatMapValues(lambda word: word.upper()).collect()   #('s', 'S'), ('s', 'P'), ('s', 'A')...  

13.1.3 키와 값 추출하기
*print(keyword.keys().collect())  #['s', 't', 'd', 'g', ':', 'b', 'd', 'p', 'm', 's']
*print(keyword.values().collect())  #['Spark', 'The', 'Definitive', 'Guide', ':', 'Big', 'Data', 'Processing', 'Made', 'Simple']

13.1.4 lookup : 특정 키에 관한 결과를 찾는 함수
*keyword.lookup("s")

13.1.5 sampleByKey : 근사치나 정확도를 이용해 키를 기반으로 RDD 샘플을 생성. 간단한 무작위 샘플링
*words.map(lambda word: (word.lower()[O], word)).sampleByKey(True, sampleMap, 6).collect()

### 13.2 집계

Q. sc 는 무엇?
chars = words.flatMap(lambda word: word.lower())
KVcharacters = chars.map(lambda letter: (letter, 1))
def maxFunc(left, right):
  return max(left, right)
def addFunc(left, right):
  return left + right
nums = sc.parallelize(range(1,31), 5)

13.2.1 countByKey : 각 키의 이이템 수를 구하고 로컬 맵으로 결과를 수집
*print(KVcharacters.countByKey()) #defaultdict(<class 'int'>, {'s': 4, 'p': 3, 'a': 4,...

13.2.2 집계 연산 구현 방식 이해하기
키-값 형태의 Pair RDD를 생성하는 몇 가지 방법이 있음
- GroupByKey : 키의 총 레코드 수를 구하는 경우 groupByKey의 결과로 만들어진 그룹에 map 연산을 수행히는 방식 이 가장 좋아보이지만 out of memory error 발생 가능성이큼
- 각 키에 대한 값의 크기가 일정히고 익스큐터에 할딩된 메모리에서 처리 가능할 정도의 크기라면 groupByKey 메서드를 사용
*from functools import reduce # Python 3부터는 import 해야함
*print(KVcharacters.groupByKey().map(lambda row: (row[0], reduce(addFunc, row[1]))).collect())

- reduceByKey : 최종 리듀스 과정을 제외한 모든 작업은 개별 워커에서 처리하기 때문에 연산 중에 셔플이 발생 x -> 안정성 상승, 연산 수행 속도 크게 상승. 단 결과의 순서가 중요한 경우에는 적합하지 않음


13.2.3 기타 집계 메서드
- 구조적 API를 시용하면 훨씬 간단하게 집계를 수행할 수 있으므로 굳이 고급 집계 함수를 시용하지 않이도 됩니다.

- Aggregate : 첫번째 함수는 파티션 내에서 수행되고 두 번째 함수는 모든 파티션에 걸쳐 수행됩니다. 두 함수 모두 시작값을 사용합니다.
- aggregate 함수는 드라이버에서 최종 집계를 수행하기 때문에 성능에 약간의 영향이 있음. 
- treeAggregate는 Aggregate와 동일한 작업을 수행하지만 다른 처리과정을 거치는 함수. 집계처리를 여러 단계로 수행. -> 작업의 안정성 증가
*depth = 3 / nums.treeAggregate(0, maxFunc, addFunc, depth)
- aggregateByKey : aggregate 함수와 동일하지만 파티션 대신 키를 기준으로 연산ㅇ르 수행
*KVcharacters.aggregateByKey(0, addFunc, maxFunc).collect()
- combineByKey : 집계 함수 대신 컴바이너를 사용. 컴바이너는 키를 기준으로 연산을 수행하며 파라미터로 사용된 함수에 따라 값을 병합합
def valToCombiner(value):
  return [value] 
def mergeValuesFunc(vals, valToAppend):
  vals.append(valToAppend)
  return vals  
def mergeCombinerFunc(vals1, vals2):
  return vals1 + vals2 
outputPartitions = 6
KVcharacters.combineByKey(valToCombiner,mergeValuesFunc,mergeCombinerFunc,outputPartitions).collect()  # [('s', [1, 1, 1, 1]), ('d', [1, 1, 1, 1]), ...

- foldByKey : 결합 함수와 항등원인 "제로값"을 이용해 각 키의 값을 병합.
*KVcharacters.foldByKey(0, addFunc).collect()  # [('s', 4), ('p', 3), ('r', 2), ('h', 1), ('d', 4), ('i', 7)...

- cogroup : 파이썬을 사용하는 경우 최대 2개의 키-값 형태의 RDD를 그룹화할 수 있으며 각 키를 기준으로 값을 결합. RDD에 대한 그룹기반의 조인을 수행. 사용자 정의 파티션 함수를 파라미터로 사용할 수 있음. 
*import random
distinctChars = words.flatMap(lambda word: word.lower()).distinct()
charRDD = distinctChars.map(lambda c: (c, random.random()))
charRDD2 = distinctChars.map(lambda c: (c, random.random()))
charRDD.cogroup(charRDD2).take(5) # [('s', (<pyspark.resultiterable.ResultIterable object at 0x7f46e92a3110>, <pyspark.resultiterable.ResultIterable object at 0x7f46e92a3bd0>)), ('p',...

### 13.4 조인 : 구조적 API에서 알아본 것과 거의 동일한 조인 방식을 가지고 있음. RDD 조인이 사용자가 더 많은 부분은 관여해야함.

13.4.1 내부조인
*KVcharacters.join(keyedChars).count()
*KVcharacters.join(keyedChars, outputPartitions).count()

13.4.2 zip : zip 함수를 사용해 동일한 길이의 두 개의 RDD를 지퍼 zipper를 잠그듯이 연결할 수 있으며 PairRDD를 생성.
*words.zip(numRange).collect()

### 13.5 파티션 제어하기 : RDD를 사용하면 데이터가 클러스터 전체에 물리적으로 정확히 분산되는 방식을 정의할 수 있음. 구조적 API와 가장 큰 차이점은 파티션 함수를 파라미터로 사용할 수 있디는 사실

13.5.1 coalesce : 동일한 워커에 존재하는 파티션을 합치는 메서드. coalesce 메서드를 사용해 데이터 셔플링 없이 하나의 파티션으로 합칠 수 있음.
*words.coalesce(1).getNumPartitions() # 1

13.5.2 repartition : 파티션을 늘리거나 줄일 수 있음. 파티션 수를 늘리면 맵 타입이나 필터 타입의 연산을 수행할 때 병렬 처리 수준을 높일 수 있음.
*words.repartition(10) // 10개의 파티션이 생성

13.5.3 repartitionAndSortWithinPartitions : 파티션을 재분배할 수 있고, 재분배된 결과 파티션의 정렬 방식을 지정할 수 있음

13.5.4 사용자 정의 파티셔닝 : 구조적 API 에서는 사용자 정의 파티셔닝을 파라미터로 사용할 수 없음. 목표는 클러스터 전체에 걸쳐 데이터를 균등하게 분배
- HashPartitioner  : 이산형 값을 다룰 때 사용. 구조적 API / RDD 모두 사용 가능
- RangePartitioner : 연속형 값을 다룰 때 사용. 구조적 API / RDD 모두 사용 가능
- 사용자 정의 키 분배 로직은 RDD 수준에서만 사용할 수 있습니다.

def partitionFunc(key):
  import random
  if key == 17850 or key == 12583:
    return 0
  else:
    return random.randint(1,2)

keyedRDD = rdd.keyBy(lambda row: row[6])
keyedRDD\
  .partitionBy(3, partitionFunc)\
  .map(lambda x: x[0])\
  .glom()\
  .map(lambda x: len(set(x)))\
  .take(5)
  
### 13.6 사용자 정의 직렬화 : Kryo 직렬화. Kryo는 자바 직렬화보다 속도가 빠르지만, 사용할 클래스를 사전에 등록해야함 (4부에서 계속...)
