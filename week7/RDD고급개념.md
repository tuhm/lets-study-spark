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


