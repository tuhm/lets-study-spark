# 12장. RDD (Resilient Distributed Dataset) 

- 여러 분산 노드에 걸쳐서 저장되는 변경이 불가능한 데이타(객체)의 집합
- Spark의 저수준 API는 1. 분산 데이터 처리를 위한 RDD 2. Accumulator, 브로드캐스트 분산형 공유 변수(14장)를 배포하고 다루기 위한 API

+ 저수준 API가 필요한 경우
  + 고수준 API에서 제공하지않는 기능이 필요한 경우
  + RDD를 사용해 개발된 기존 코드를 유지해야 하는 경우
  + 사용자가 정의한 공유 변수(14장)를 다뤄야 하는경우
  + 세밀한 제어 방법을 제공하여 개발 시 사용하는 경우
  
## 12.2 RDD 개요

- 모든 Dataframe, Dataset 코드는 RDD로 컴파일됨. 스파크 UI에서 RDD 단위로 잡이 수행
- RDD는 불변성(변경이 불가능, 회복력)을 가지며 **병렬로 처리할 수 있는 파티셔닝된 레코드의 모음**
- DataFrame의 각 레코드는 스키마를 알고 있는 필드로 구성된 구조화된 로우인 반면, **RDD의 레코드는 그저 프로그래머가 선택하는 자바, 스칼라, 파이썬의 객체**
- 이 객체에는 사용자가 원하는 포맷을 사용해 원하는 모든 데이터를 저장 할 수 있음
- 구조적 API와는 다르게 레코드의 내부 구조를 스파크에서 파악할 수 없으므로 최적화를 하려면 훨씬 많은 수작업이 필요
- RDD API는 11장에서 알아본 Dataset과 유사하지만, RDD는 구조화된 데이터 엔진을 사용해 데이터를 저장하거나 다루지 않음 
- 하지만 RDD와 Dataset 사이의 전환은 매우 쉬우므로 두 API를 모두 사용해 각 API의 장점을 동시에 활용 가능


### 12.2.1 유형
- RDD는 DataFrame API에서 최적화된 물리적 실행 계획을 만드는 데 대부분 사용
  1. 제네릭 RDD type
  2. 키 기반의 집계가 가능한 키-값 RDD : 특수 연산 뿐만 아니라 키를 이용한 사용자 지정 파티셔닝 개념 보유

- 특징
  - RDD 역시 분산 환경에서 데이터를 다루는 데 필요한 지연 처리 방식의 **트랜스포메이션과 즉시 실행 방식의 액션을제공**
  - **RDD에는 ‘로우’라는 개념이 없습니다.** 개별 레코드는 지바, 스칼라, 파이썬 객체일 뿐이며 구조적 API에서 제공하는 여러 함수를 사용하지 못하기 때문에 수동으로 처리해야함
  - RDD API는 스칼라, 자바 뿐만 아니라 파이썬에도 사용 가능. 하지만 파이썬으로 RDD를 사용할 때는 성능 저하가 발생하므로 **파이썬에서는 구조적 API 사용을 권장**
  - 수동으로 RDD생성은 권장하지 않음. 
  - **물리적으로 분산된 데이터(자체적으로 구성한 데이터 파티셔닝)에 세부적인 제어가 필요할 때 RDD 를 사용**

## 12.3 RDD 생성하기

### 12.3.1 Dataframe/Dataset으로 RDD 생성하기
``` C
spark.range(10).rdd
spark.range(10).toDF("id").rdd.map(lambda row : row[0])
spark.range(10).rdd.toDF() #RDD 를 사용하여 Dataframe, Data set 생성
```

### 12.3.2 로컬 컬렉션으로 RDD 생성하기
- SparkSession 안에 있는 SparkContext의 parallelize 메서드를 호출.
``` C
myCollection = "Spark The Definitive guide : Big Data Processing Made Simple".split(" ")
words = spark.SparkContext.parallelize(myCollection, 2)  // 파티션 수 지정가능
words.setName("myWords")
words.name() // 스파크 UI에 지정한 이름으로 RDD 표시
```

### 12.3.3 데이터 소스로 RDD 생성하기
- DataSource API는 데이터를 읽는 가장 좋은 방법입니다. 또한 sparkContext를 시용해 데이터를 RDD로 읽을 수 있습니다.
``` C
Spark.sparkContext.textFile("/some/path/withTextFiles")
Spark.sparkcontext.wholeTextFiles("/some/path/withTextFiles")
```

## 12.4 RDD 다루기
- RDD는 스파크 데이터 타입 대신 자바나 스칼라의 객체를 다룬다는 사실이 가장 큰 차이점
- 필터, 맵 함수, 집계 등 Dataframe의 다양한 함수를 사용자가 직접 정의

## 12.5 트랜스포메이션
- Dataframe이나 Dataset과 동일하게 RDD에 트랜스포메이션을 지정해 새로운 RDD를 생성

  1. Distinct : 중복된 항목 제거
  ``` C
  words.distinct().count()
  ```

  2. filter (=where)
  ``` C
  def startsWithS (individual):
      return individual.startsWith("S")
  words.filter(lambda word: StartsWithS(word)).collect()
  ```

  3. map : 주어진 값을 원하는 값으로 반환
  ``` C
  words = words.map(lambda word: (word, word [0] , word.startswith("5")))
  ```

  4. SortBy : 정렬
  ``` C
  words.sortBy(lambda word: len(word) * -1).take(2)
  ```

  5. randomSplit : RDD를 임의로 분할하여 RDD 배열을 생성 
  ``` C
  fiftyFiftySplit = words.randomSplit([O.5, 0.5])
  ```

## 12.6 액션 : 트랜스포메이션 연산을 시작하는 액션!

  1. reduce : 모든 값을 하나의 값으로 만들 때 사용
  ``` C
  spark.sparkContext.parallelize(range(1, 20)).reduce(lambda x, y: x + y)  // result = 210
  ```
  2. count : 전체 로우수 반환 
  ``` C
  countApprox(timeoutMilliseconds, confidence) 
  countByValue : RDD 값의 갯수 반환 (이 메서드는 전체 로우 수나 고유 아이템 수가 작은 경우에만 사용하는 것이 좋음)
  ```
  3. first : Data의 첫번째 값 반환
  4. min , max : Data min, max
  5. take : "값"의 개수를 파라미터로 사용. 필요 수 만큼 출력
  ``` C 
  words.take(5)
  word5.takeOrdered(5)
  word5.top(5)
  ```

## 12.7 파일 저장하기
- RDD를 사용하면 일반적인 의미의 데이터소스에 저장 할 수 없음
- 각 파티션의 내용을 저장하려면 전체 파티션을 순회하면서 외부 데이터베이스에 저장
- 스파크는 각 파티션의 데이터를 파일로 저장.

  1. saveAsTextFile : text 파일로 저장
  2. 시퀀스 파일 *words.saveAsObjectFile("/tmp/my/sequenceFilePath")
    시퀀스 파일 : 키와 값으로 구성된 데이터를 저장하는 이진 파일 포맷으로, 하둡에서 자주 사용됨
  3. 하둡파일 : 여러가지 하둡 파일 포맷 가능

## 12.8 캐싱 : RDD를 캐싱하거나 저장 할 수 있음. 
- 기본적으로 캐시와 저장은 메모리에 있는 데이터만을 대상으로함
- SetName 함수를 사용하면 캐시된 RDD에 이름을 지정
``` C 
words.cache()
words.getStorageLevel() // 저장소 수준 조회
```

## 12.9 체크 포인팅 : RDD를 디스크에 저장하는 방식. 반복적인 연산 시 매우 유용
- 스파크는 단순히 캐시처럼 RDD의 데이터만 저장하는 것이 아니라 RDD의 계보까지 모두 저장
``` C 
spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
words.checkpoint()
```

## 12.10 RDD를 시스템 명령으로 전송하기
- pipe 메서드를 시용하면 파이핑 요소로 생성된 RDD를 외부 프로세스로 전달할 수 있습니다 ? 
``` C 
words.pipe("wc -1").collect() #파티션당 5개 로우 반환
```
  1. mapPartions : 메서드가 기본적으로 파티션 단위로 작업을 수행. -> 전체 파티션에 대한 연산을 수행 할 수 있음. mapPartitions는 개별 파티션(이터레이터로 표현)에 대해 map 연산을 수행.
  ``` C 
  words.mapPartitions(lambda part: [1]).sum() # 합계는 2입니다.
  ```
  2. foreach Partition : foreach Partition 함수는 파티션의 모든 데이터를 순회할 뿐 결과는 반환하지 않습니다. mappartitions와 차이는 반환값의 존재 여부일뿐
  3. glom : 데이터셋의 모든 파티션을 배열로 변환하는 함수. 데이터를 드라이버로 모으고 데이터가 존재하는 파티션의 배열이 필요한 경우 사용.
  ``` C 
  spark.sparkContext.parallelize(["Hello" , "World"] , 2).glom().collect() 
  ```
  
  

