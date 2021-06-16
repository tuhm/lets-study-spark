1. 아파치 스파크란?
 - 통합 컴퓨팅 엔진이며, 데이터를 병렬로 처리하는 라이브러리의 집합이다 
 | 개념 | 뜻 |
 | ---- | ---- |
 | 통합 | 데이처 처리부터 머신러닝, 스트림 처리까지 일관성있는 API |
 | 컴퓨팅 엔진| 연산 (컴퓨팅) 만 하지,, 저장은 X (저장용으로 하둡 파일 시스템을가지고, 맵리듀스로 연산을 하는 하둡 프레임워크와는 다름) |
 | 라이브러리| 통합 API (스파크 SQL, MLlib,, GraphX, 스트리밍, 저장소와의 연결 커넥트 외 다양한 외부 라이브러리) |
 
 - 어떻게 나왔나? 클러스터 컴퓨팅의 필요성 + 맵리듀스의 난이도와 효율성 개선

2. 스파크 간단히 살펴보기
 - 컴퓨터가 한 대가 아니기 때문에 (클러스터) 작업을 조율하고 자원을 관리할 필요가 있음 (그래서 클러스터 매니저가 필요)
 - 유저는 클러스터 매니저에 애플리케이션을 제출 -> 클러스터 매니저가 애플리케이션 실행에 필요한 자원을 할당 -> 할당받은 클러스터에서 작업이 실행됨
 
 | 개념 | 의미 |
 | ---- |  ---- | 
 | Application | = Driver + Executor: 드라이버가 머리 (익스큐터 프로세스의 작업과 관련된 분석, 배포, 스케줄링 역할)<br/> 하며 익스큐터는 드라이버가 시킨 작업을 수행 |
 | SparkSession| (드라이버 프로세스): Scala, Python, R 등 언어에 상관없이 스파크 코드의 진입점이 됨. 실제적으로는 자바 가상 머신이 돌아가므로, 스파크가 JVM 이 이해할 수 있는 언어로 바꿔준다!!
 | Spark API | 저수준의 비구조적 API 와 고수준의 구조적 API |

|Section | Summary |
| ---- |  ---- |
|2.6 Data Frame | - 우리가 아는 테이블 형태의 Data Frame 과 Schema (단, 보통 분산하여 저장되어 있음)<br/> - 파티션단위로 데이터를 분할해 저장함 (파티션이 하나면 병렬성 1, 파티션이 여러개라도 일을 할 익스큐터가 하나밖에 없으면 병렬성 1) |
|2.7 Transformation | - immutable: 한번생성하면 변경할 수 없어서(??) 변경 방법을 스파크에게 알려줘야하며 액션 하기 전까지는 수행되지 않는다!<br/> - narrow transformation: 각 입력 파티션이 하나의 출력 파티션에만 영향을 끼침(입력 대 출력 1:1) where 절 같은 것 필터링 (Pipelining: 메모리에서만 수행)<br/>- wide transofmration: 하나의 입력 파티션이 여러 출력 파티션에 영향을 미침.(Shuffle - 디스크에 저장)<br/> - lazy evaluation: 실행 계획만 가지고 있다가 트랜스포메이션 마지막 단계에서 한꺼번에 실행하기 때문에 데이터 흐름이 최적화됨|
| 2.8 Action | - Transformation 이 논리적 실행 계획이라면, 실제 연산은 액션을 명령해야만 일어남<br/> - 예: count/콘솔에서 데이터 열람/ 네이티브 객체에 데이터 모으기/출력 소스에 저장<br/> - 액션을 지정하면 스파크 잡이 시작되어 필터 (narrow transformation) 수행후 count (wide transformation 수행) |
| 2.9 Spark UI| - Spark job 의 모니터링 용도 |
| 2.10 예제 | - InferSchema (운영환경에서는 추론 하지 말고 엄격하게 지정해야 함): 데이터를 다 읽을 필요가 없기 때문에 lazy evaluation 수행<br/> - Take (narrow) -> Sort (wide) 실행계획을 컴파일하며 액션 전까지는 실제로 데이터 변환이 일어나지 않는다<\br>Spark.sql 로 하나, Dataframe 에 Groupby 적용하나 스파크 상으로는 같다 |

3. 스파크 기능 둘러보기

3.1 운영용 애플리케이션 실행
 - spark-submit 을 통해 쉽게 운영용 애플리케이션으로 전환 (제출되면 클러스터에서 실행됨) 
 - 여기에 애플리케이션 실행에 필요한 자원과 실행방식까지 지정 할 수 있다
 
3.2 구조적 API (DataSet)
 - 타입 안정성을 제공하는 API

3.3 구조적 스트리밍

3.4 머신러닝과 고급분석 
  - MLlib 으로 대용량 데이터 preprocessing, munging, model training, predicition 가능 
 A. Transformation
   - StringIndexer (LableEncoder 같은), OneHotEncoder
   - Vector Assembly 로 Pipeline 설계 (Transformer 를 Fit 하는 과정)
   - 캐싱하면 변환된 데이터셋 복사본을 메모리에 저장
 B. Training
  - 초기화된 모델의 세팅(Algorithm: KMeans) -> 데이터에 학습시킴 (AlgorithmModel: KmeansModel)
 C. Post-Training
  - Cost 계산

3.5 저수준 API (RDD) 
  - RDD (Resilient Distributed Dataset) 스파크의 기본 데이터 구조. 
  - 분산 변경 불가능한 객체 모음이며 스파크의 모든 작업은 새로운 RDD를 만들거나 존재하는 RDD를 변형하거나 결과 계산을 위해 RDD에서 연산하는 것을 표현하고 있음 (https://bomwo.cc/posts/spark-rdd/)
  - 하둡의 맵리듀스의 연산 방식을 극복하기 위해 

3.6 SparkR 
 - 스파크를 R 로 사용하기! 파이썬과 유사함

3.7 Spark Ecosystem 
 - spark-packages.org 
