### 10장 스파크 SQL

Structured Query Language 의 범용적인 인터페이스로 스파크의 연산 능력 활용 + 스파크 기반 파이프라인 구축 가능 (쿼리 -> DataFrame 변환 -> MLlib 사용하여 트레이닝) 


#### 10.3.1 스파크와 하이브의 관계
하이브 메타스토어 (여러세션에서 사용할 테이블 정보 보관) 를 통해 하이브와 연동 
조회할 테이블 수를 최소화하기 위해 메타스토어 이용 

##### < comparison >
| HIVE | Spark |
| --- | --- |
| RDBMS 와 같은 식으로 데이터를 저장함 | DB 없음 |
| HiveQL (Hive 고유의 SQL Engine) 로 데이터 추출 가능 | SQL, Java, Scala, Python 모두 지원 가능하며 (자체적인) MapReduce 메커니즘 사용 | 
| Hadoop 위에서 가동됨 | Spark 는 저장장치 없으므로 External Distributed Data Stores (외부 분산 데이터 저장소) 인 Hive, HBase 에서 데이터를 가져옴 | 
| data warehousing 에 포커스됨 | 좀더 복잡한 연산과 in-memory 로 분석 수행, data streaming에 용이함 |
