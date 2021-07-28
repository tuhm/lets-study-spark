### 10장 스파크 SQL

Structured Query Language 의 범용적인 인터페이스로 스파크의 연산 능력 활용 + 스파크 기반 파이프라인 구축 가능 (쿼리 -> DataFrame 변환 -> MLlib 사용하여 트레이닝) 


#### 10.3.1 스파크와 하이브의 관계
##### < comparison >
| HIVE | Spark |
| --- | --- |
| RDBMS 와 같은 식으로 데이터를 저장함 | DB 없음 |
| HiveQL (Hive 고유의 SQL Engine) 로 데이터 추출 가능 | SQL, Java, Scala, Python 모두 지원 가능하며 (자체적인) MapReduce 메커니즘 사용 | 
| Hadoop 위에서 가동됨 | Spark 는 저장장치 없으므로 External Distributed Data Stores (외부 분산 데이터 저장소) 인 Hive, HBase 에서 데이터를 가져옴 | 
| data warehousing 에 포커스됨 | 좀더 복잡한 연산과 in-memory 로 분석 수행, data streaming에 용이함 |

하이브 메타스토어 (여러세션에서 사용할 테이블 정보 보관) 를 통해 하이브와 연동: 메타스토어를 이용하면 조회할 파일 수를 최소화할 수 있음  

##### < 속성값 설정 >
- 접속하려는 하이브 메타스토어 버전 번경: spark.sql.hive.metastore.version 에 설정
- HiveMetastoreClient 가 초기화되는 방식 변경: spark.sql.hive.metastore.jars 설정
- 하이브 메타스토어가 저장된 다른 데이터베이스에 접속: 각 DB 에 상응하는 클래스 접두사 정의하고 스파크와 하이브에서 공유할 수 있도록 spark.sql.hive.metastore.sharedPrefixes 에 설정

#### 10.4 스파크 SQL 쿼리 실행방법 1. SQL CLI (Command-Line-Interface)
쓰리프트 JDBC 서버와 통신할 수 없다 (???) 
스파크 디렉터리에서 .bin/spark-sql 을 실행하거나 oasis 같은 환경에서는 %sparksql 로 실행 (이 때 하이브 서버와 연동하려면 conf 설정이 필요함) 

#### 10.4 스파크 SQL 쿼리 실행방법 2. 스파크의 프로그래밍 SQL Interface
스파크에서 지원하는 language API (Python, Scala) 를 통해 SparkSession 객체의 sql 메서드를 사용 
- %pyspark 같은 환경에서 spark.sql 사용하는 것을 말함 
처리된 결과는 DataFrame 을 반환
DataFrame 을 사용하는 것보다 SQL 코드로 표현하기 쉬운 트랜스포메이션의 경우 강력함 
또 SQL 과 DataFrame 이 완벽하게 연동됨 
- .createOrReplaceTempView("someTable") 로 SQL 에서 사용할 수 있도록 처리 후 SQL 코드로 연산 

#### 10.4 스파크 SQL 쿼리 실행방법 3. 스파크 SQL 쓰리프트 JDBC/ODBC 서버 Interface
Tableau나 Excel 같은 소프트웨어에서 스파크에 접속하여 Spark SQL query engine 을 활용 
Spark Thrift Server 에 접속하기 위해 JDBC client 를 갖춘 beeline 이나 squirrel 을 이용

#### 10.5 카탈로그 
스파크 SQL 에서 가장 높은 추상화 단계. 메타데이터 뿐 아니라 데이터베이스, 테이블, 함수, 뷰에 대한 정보를 추상화. 스파크 SQL 을 사용하는 또 다른 프로그래밍 인터페이스 (10.4.2 의 내용) 

#### 10.6 테이블 
데이터의 구조라는 점에서 DataFrame 과 논리적으로 동일함. 단, DataFrame 은 프로그래밍 언어로 정의하지만 테이블은 데이터ㅔㅂ이스에서 정의함 
스파크에서 테이블은 항상 데이터를 가지고 있음 (임시 테이블 없고, 데이터를 가지지 않는 것은 뷰 뿐임) 

