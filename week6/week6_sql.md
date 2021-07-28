## 10장 스파크 SQL

Structured Query Language 의 범용적인 인터페이스로 스파크의 연산 능력 활용 + 스파크 기반 파이프라인 구축 가능 (쿼리 -> DataFrame 변환 -> MLlib 사용하여 트레이닝) 


### 10.3.1 스파크와 하이브의 관계
#### < 스파크와 하이브의 비교 >
| HIVE | Spark |
| --- | --- |
| RDBMS 와 같은 식으로 데이터를 저장함 | DB 없음 |
| HiveQL (Hive 고유의 SQL Engine) 로 데이터 추출 가능 | SQL, Java, Scala, Python 모두 지원 가능하며 (자체적인) MapReduce 메커니즘 사용 | 
| Hadoop 위에서 가동됨 | Spark 는 저장장치 없으므로 External Distributed Data Stores (외부 분산 데이터 저장소) 인 Hive, HBase 에서 데이터를 가져옴 | 
| data warehousing 에 포커스됨 | 좀더 복잡한 연산과 in-memory 로 분석 수행, data streaming에 용이함 |

하이브 메타스토어 (여러세션에서 사용할 테이블 정보 보관) 를 통해 하이브와 연동: 메타스토어를 이용하면 조회할 파일 수를 최소화할 수 있음  

#### < 속성값 설정 >
- 접속하려는 하이브 메타스토어 버전 번경: ```spark.sql.hive.metastore.version``` 에 설정
- HiveMetastoreClient 가 초기화되는 방식 변경: ```spark.sql.hive.metastore.jars``` 설정
- 하이브 메타스토어가 저장된 다른 데이터베이스에 접속: 각 DB 에 상응하는 클래스 접두사 정의하고 스파크와 하이브에서 공유할 수 있도록: ```spark.sql.hive.metastore.sharedPrefixes``` 에 설정
- ```spark.sqlContext.setConf(spark.sql.hive.metastore.version, "1.2.1")``` 방식으로 설정할 수 있음  

### 10.4 스파크 SQL 쿼리 실행방법
### 10.4.1. SQL CLI (Command-Line-Interface)

- 쓰리프트 JDBC 서버와 통신할 수 없다 (???) 
- 스파크 디렉터리에서 ```.bin/spark-sql``` 을 실행하거나 oasis 같은 환경에서는 ```%sparksql``` 로 실행 (이 때 하이브 서버와 연동하려면 conf 설정이 필요함) 

### 10.4.2. 스파크의 프로그래밍 SQL Interface

- 스파크에서 지원하는 language API (Python, Scala) 를 통해 SparkSession 객체의 sql 메서드를 사용 
  -  ```%pyspark``` 같은 환경에서 ```spark.sql``` 사용하는 것을 말함 
또 SQL 과 DataFrame 이 완벽하게 연동되고, 처리된 결과를 DataFrame 으로 반환
- ``` .createOrReplaceTempView("someTable") ``` 로 SQL 에서 사용할 수 있도록 처리 후 SQL 코드로 연산 
- DataFrame 을 사용하는 것보다 SQL 코드로 표현하기 쉬운 트랜스포메이션의 경우 강력함 

### 10.4.3. 스파크 SQL 쓰리프트 JDBC/ODBC 서버 Interface

- Tableau나 Excel 같은 소프트웨어에서 스파크에 접속하여 Spark SQL query engine 을 활용 
- Spark Thrift Server 에 접속하기 위해 JDBC client 를 갖춘 beeline 이나 squirrel 을 이용

### 10.5 카탈로그 
스파크 SQL 에서 가장 높은 추상화 단계. 메타데이터 뿐 아니라 데이터베이스, 테이블, 함수, 뷰에 대한 정보를 추상화. 스파크 SQL 을 사용하는 또 다른 프로그래밍 인터페이스 (10.4.2 의 내용) 

### 10.6 테이블 

- 데이터의 구조라는 점에서 DataFrame 과 논리적으로 동일함. 단, DataFrame 은 프로그래밍 언어로 정의하지만 테이블은 데이터베이스에서 정의함 
- 스파크에서 테이블은 항상 데이터를 가지고 있음 (임시 테이블 없고, 데이터를 가지지 않는 것은 뷰 뿐임) 

### 10.6.1 스파크 관리형 테이블 (Managed Table)

- 테이블의 데이터와 테이블에 대한 데이터 (메타 데이터) 를 모두 저장함 
- ```saveAsTable``` 로 관리형 테이블을 만듬 
- 관리형 테이블 (Internal Table) 을 생성하면 파일이 기본 저장 경로인 ```/user/hive/warehouse``` 에 저장되고, 외부테이블과는 달리 drop 하면 데이터와 스키마가 함께 삭제됨 
- 저장 경로 하위(??) 에서 데이터베이스 목록을 확인할 수 있음

### 10.6.2 테이블 생성하기 

- 전체 데이터소스 API 를 재사용할 수 있는 독특한 기능(???) 을 지원함: 실행 즉시 테이블을 생성하므로 스키마 정의 따로 데이터 적재 따로 안해도 됨 
- ```Using``` 구문을 통해 읽어오는 데이터의 포맷을 지정할 수 있고 (지정하여 성능에 무리가 없음) ```STORED AS``` 구문으로 하이브 테이블을 생성할 수 있음
- ```CREATE TABLE some_table USING parquet PARTITIONED BY (some_col) AS SELECT * FROM other_table ```
 
### 10.6.3. 외부 테이블 생성하기 (External Table)

- 이미 하둡에 있는 데이터를 기반으로 테이블을 만들기 때문에 스키마만 정해주면 됨. 그래서 파일 따로 스키마 따로 관리할 수 있음 (스파크는 메타데이터를 관리하고, 데이터파일은 스파크에서 관리하지 않음)
- 누군가 테이블을 날려도 데이터는 있음. 단, 더이상 외부 테이블명을 이용해 데이터를 조회할 수는 없음 
- Location 에 저장된 CSV 형태의 파일을 테이블 형태로 열람하거나
```
CREATE EXTERNAL TABLE tbl ( 
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '.' 
STORED AS CSV 
LOCATION '/some/path' ;
```
- 또는 테이블로 부터 데이터를 읽어서 외부 테이블을 다시 만들 수도 있음 
```
CREATE EXTERNAL TABLE tbl2 ( 
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '.' 
LOCATION '/some/path' 
AS SELECT * FROM tbl1;
```


### 10.6.4. 테이블에 데이터 삽입하기 

- 일반 SQL 문법과 같으나 파티션에만 저장하고 싶은 경우 파티션 명세를 추가할 수 있음 
- 하지만 마지막 파티션에만 데이터 파일이 추가된다?? 
-

### 10.6.5. 테이블 메타데이터 확인하기 

```Describe Table ``` : ```Describe Extended Table``` 할 경우 관리형인지 외부 테이블인지 확인 가능

### 10.6.6. 테이블 메타데이터 갱신하기 

- ``` Refresh Table ``` : 테이블과 관련된 모든 캐싱된 항목 (기본적으로 파일) 갱신함 
- ``` MSCK Repair Table```: 카탈로그에서 관리하는 테이블의 파티션 정보를 새로고침

### 10.6.7. 테이블 제거하기 

- 삭제 (delete) 는 불가하고 오로지 제거 (drop) 만 가능

### 10.6.8. 테이블 캐싱하기

- DataFrame 에서처럼 테이블을 캐시하거나 (cache) 캐시에서 제거할 수 (uncache) 있음
- in-memory 로 불러왔을 때 효율적인 연산 (필요한 컬럼만 본다거나, 일부 데이터만 스캐닝한다거나 등등) 을 위해 활용됨
- https://spark.apache.org/docs/1.6.1/sql-programming-guide.html#configuration

### 10.7. 뷰
뷰는 단순히 쿼리 실행계획!

### 10.7.1. 뷰 생성하기
- 최종 사용자에게는 단순히 테이블 처럼 보임. (physical data 는 없이 virtual table 을 구성) 
- 쿼리 시점에 데이터 소스에 트랜스포메이션을 적용해 구성함 (기존 DataFrame 에서 새로운 DataFrame 을 만드는 것과 실행계획에서 동일함)
- 세션에서만 살아있는 경우는 ```Temporary View```

### 10.7.2. 뷰 제거하기
- 테이블 제거와 동일한 방식

### 10.8 데이터베이스
- 테이블을 조직화하기 위한 도구 

### 10.10 고급 주제 
### 10.10.1. 복합 데이터 타입
- 구조체: 중첩 데이터 타입 ```(col1, col2) as col3 ```
- 리스트: Array [] 형태로 여러 row 를 배열 형태로 바꿔낼 수 있음.  ```explode ``` 를 통해 다시 원복할 수 있음
### 10.10.2 함수
-  ```SHOW [System|User] Functions  ``` 를 통해 등록된 함수 열람 가능
### 10.10.3 서브쿼리
- 비상호연관 서브쿼리: 테이블 하나에서 연산을 하고 그 결과를 서브 쿼리로 활용하는 경우
- 상호연관 서브쿼리: 테이블 여러개를 join 하며 연산하며 서브 쿼리로 활용하는 경우

### 10.11.1 설정 
https://spark.apache.org/docs/latest/sql-performance-tuning.html
